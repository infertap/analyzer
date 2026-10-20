// SPDX-License-Identifier: AGPL-3.0-only

//! The public modules as the registry holds them (`crate::adapter`): each registration's
//! declarations and its run over one settled day, type-erased only here, in the heterogeneous
//! registry. The core registers (repeat compute, placement, coverage, the roster, the tag cells)
//! are the keyed driver's own operators, never registrations; of them, coverage is offered to a
//! module as a declared dependency.

use crate::Error;
use analyzer_analysis::module::{DerivationModule, ModuleSpec};
use analyzer_analysis::{Params, Window};
use std::any::{Any, TypeId};
use std::collections::BTreeMap;

/// The core registers' names: the keyed driver's operators. A module cannot be named so, and
/// cannot be disabled, since the receipt and the console read every one of them.
pub const CORE: &[&str] = &[
    <analyzer_analysis::class_band::ClassBand as DerivationModule>::NAME,
    <analyzer_analysis::observed_coverage::ObservedCoverage as DerivationModule>::NAME,
    <analyzer_analysis::dup_cells::Duplication as DerivationModule>::NAME,
    <analyzer_analysis::roster::Roster as DerivationModule>::NAME,
];

/// Of the core registers, the one whose output a module may declare as a dependency: the day's
/// coverage.
pub(crate) const OFFERED: &str =
    <analyzer_analysis::observed_coverage::ObservedCoverage as DerivationModule>::NAME;

/// A module's run over one settled day: reconstruct, replace the day's rows, and hand the
/// output to its dependents.
type Settle = fn(
    &crate::adapter::Context<'_>,
    &Outputs,
    &dyn crate::publish::Publish,
    u64,
) -> Result<Box<dyn Any>, Error>;

/// A statically linked public module, validated by `ModuleSet`.
#[derive(Clone)]
pub struct Registration {
    pub(crate) spec: ModuleSpec,
    pub(crate) cadence: crate::adapter::Cadence,
    identity: TypeId,
    settle: Settle,
}

impl std::fmt::Debug for Registration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Registration")
            .field("spec", &self.spec)
            .field("cadence", &self.cadence)
            .finish_non_exhaustive()
    }
}

impl PartialEq for Registration {
    fn eq(&self, other: &Self) -> bool {
        self.identity == other.identity && self.spec == other.spec && self.cadence == other.cadence
    }
}

impl Eq for Registration {}

impl Registration {
    /// Register a typed day or window module. Construction performs no I/O.
    #[must_use]
    pub fn of<A: crate::adapter::Adapter>() -> Self {
        Self {
            spec: ModuleSpec::of::<A::Definition>(),
            cadence: A::CADENCE,
            identity: TypeId::of::<A>(),
            settle: settle::<A>,
        }
    }

    /// A module's results reach only modules of its own cadence.
    pub(crate) fn can_depend_on(&self, prerequisite: &Self) -> bool {
        prerequisite.cadence == self.cadence
    }

    /// The initializations a public module may declare.
    pub(crate) fn validate_initialization(&self) -> Result<(), analyzer_analysis::Error> {
        use analyzer_analysis::module::Initialization;
        match (self.cadence, self.spec.initialization) {
            (crate::adapter::Cadence::Window, Initialization::ExaminedHistory) => {
                Err(analyzer_analysis::Error::Modules(format!(
                    "{} declares ExaminedHistory, which window modules do not support",
                    self.spec.name
                )))
            }
            (_, Initialization::ObservedPrefix) => Err(analyzer_analysis::Error::Modules(format!(
                "{} requires a prefix-stateful execution contract, which public modules do not \
                 have",
                self.spec.name
            ))),
            _ => Ok(()),
        }
    }
}

/// The outputs of one day's modules, and the day's coverage they may depend on.
pub(crate) struct Outputs {
    modules: BTreeMap<TypeId, Box<dyn Any>>,
    coverage: CoverageOutput,
}

impl Outputs {
    /// No module's output yet, over a day of `coverage`.
    pub(crate) fn with_coverage(coverage: analyzer_analysis::completeness::Coverage) -> Self {
        Self {
            modules: BTreeMap::new(),
            coverage: CoverageOutput { coverage },
        }
    }

    /// A module's completed output, by its adapter's type.
    pub(crate) fn require<A: crate::adapter::Adapter>(&self) -> Result<&A::Output, Error> {
        self.modules
            .get(&TypeId::of::<A>())
            .and_then(|v| v.downcast_ref())
            .ok_or_else(|| {
                Error::Derivation(format!(
                    "missing completed dependency {}",
                    <A::Definition as DerivationModule>::NAME
                ))
            })
    }

    /// The day's coverage.
    pub(crate) fn coverage(&self) -> &CoverageOutput {
        &self.coverage
    }
}

/// One output day's coverage, as a module that declares the `coverage` dependency reads it.
pub struct CoverageOutput {
    coverage: analyzer_analysis::completeness::Coverage,
}

impl CoverageOutput {
    /// Coverage over this output day, built from the summed coverage cells.
    #[must_use]
    pub fn coverage(&self) -> &analyzer_analysis::completeness::Coverage {
        &self.coverage
    }
}

/// The UTC day indexes an output window touches.
fn output_days(output: &Window) -> Vec<i64> {
    let mut days = Vec::new();
    let mut day = analyzer_analysis::emitted::day_index(output.start_ms);
    #[allow(clippy::cast_precision_loss)] // day indexes are small integers
    while (day as f64) * analyzer_analysis::DAY_MS < output.end_ms {
        days.push(day);
        day += 1;
    }
    days
}

/// A public module over one settled day: reconstruct, then make its tables' rows over the
/// output day exactly the output's, under `publication` and the context's meaning. The keyed
/// emission batch carries the day, so a replay states it again, whole.
fn settle<A: crate::adapter::Adapter>(
    ctx: &crate::adapter::Context<'_>,
    outputs: &Outputs,
    publish: &dyn crate::publish::Publish,
    publication: u64,
) -> Result<Box<dyn Any>, Error> {
    let output = A::reconstruct(
        ctx,
        &crate::adapter::Dependencies {
            outputs,
            declared: <A::Definition as DerivationModule>::NEEDS,
        },
    )?;
    let meaning = analyzer_analysis::emitted::Meaning::current(
        &analyzer_analysis::Discriminator::of(ctx.params),
    );
    publish.replace(
        &crate::publish::Scope {
            head: crate::publish::Head {
                fleet_id: ctx.fleet,
                meaning: &meaning,
            },
            tables: A::TABLES,
            days: &output_days(&ctx.output),
        },
        publication,
        &A::rows(ctx, &output),
    )?;
    Ok(Box::new(output))
}

/// What the settled-day hook runs the public modules against.
pub(crate) struct SettledDay<'a> {
    pub(crate) lake: &'a analyzer_lake::Lake,
    pub(crate) source: &'a dyn analyzer_analysis::events::EventSource,
    pub(crate) fleet: &'a str,
    pub(crate) params: &'a Params,
    pub(crate) publish: &'a dyn crate::publish::Publish,
    pub(crate) publication: u64,
}

/// Run every one of `registrations` over settled day `day`, in their order, against the day's
/// `coverage`. A day module reads the examined history before the day, which the input log must
/// still hold when its module declares it needs that history.
///
/// # Errors
/// The input log no longer holds a history a module needs, or a module failed.
pub(crate) fn settle_day<'a>(
    registrations: impl Iterator<Item = &'a Registration>,
    at: &SettledDay<'_>,
    day: i64,
    coverage: analyzer_analysis::completeness::Coverage,
) -> Result<(), Error> {
    #[allow(clippy::cast_precision_loss)] // day indexes are small integers
    let output = Window {
        start_ms: day as f64 * analyzer_analysis::DAY_MS,
        end_ms: (day + 1) as f64 * analyzer_analysis::DAY_MS,
    };
    let mut outputs = Outputs::with_coverage(coverage);
    for registration in registrations {
        let input = match registration.cadence {
            crate::adapter::Cadence::Day => Window {
                start_ms: output.start_ms - at.params.examined_history_ms,
                end_ms: output.end_ms,
            },
            crate::adapter::Cadence::Window => output,
        };
        if registration.spec.initialization
            == analyzer_analysis::module::Initialization::ExaminedHistory
        {
            let held_from = crate::inputs::log(at.lake)?
                .head()?
                .days
                .keys()
                .next()
                .copied();
            if held_from
                .is_none_or(|first| first > analyzer_analysis::emitted::day_index(input.start_ms))
            {
                return Err(Error::UnavailableHistory { day });
            }
        }
        let ctx = crate::adapter::Context {
            source: at.source,
            fleet: at.fleet,
            params: at.params,
            input,
            output,
        };
        let done = (registration.settle)(&ctx, &outputs, at.publish, at.publication)?;
        outputs.modules.insert(registration.identity, done);
    }
    Ok(())
}
