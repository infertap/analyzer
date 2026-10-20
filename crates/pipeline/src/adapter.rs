// SPDX-License-Identifier: AGPL-3.0-only

//! Static, typed day/window modules: the public modules, run by the settled-day hook over each
//! settled day (`crate::keyed::Hook`). The framework owns the module's storage: a module
//! declares its tables and renders its output as rows; it never names the engine.

use crate::{adapters, Error};
use analyzer_analysis::{
    events::EventSource,
    module::{DerivationModule, Row, Table},
    Params, Window,
};

pub use adapters::{CoverageOutput, Registration, CORE};

/// Output cadence; prerequisite results never cross cadences. Both run over each settled day.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cadence {
    /// A complete output day, with separately declared input history.
    Day,
    /// A requested window; retries can split it into whole days. Output must compose
    /// across those scopes.
    /// Only `Initialization::OutputWindow` is supported; registration rejects
    /// examined-history initialization because this cadence supplies no separate history.
    Window,
}

/// Stable operation input. Writer ownership and storage remain with the framework.
pub struct Context<'a> {
    /// Committed source input.
    pub source: &'a dyn EventSource,
    /// Fleet in the execution namespace.
    pub fleet: &'a str,
    /// Semantic parameters of this operation.
    pub params: &'a Params,
    /// Available context requested for reconstruction, possibly preceding output.
    pub input: Window,
    /// Authorized output scope. Never emit context outside this window.
    pub output: Window,
}

/// Typed results of declared prerequisites completed in this execution.
pub struct Dependencies<'a> {
    pub(crate) outputs: &'a adapters::Outputs,
    pub(crate) declared: &'static [&'static str],
}
impl Dependencies<'_> {
    fn declared(&self, name: &str) -> Result<(), Error> {
        if self.declared.contains(&name) {
            Ok(())
        } else {
            Err(Error::Derivation(format!("undeclared dependency {name}")))
        }
    }

    /// Borrow a completed prerequisite module's result.
    ///
    /// # Errors
    /// The prerequisite is undeclared, unregistered or has not completed.
    pub fn require<A: Adapter>(&self) -> Result<&A::Output, Error> {
        self.declared(A::Definition::NAME)?;
        self.outputs.require::<A>()
    }

    /// Borrow the day's completed coverage, from the built-in `coverage` module.
    ///
    /// # Errors
    /// `coverage` is undeclared, or it has not completed in this execution.
    pub fn coverage(&self) -> Result<&CoverageOutput, Error> {
        self.declared(adapters::OFFERED)?;
        Ok(self.outputs.coverage())
    }
}

/// One day/window module: reconstruct complete output, and render it as rows of its
/// declared tables.
///
/// The settled-day hook runs a module over each settled day, and again over a day a correction
/// restates. Reconstruction is read-only with respect to serving output. The framework then
/// writes the rows under the emission batch's publication and retires every older version in
/// the output days, so a key absent from the new output leaves the served tables. The batch
/// carries its days until it is applied, so a replay states them again, whole.
///
/// The tables are created from [`Adapter::TABLES`] by [`install`]: each row carries the
/// framework's head (`fleet_id`, `semantics_version`, `discriminator`) before the key and
/// its tail (`version`, `gone`) after the measures.
pub trait Adapter: 'static {
    /// Pure arithmetic and semantic dependency declarations.
    type Definition: DerivationModule;
    /// Complete reconstructed output and typed prerequisite result.
    type Output: 'static;
    /// Day or window execution.
    const CADENCE: Cadence;
    /// The tables the output is published in. Every table's day column places its rows in
    /// an output day.
    const TABLES: &'static [Table];
    /// Reconstruct complete output from retained input and completed dependencies.
    ///
    /// # Errors
    /// Missing history, invalid input or failed reads. Do not publish partial output.
    fn reconstruct(
        context: &Context<'_>,
        dependencies: &Dependencies<'_>,
    ) -> Result<Self::Output, Error>;
    /// The complete output as rows of [`Adapter::TABLES`], inside the context's output
    /// window. Pure: the framework writes them.
    fn rows(context: &Context<'_>, output: &Self::Output) -> Vec<Row>;
}

/// Create an adapter's declared tables where they do not exist. The application calls this
/// once before registering the adapter; registration itself performs no schema change.
///
/// # Errors
/// The serving store refused.
pub fn install<A: Adapter>(serving: &dyn crate::publish::Serving) -> Result<(), Error> {
    serving.install(A::TABLES)
}

#[cfg(test)]
mod tests {
    #[test]
    fn dependency_access_requires_a_declared_need() {
        let outputs = crate::adapters::Outputs::with_coverage(
            analyzer_analysis::coverage_sums::day_coverage(0, &[]),
        );
        let deps = super::Dependencies {
            outputs: &outputs,
            declared: &[],
        };
        assert!(deps
            .coverage()
            .err()
            .unwrap()
            .to_string()
            .contains("undeclared dependency"));
    }
}
