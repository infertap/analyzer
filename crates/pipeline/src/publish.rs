// SPDX-License-Identifier: AGPL-3.0-only

//! The serving port: how the core states its output to a serving store
//! (`docs/lake.md`).
//!
//! The core writes serving and never reads it. It writes two ways. A tick states the cells it
//! changed ([`Publish::apply`]): each [`Row`] replaces its key's cell, or deletes it when
//! `gone`. A public module restates its output days ([`Publish::replace`]): the scope then holds
//! exactly the rows given. Both carry a publication id, the journal revision
//! the write belongs to; per key, the highest publication wins, and a retried write with the
//! same id converges. How a store realizes that (row versions and asynchronous retirement for
//! `ClickHouse`, a partition swap or a transaction elsewhere) is the implementation's business,
//! and [`Publish::settled`] says when readers see it.

use crate::Error;
use analyzer_analysis::emitted::Meaning;
use analyzer_analysis::module::{Row, Table};

/// What a write landed.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Written {
    /// Rows written across the module's tables, tombstones included.
    pub rows: u64,
    /// The bytes those rows were sent as, in the store's encoding.
    pub bytes: u64,
}

/// Whose output a write states: the fleet and the meaning pair every served row carries.
#[derive(Debug, Clone, Copy)]
pub struct Head<'a> {
    /// The fleet the rows belong to.
    pub fleet_id: &'a str,
    /// The meaning pair (semantics version, discriminator) the rows are stated under.
    pub meaning: &'a Meaning,
}

/// What a [`Publish::replace`] restates: these tables, over these UTC days, under a head.
#[derive(Debug, Clone, Copy)]
pub struct Scope<'a> {
    /// The fleet and meaning.
    pub head: Head<'a>,
    /// The tables restated. A row of another table is refused.
    pub tables: &'a [Table],
    /// The UTC day indices restated, each whole.
    pub days: &'a [i64],
}

/// The derivation's standing facts, as readers see them: one row per fact under the head. The
/// API's freshness figures come from here, so a reader never consults the core's own state.
pub const DERIVATION_STATUS: Table = Table {
    name: "derivation_status",
    key: &[analyzer_analysis::module::Column::new(
        "fact",
        analyzer_analysis::module::Kind::Str,
    )],
    measures: &[analyzer_analysis::module::Column::new(
        "value",
        analyzer_analysis::module::Kind::F64,
    )],
    day: analyzer_analysis::module::DayOf::Undated,
};

/// One standing fact of [`DERIVATION_STATUS`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Status {
    /// Repeat compute's classified edge, epoch milliseconds: every store before it is
    /// classified.
    ClassifiedToMs(f64),
    /// Placement's emitted edge, epoch milliseconds: the placement cells are stated before it.
    EmittedToMs(f64),
    /// Coverage's stated edge, epoch milliseconds: coverage is settled before it.
    CoveredToMs(f64),
    /// The sampled audit's edge, epoch milliseconds: every block it re-derived before it agreed
    /// with what was stated.
    AuditedToMs(f64),
    /// Where the sampled audit first disagreed, epoch milliseconds: the start of the window
    /// whose sample stated differently. Absent while every sample agrees.
    AuditDisagreedAtMs(f64),
    /// The last UTC day every edge has passed by the settle delay.
    SettledThroughDay(i64),
    /// When the last tick was applied, wall-clock epoch milliseconds.
    LastTickAtMs(f64),
}

impl Status {
    /// The fact's key in [`DERIVATION_STATUS`].
    #[must_use]
    pub fn fact(&self) -> &'static str {
        match self {
            Self::ClassifiedToMs(_) => "classified_to_ms",
            Self::EmittedToMs(_) => "emitted_to_ms",
            Self::CoveredToMs(_) => "covered_to_ms",
            Self::AuditedToMs(_) => "audited_to_ms",
            Self::AuditDisagreedAtMs(_) => "audit_disagreed_at_ms",
            Self::SettledThroughDay(_) => "settled_through_day",
            Self::LastTickAtMs(_) => "last_tick_at_ms",
        }
    }

    /// This fact as a row of [`DERIVATION_STATUS`].
    #[must_use]
    pub fn row(&self) -> Row {
        #[allow(clippy::cast_precision_loss)] // day indices are small integers
        let value = match *self {
            Self::ClassifiedToMs(v)
            | Self::EmittedToMs(v)
            | Self::CoveredToMs(v)
            | Self::AuditedToMs(v)
            | Self::AuditDisagreedAtMs(v)
            | Self::LastTickAtMs(v) => v,
            Self::SettledThroughDay(day) => day as f64,
        };
        Row {
            table: DERIVATION_STATUS.name,
            key: vec![analyzer_analysis::module::Value::Str(
                self.fact().to_string(),
            )],
            measures: vec![analyzer_analysis::module::Value::F64(value)],
            gone: false,
        }
    }
}

/// State one standing fact under `head` at `publication`.
///
/// # Errors
/// As [`Publish::apply`].
pub fn status(
    publish: &dyn Publish,
    head: &Head<'_>,
    publication: u64,
    fact: Status,
) -> Result<(), Error> {
    publish.apply(head, &[DERIVATION_STATUS], publication, &[fact.row()])?;
    Ok(())
}

/// A serving store the core states its output to.
pub trait Publish: Send + Sync {
    /// Create or verify the declared tables.
    ///
    /// # Errors
    /// The store refused, or holds a table of that name with another shape.
    fn install(&self, tables: &[Table]) -> Result<(), Error>;

    /// Apply keyed changes to `tables`: each row replaces its key's cell, or deletes it when
    /// `gone`.
    ///
    /// # Errors
    /// A row of an undeclared table, a row its table refuses, or the store's failure.
    fn apply(
        &self,
        head: &Head<'_>,
        tables: &[Table],
        publication: u64,
        rows: &[Row],
    ) -> Result<Written, Error>;

    /// Make `scope` hold exactly `rows`: every other key in it is deleted.
    ///
    /// # Errors
    /// As [`Publish::apply`].
    fn replace(&self, scope: &Scope<'_>, publication: u64, rows: &[Row]) -> Result<Written, Error>;

    /// Whether every write made so far is visible to readers exactly as stated.
    ///
    /// # Errors
    /// The store could not be asked.
    fn settled(&self) -> Result<bool, Error>;
}

/// A serving store: the [`Publish`] port, bound to one namespace.
pub trait Serving: Publish {
    /// Bind this store to `namespace`; refuse one already bound to another.
    ///
    /// # Errors
    /// The store is bound to another namespace, or failed.
    fn bind(&self, namespace: &crate::execution::Namespace) -> Result<(), Error>;

    /// The namespace this store is bound to, or `None` before a bind.
    ///
    /// # Errors
    /// The store failed.
    fn bound(&self) -> Result<Option<crate::execution::Namespace>, Error>;
}
