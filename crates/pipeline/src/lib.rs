// SPDX-License-Identifier: AGPL-3.0-only

//! The engine-free core: derivation execution, durable recovery and derived-object
//! publication, over the lake (`docs/lake.md`).
//!
//! **Why this crate exists.** `analysis` is a derivation — typed events in, figures out, no I/O.
//! This crate is everything that feeds it and keeps its results: the source it reads, the fold's
//! state and the execution journal in the lake, and the port it states its output through. It
//! depends on no serving store. A store implements [`publish::Serving`] (the `ClickHouse` one is
//! `analyzer-serving-clickhouse`), and the core writes to it and never reads it back, so a
//! serving store can be dropped and rebuilt from the lake.
//!
//! Three kinds of thing belong here, and nothing else does:
//!
//! - **The keyed pipeline** — [`keyed`]: the durable input log delivered in order per producer
//!   run ([`keyed::Source`]), repeat compute and placement on lanes keyed by block over their own
//!   state ([`keyed::KeyedState`]), coverage, the roster and the tag cells on their own clocks,
//!   and the summed cells they emit in one exactly-once batch per tick.
//! - **Execution** — [`Executor`] runs one keyed tick under the journal's intent, publishes its
//!   batch, runs the settled-day hook for the public modules ([`adapter`], [`modules`]), and
//!   applies it.
//! - **The port** — [`publish`]: what the core states and how a serving store takes it.
//!
//! **The rule for anything added here:** if it can be written without I/O, it belongs in
//! `analysis` instead; if it speaks a serving store's language, it belongs in that store's
//! crate.
//!
//! # Where to start
//!
//! The `pub use` block below names the supported surface; the modules hold everything.
//!
//! - [`parquet_source::ParquetSource`] reads sealed segments through the content cache; the
//!   keyed driver reads each committed batch's records through it ([`keyed::FillReader`]).
//! - [`Executor::open`] binds a spine, parameters and a serving store; [`Executor::advance`] is
//!   one keyed tick.
//! - [`keyed::Driver`] is the pipeline itself, and `docs/pipeline.md` its map.

pub mod adapter;
mod adapters;
pub mod arrivals;
mod core;
pub mod degradation_io;
pub mod derived;
mod error;
pub mod execution;
pub mod executor;
pub mod facts;
mod fold_state;
pub mod inputs;
pub(crate) mod journal;
pub mod keyed;
pub use analyzer_lake::ledger;
pub use analyzer_lake::lineage;
pub use analyzer_lake::Lake;
pub mod memo;
pub mod modules;
pub mod parquet_source;
pub mod publish;
pub mod read;
pub mod reset;
mod rowbinary;
pub mod source;
pub mod spine;
pub mod state;

// **The supported surface, in one place**: the source a
// derivation reads, the executor that ticks it, and the spine it derives in. Every name is also
// reachable at its module; this block says which of the reachable names a caller is meant to
// start from.
pub use error::Error;
pub use executor::Executor;
pub use parquet_source::{ParquetSource, SourceConfig};
pub use source::FillSource;
pub use spine::Spine;
