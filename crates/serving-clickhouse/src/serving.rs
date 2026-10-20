// SPDX-License-Identifier: AGPL-3.0-only

//! The `ClickHouse` serving store: the one implementation of [`Serving`].
//!
//! A change is a row at `version = publication` with its `gone` flag, read latest per key. A
//! replacement writes the scope's rows and retires every older version in the scope,
//! asynchronously, and the store is not settled while that retirement runs. The fact ledger
//! is partitioned by its day, so sealing a day merges its partition and retiring a day drops
//! it.

use crate::engine::Engine;
use analyzer_analysis::module::{Row, Table};
use analyzer_pipeline::publish::{Head, Publish, Scope, Serving};
use analyzer_pipeline::Error;

/// The serving store behind a `ClickHouse` database: a connection of its own for the port's
/// calls.
pub struct ClickHouse {
    engine: Engine,
}

impl std::fmt::Debug for ClickHouse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickHouse")
            .field("database", &self.engine.identity())
            .finish_non_exhaustive()
    }
}

impl ClickHouse {
    /// The serving store behind `engine`'s database, on its own connections.
    ///
    /// # Errors
    /// The server refused a connection.
    pub fn new(engine: &Engine) -> Result<Self, Error> {
        Ok(Self {
            engine: engine.sibling()?,
        })
    }
}

fn versioned<'a>(head: &Head<'a>, publication: u64) -> crate::persist::Head<'a> {
    crate::persist::Head {
        fleet_id: head.fleet_id,
        meaning: head.meaning,
        version: publication,
    }
}

impl Publish for ClickHouse {
    fn install(&self, tables: &[Table]) -> Result<(), Error> {
        crate::persist::install_tables(&self.engine, tables)
    }

    fn apply(
        &self,
        head: &Head<'_>,
        tables: &[Table],
        publication: u64,
        rows: &[Row],
    ) -> Result<analyzer_pipeline::publish::Written, Error> {
        crate::persist::write_tables(&self.engine, &versioned(head, publication), tables, rows)
    }

    fn replace(
        &self,
        scope: &Scope<'_>,
        publication: u64,
        rows: &[Row],
    ) -> Result<analyzer_pipeline::publish::Written, Error> {
        let head = versioned(&scope.head, publication);
        let written = crate::persist::write_tables(&self.engine, &head, scope.tables, rows)?;
        for &day in scope.days {
            crate::persist::retire_older_tables(&self.engine, &head, scope.tables, day)?;
        }
        Ok(written)
    }

    fn settled(&self) -> Result<bool, Error> {
        // Retirement is an asynchronous delete: until the engine's mutations drain, a retired
        // key can still be read.
        Ok(self
            .engine
            .run("SELECT count() FROM system.mutations WHERE database = currentDatabase() AND is_done = 0")?
            .trim()
            == "0")
    }
}

impl Serving for ClickHouse {
    fn bind(&self, namespace: &analyzer_pipeline::execution::Namespace) -> Result<(), Error> {
        if let Some(existing) = self.bound()? {
            return if existing == *namespace {
                Ok(())
            } else {
                Err(Error::Derivation(
                    "serving database belongs to a different execution namespace".into(),
                ))
            };
        }
        let row = serde_json::json!({"slot": 0, "identity": namespace.identity()});
        self.engine.insert_stream(
            "INSERT INTO execution_namespace FORMAT JSONEachRow",
            &mut format!("{row}\n").as_bytes(),
        )?;
        Ok(())
    }

    fn bound(&self) -> Result<Option<analyzer_pipeline::execution::Namespace>, Error> {
        let rows = self
            .engine
            .run("SELECT identity FROM execution_namespace FINAL FORMAT JSONEachRow")?;
        let Some(row) = rows.lines().next() else {
            return Ok(None);
        };
        let row: serde_json::Value =
            serde_json::from_str(row).map_err(|e| Error::Row(e.to_string()))?;
        analyzer_pipeline::execution::Namespace::from_identity(
            row["identity"]
                .as_str()
                .ok_or_else(|| Error::Row("execution identity".into()))?,
        )
        .map(Some)
    }
}

impl From<crate::engine::Error> for Error {
    fn from(e: crate::engine::Error) -> Self {
        Self::Serving(std::sync::Arc::new(e))
    }
}
