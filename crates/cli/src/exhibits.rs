// SPDX-License-Identifier: AGPL-3.0-only

//! The compliance exhibits: `schema dump`, the running schema, and `engine-config dump`, the
//! engine's configuration. Neither starts a server.

use crate::error::Error;
use crate::flags::Flags;
use crate::settings;
use std::process::ExitCode;

/// The engine's configuration as an artifact, the `schema dump` pattern at its second
/// surface. No engine is started and none is spawned: the exhibit is a pure function of the
/// configuration (or a read of what is already on disk), which is why an auditor can be
/// handed it on a box where nothing is running.
pub(crate) fn engine_config_dump(args: &[String]) -> Result<ExitCode, Error> {
    let flags = Flags::parse(args, &["--data-dir", "--http-port", "--tcp-port", "--user"])?;
    let data_dir = flags.required("--data-dir", "engine-config dump")?;
    let mut config = analyzer_serving_clickhouse::engine::server::ServerConfig::new(data_dir);
    if let Some(port) = flags.parsed("--http-port", "a port number")? {
        config.http_port = port;
    }
    if let Some(port) = flags.parsed("--tcp-port", "a port number")? {
        config.tcp_port = port;
    }
    if let Some(user) = flags.value("--user") {
        config.user = user.to_string();
    }
    print!(
        "{}",
        analyzer_serving_clickhouse::engine::server::engine_config_exhibit(&config)?
    );
    Ok(ExitCode::SUCCESS)
}

/// `schema dump`: the running schema, the compliance exhibit.
pub(crate) fn schema_dump(args: &[String]) -> Result<ExitCode, Error> {
    let flags = Flags::parse(args, &["--data-dir", "--clickhouse"])?;
    let data_dir = flags.required("--data-dir", "schema dump")?;
    let bin = settings::resolve_bin(flags.value("--clickhouse"));
    let session = analyzer_serving_clickhouse::engine::server::Session::open(&bin, data_dir)?;
    print!(
        "{}",
        analyzer_serving_clickhouse::engine::schema_dump(session.engine())?
    );
    Ok(ExitCode::SUCCESS)
}
