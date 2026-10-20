// SPDX-License-Identifier: AGPL-3.0-only

//! `annotate`: a deploy declared into the bucket, the operator-recorded marker the attribution
//! interaction depends on.

use crate::error::Error;
use crate::settings;
use std::process::ExitCode;

/// A UTC instant `YYYY-MM-DDTHH:MM:SSZ` to epoch milliseconds.
///
/// **One accepted form, stated in the error** — offsets, fractions and local times are refused
/// rather than guessed at, because a deploy marker lands on the trend at exactly this number
/// and a silently re-interpreted instant is a marker on the wrong bucket. The date half
/// round-trips through the workspace's own calendar arithmetic, so `02-30` is refused rather
/// rather than normalised.
fn parse_utc_instant(text: &str) -> Result<f64, Error> {
    let fail = || {
        Error::usage(format!(
            "--at takes a UTC instant as YYYY-MM-DDTHH:MM:SSZ, got {text:?}"
        ))
    };
    let bytes = text.as_bytes();
    if bytes.len() != 20
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
        || bytes[19] != b'Z'
    {
        return Err(fail());
    }
    let num = |r: std::ops::Range<usize>| text[r].parse::<i64>().map_err(|_| fail());
    let (year, month, day) = (num(0..4)?, num(5..7)?, num(8..10)?);
    let (hour, minute, second) = (num(11..13)?, num(14..16)?, num(17..19)?);
    if hour > 23 || minute > 59 || second > 59 {
        return Err(fail());
    }
    let (month, day) = (
        u32::try_from(month).map_err(|_| fail())?,
        u32::try_from(day).map_err(|_| fail())?,
    );
    let days = analyzer_analysis::emitted::days_from_civil(year, month, day);
    if analyzer_analysis::emitted::civil_from_days(days) != (year, month, day) {
        return Err(fail());
    }
    #[allow(clippy::cast_precision_loss)] // epoch-ms is exact in f64 for any legal instant here
    Ok((days * 86_400_000 + (hour * 3_600 + minute * 60 + second) * 1_000) as f64)
}

/// The flags `annotate` takes, layered like `run`'s so the deployment's configuration file
/// names the bucket.
const ANNOTATE_FLAGS: &[&str] = &[
    "--config",
    "--log-level",
    "--log-format",
    "--bucket-url",
    "--bucket-prefix",
    "--at",
    "--label",
    "--scope",
];

/// `annotate`: declare a deploy, the operator-recorded marker the attribution interaction
/// depends on. It writes the annotation object into the deployment's bucket, the durable form
/// the API and a customer's pipeline write too; the running daemon's next ingest cadence
/// records it and states its marker. No engine is opened.
pub(crate) fn annotate(args: &[String]) -> Result<ExitCode, Error> {
    let flags = settings::layered_flags(args, ANNOTATE_FLAGS)?;
    let requires = "annotate";
    let at = flags.required("--at", requires)?;
    let label = flags.required("--label", requires)?;
    let at_ms = parse_utc_instant(at)?;
    let key = analyzer_daemon::declare_deploy(
        &settings::bucket_config(&flags, requires)?,
        &analyzer_serving_clickhouse::DeployAnnotation {
            at_ms,
            label: label.to_string(),
            scope: flags.value("--scope").map(str::to_string),
            source: analyzer_serving_clickhouse::DeploySource::Cli,
            // When the statement was made: now, by definition of a CLI invocation. The
            // lateness relative to --at is provenance the glass may state.
            #[allow(clippy::cast_precision_loss)] // epoch ms, far inside f64
            declared_at_ms: Some(settings::now_ms() as f64),
        },
    )?;
    println!(
        "declared deploy {label:?} at {at} ({key}); the marker appears when the next ingest \
         cycle records it"
    );
    Ok(ExitCode::SUCCESS)
}

#[cfg(test)]
mod tests {
    use super::parse_utc_instant;

    #[test]
    fn a_deploy_instant_is_one_form_and_a_real_date() {
        assert_eq!(
            parse_utc_instant("2026-09-07T00:00:01Z").ok(),
            Some(20_703.0 * 86_400_000.0 + 1_000.0)
        );
        assert!(parse_utc_instant("2026-02-30T00:00:00Z").is_err());
        assert!(parse_utc_instant("2026-09-07T00:00:01+02:00").is_err());
        assert!(parse_utc_instant("2026-09-07 00:00:01Z").is_err());
    }
}
