// SPDX-License-Identifier: AGPL-3.0-only

//! The two artifact forms: the JSON document and the Prometheus text exposition, each written
//! atomically.

use super::{SelfHealth, Status};
use analyzer_pipeline::Error;
use std::fmt::Write as _;
use std::path::PathBuf;

impl SelfHealth {
    /// Worst-wins overall status.
    #[must_use]
    pub fn status(&self) -> Status {
        self.checks
            .iter()
            .map(|c| c.status)
            .max()
            .unwrap_or(Status::Unknown)
    }

    /// A metric's value by name — the loop's accessor, so control decisions read the
    /// same numbers the dashboard does rather than a parallel computation.
    #[must_use]
    pub fn value(&self, name: &str) -> Option<f64> {
        self.metrics
            .iter()
            .find(|m| m.name == name)
            .and_then(|m| m.value)
    }

    /// Prometheus text-exposition rendering (**format version 0.0.4**, the format the
    /// `node_exporter` textfile collector parses from `*.prom` files). Deliberately
    /// file-shaped: an airgapped deployment gets metrics with no listener, no port, and
    /// no static-audit fence exception.
    ///
    /// Conformed to the spec, each point checked against it rather than assumed:
    /// exactly one `# HELP` and one `# TYPE` line per metric name, both before that
    /// metric's first sample; `\` and newlines escaped in help text; `\`, `"` and
    /// newlines escaped in label values; the last line ends with a line feed; and
    /// non-finite values render as `+Inf` / `-Inf` / `NaN` rather than Rust's `inf`,
    /// which Go's `ParseFloat` — and therefore every scraper — rejects. No timestamps:
    /// the textfile collector ignores them by documented design.
    ///
    /// Not `OpenMetrics`: that dialect requires a trailing `# EOF` and its own content
    /// type. The `.prom` textfile path wants this one.
    #[must_use]
    pub fn to_prometheus(&self) -> String {
        let mut out = String::new();
        let identity = match &self.analyzer_id {
            Some(id) => format!(",analyzer_id=\"{}\"", escape_label(id)),
            None => String::new(),
        };
        let _ = writeln!(
            out,
            "# HELP infertap_analyzer_info Analyzer identity; the labels are the payload."
        );
        let _ = writeln!(out, "# TYPE infertap_analyzer_info gauge");
        let _ = writeln!(
            out,
            "infertap_analyzer_info{{version=\"{}\"{identity}}} 1",
            env!("CARGO_PKG_VERSION")
        );
        if let Some((semantics, discriminator)) = &self.meaning {
            let _ = writeln!(
                out,
                "# HELP infertap_fleet_meaning_info The meaning pair the fleet figures \
                 were computed under; the labels are the payload. Figures either side of \
                 a change are different quantities."
            );
            let _ = writeln!(out, "# TYPE infertap_fleet_meaning_info gauge");
            let _ = writeln!(
                out,
                "infertap_fleet_meaning_info{{semantics_version=\"{}\",discriminator=\"{}\"}} 1",
                escape_label(semantics),
                escape_label(discriminator)
            );
        }
        for m in &self.metrics {
            // an unmeasured metric is an ABSENT series, the format's own idiom for "no
            // data" — never a zero, which reads as a healthy measurement
            let Some(value) = m.value else { continue };
            let _ = writeln!(out, "# HELP {} {}", m.name, escape_help(m.help));
            let _ = writeln!(out, "# TYPE {} {}", m.name, m.kind.as_str());
            let _ = writeln!(out, "{} {}", m.name, format_value(value));
        }
        let _ = writeln!(
            out,
            "# HELP infertap_analyzer_metrics_unmeasured Metrics this sample could not \
             measure (absent series above)."
        );
        let _ = writeln!(out, "# TYPE infertap_analyzer_metrics_unmeasured gauge");
        let _ = writeln!(
            out,
            "infertap_analyzer_metrics_unmeasured {}",
            self.unmeasured().len()
        );
        let _ = writeln!(
            out,
            "# HELP infertap_analyzer_check_status Per-check status: \
             0 ok, 1 unknown, 2 warn, 3 critical."
        );
        let _ = writeln!(out, "# TYPE infertap_analyzer_check_status gauge");
        for c in &self.checks {
            let _ = writeln!(
                out,
                "infertap_analyzer_check_status{{check=\"{}\"}} {}",
                escape_label(c.name),
                status_code(c.status)
            );
        }
        out
    }

    /// The JSON artifact as text. Serialization lives HERE, behind the crate boundary,
    /// because the CLI shell's dependency allowlist is deliberately empty — the shell
    /// composes, it does not serialize.
    ///
    /// # Errors
    /// Serialization failure, which for this document shape is not reachable — note
    /// that a non-finite value would NOT be one: `serde_json` renders those as `null`
    /// and returns Ok, which is exactly why [`Metric`](super::Metric) forbids them upstream.
    pub fn to_json_string(&self) -> Result<String, Error> {
        serde_json::to_string_pretty(&self.to_json())
            .map_err(|e| Error::Derivation(format!("health json: {e}")))
    }

    /// The machine-readable artifact: same numbers, same names, for a log shipper or a
    /// dashboard that prefers JSON to exposition format.
    #[must_use]
    pub fn to_json(&self) -> serde_json::Value {
        // `null` for unmeasured is JSON's honest rendering — and because a reader
        // cannot tell OUR null from a serializer's silent non-finite null, the
        // unmeasured set is named explicitly beside it rather than inferred
        let metrics: serde_json::Map<String, serde_json::Value> = self
            .metrics
            .iter()
            .map(|m| (m.name.to_string(), serde_json::json!(m.value)))
            .collect();
        let checks: Vec<serde_json::Value> = self
            .checks
            .iter()
            .map(|c| {
                serde_json::json!({
                    "check": c.name,
                    "status": c.status.as_str(),
                    "message": c.message,
                })
            })
            .collect();
        serde_json::json!({
            "schema": "infertap-analyzer-selfhealth-1",
            "version": env!("CARGO_PKG_VERSION"),
            "analyzer_id": self.analyzer_id,
            "fleet": self.fleet,
            "meaning": self.meaning.as_ref().map(|(semantics, discriminator)| {
                serde_json::json!({
                    "semantics_version": semantics,
                    "discriminator": discriminator,
                })
            }),
            "status": self.status().as_str(),
            "metrics": metrics,
            "unmeasured": self.unmeasured(),
            "checks": checks,
        })
    }

    /// Metric names this sample could not measure. Exported both ways: as this list in
    /// JSON and as a count in the Prometheus rendering, so "the analyzer stopped being
    /// able to see something" is itself an alarmable condition.
    #[must_use]
    pub fn unmeasured(&self) -> Vec<&'static str> {
        self.metrics
            .iter()
            .filter(|m| m.value.is_none())
            .map(|m| m.name)
            .collect()
    }
}

/// Help text: backslash and line feed are the two escapes the format defines.
fn escape_help(text: &str) -> String {
    text.replace('\\', "\\\\").replace('\n', "\\n")
}

/// Label values additionally escape the double quote that delimits them.
fn escape_label(text: &str) -> String {
    text.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

/// The exposition format takes a Go-`ParseFloat`-parseable number, or the literals
/// `NaN`, `+Inf`, `-Inf`. Rust renders those three as `NaN`, `inf`, `-inf` — two of
/// which every scraper rejects, turning one bad division into a silently unparsed file
/// in an environment where nobody is watching.
fn format_value(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if value.is_infinite() {
        if value.is_sign_positive() {
            "+Inf".to_string()
        } else {
            "-Inf".to_string()
        }
    } else {
        value.to_string()
    }
}

fn status_code(status: Status) -> u8 {
    match status {
        Status::Ok => 0,
        Status::Unknown => 1,
        Status::Warn => 2,
        Status::Critical => 3,
    }
}

/// Write through a temp file in the SAME directory, then rename. The `node_exporter`
/// textfile collector re-reads `*.prom` on every scrape and may catch a partial write;
/// rename is atomic within a filesystem, so a scraper sees the previous file or the new
/// one, never half of one. Same-directory placement is what keeps the rename atomic —
/// across filesystems it degrades to a copy.
fn write_atomic(path: &std::path::Path, bytes: &[u8]) -> Result<(), Error> {
    let dir = path.parent().unwrap_or_else(|| std::path::Path::new("."));
    let name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("health");
    let tmp = dir.join(format!(".{name}.{}.tmp", std::process::id()));
    std::fs::write(&tmp, bytes)
        .map_err(|e| Error::Derivation(format!("writing {}: {e}", tmp.display())))?;
    std::fs::rename(&tmp, path).map_err(|e| {
        std::fs::remove_file(&tmp).ok();
        Error::Derivation(format!(
            "renaming {} -> {}: {e}",
            tmp.display(),
            path.display()
        ))
    })
}

/// The two health artifacts one write produces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthArtifacts {
    /// `health.json`: the document form.
    pub json: PathBuf,
    /// `health.prom`: the Prometheus text form.
    pub prom: PathBuf,
}

/// Write `health.json` and `health.prom` into `out_dir`, atomically, and return their
/// paths.
///
/// Public because the daemon's sampler emits on its OWN cadence, faster than the report's,
/// and needs the same writer rather than a second one. Two writers of the same artifact is
/// exactly the shape that drifts — one gaining an escape rule or a rounding the other
/// lacks — so there is one, and both callers reach it.
///
/// # Errors
/// If either artifact cannot be serialized or written.
pub fn write_health_artifacts(
    out_dir: &std::path::Path,
    vitals: &SelfHealth,
) -> Result<HealthArtifacts, Error> {
    let json = out_dir.join("health.json");
    write_atomic(&json, vitals.to_json_string()?.as_bytes())?;
    let prom = out_dir.join("health.prom");
    write_atomic(&prom, vitals.to_prometheus().as_bytes())?;
    Ok(HealthArtifacts { json, prom })
}
