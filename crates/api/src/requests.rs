// SPDX-License-Identifier: AGPL-3.0-only

//! Raw HTTP parameters and validated request values.

use crate::envelope::Stamp;
use crate::error::{ApiError, Cause};
use crate::ApiState;
use analyzer_analysis::{Window, DAY_MS};
use analyzer_serving_clickhouse::{Dim, Grain};
use serde::{Deserialize, Serialize};
const HOUR_MS: f64 = 3_600_000.0;
/// The most rows any paged surface will return in one response.
///
/// **Derived from RESPONSE SIZE, and deliberately not from fleet size.** A holders row is ~339 B
/// compact (measured against the wire shape), so 200 rows is ~66 KiB — a response that needs no
/// streaming, no virtualisation, and no apology.
///
/// **It is NOT a fleet size.** Holders are `(instance, dp_rank, group_idx)` triples, so a 500-GPU
/// fleet has more than 500 of them, and a cap that read as "one row per GPU" would sometimes not
/// fit the fleet in a page while promising it would.
///
/// **The second term is that paging must be the NORMAL path.** At 500 a console could request the
/// whole table in one call and never exercise the cursor, so the mechanism would sit unrun until
/// the envelope grew and every consumer that had skipped it truncated silently. Below a typical
/// fleet's holder count, paging happens on every real use. (It cannot force the issue on a small
/// deployment whose whole fleet fits in one page — that is what goldening at `limit = 1` is for.)
///
/// Re-derived when a deployment measures a real holders response against a real console's render
/// budget; the row size is the term that would move it.
pub const PAGE_LIMIT_MAX: u32 = 200;

/// Placement's bound is in PAIRS, and pairs are quadratic in holders: 50 engines is 1,225 of
/// them, 100 is 4,950, 300 is 44,850. The default carries a fleet of about sixty engines whole
/// and the ceiling carries one of about a hundred and forty; past that the response says what it
/// left out and what that was worth, which is the only honest way to serve a quadratic.
pub const PLACEMENT_PAIRS_DEFAULT: u32 = 2_000;
pub const PLACEMENT_PAIRS_MAX: u32 = 10_000;

/// The whale panel's bounds: glass default 8,
/// limit capped ~100 — top-N is a read-time ORDER BY over the identity × day ledger, and
/// the cap is what keeps "show me more" from becoming an identity-table export.
pub const WHALES_DEFAULT: u32 = 8;
pub const WHALES_MAX: u32 = 100;

pub(crate) const GRAINS: &[&str] = &["auto", "minute", "hour", "day"];
pub(crate) const DIMS: &[&str] = &["class", "holder", "instance", "workload", "model"];
pub(crate) const COVERAGE_BY: &[&str] = &["fleet", "instance"];

/// The six classification buckets, from `analysis`' own closed enum rather than retyped.
#[must_use]
pub(crate) fn classes() -> Vec<&'static str> {
    analyzer_analysis::classify::Class::ALL
        .iter()
        .map(analyzer_analysis::classify::Class::as_str)
        .collect()
}

fn resolve_window(spec: Option<&str>, now_ms: f64) -> Result<Window, ApiError> {
    let spec = spec.unwrap_or("14d");
    if let Some((a, b)) = spec.split_once(',') {
        let parse = |v: &str| {
            v.trim()
                .parse::<f64>()
                .map_err(|_| ApiError::new(Cause::WindowInvalid, format!("not an instant: {v}")))
        };
        let (start_ms, end_ms) = (parse(a)?, parse(b)?);
        if !start_ms.is_finite() || !end_ms.is_finite() {
            return Err(ApiError::new(
                Cause::WindowInvalid,
                "a window's instants are finite epoch milliseconds",
            ));
        }
        if end_ms <= start_ms {
            return Err(ApiError::new(
                Cause::WindowInvalid,
                "the window ends at or before it starts",
            ));
        }
        return Ok(Window { start_ms, end_ms });
    }
    let relative = |unit: char, unit_ms: f64| {
        spec.strip_suffix(unit)
            .and_then(|n| n.parse::<u32>().ok())
            .filter(|n| *n > 0)
            .map(|n| f64::from(n) * unit_ms)
    };
    let span_ms = relative('d', DAY_MS)
        .or_else(|| relative('h', HOUR_MS))
        .ok_or_else(|| {
            ApiError::new(
                Cause::WindowInvalid,
                format!("{spec:?} is not `<N>d`, `<N>h` or `start_ms,end_ms`"),
            )
        })?;
    // A day span ends at the coming midnight; a sub-day span at the edge of the bucket its
    // grain draws, so a stop's buckets are whole and two calls inside one bucket agree.
    #[allow(clippy::cast_precision_loss)] // bucket widths are small integers
    let step = if spec.ends_with('d') {
        DAY_MS
    } else {
        Grain::natural(span_ms).bucket_ms() as f64
    };
    let end_ms = ((now_ms / step).floor() + 1.0) * step;
    Ok(Window {
        start_ms: end_ms - span_ms,
        end_ms,
    })
}
fn grain(spec: Option<&str>) -> Result<Option<Grain>, ApiError> {
    match spec {
        None | Some("auto") => Ok(None),
        Some("day") => Ok(Some(Grain::Day)),
        Some("hour") => Ok(Some(Grain::Hour)),
        Some("minute") => Ok(Some(Grain::Minute)),
        Some(other) => Err(ApiError::new(
            Cause::GrainUnavailable,
            format!("{other:?} is not a grain; the tiers are `minute`, `hour`, `day`, `auto`"),
        )),
    }
}
fn dims(spec: Option<&str>) -> Result<Vec<Dim>, ApiError> {
    let Some(spec) = spec.filter(|s| !s.is_empty()) else {
        return Ok(Vec::new());
    };
    spec.split(',')
        .map(|d| match d.trim() {
            "class" => Ok(Dim::Class),
            "holder" => Ok(Dim::Holder),
            "instance" => Ok(Dim::Instance),
            "workload" => Ok(Dim::Workload),
            "model" => Ok(Dim::Model),
            other => Err(ApiError::new(
                Cause::DimUnknown,
                format!(
                    "{other:?} is not a dimension; the closed set is \
                         class, holder, instance, workload, model"
                ),
            )),
        })
        .collect()
}
fn class(spec: Option<&str>) -> Result<Option<String>, ApiError> {
    match spec {
        None => Ok(None),
        Some(c) if classes().contains(&c) => Ok(Some(c.to_string())),
        Some(other) => Err(ApiError::new(
            Cause::DimUnknown,
            format!(
                "{other:?} is not a class; the closed set is {}",
                classes().join(", ")
            ),
        )),
    }
}
pub(crate) fn validate_day(day: &str) -> Result<Window, ApiError> {
    analyzer_serving_clickhouse::drill::day_window(day)
        .map_err(|e| ApiError::new(Cause::WindowInvalid, e.to_string()))
}
pub(crate) fn parse_holder(spec: &str) -> Result<analyzer_serving_clickhouse::Holder, ApiError> {
    let bad = || {
        ApiError::new(
            Cause::UnknownHolder,
            format!("{spec:?} is not `instance/dp_rank/group_idx`"),
        )
    };
    let mut parts = spec.split('/');
    let instance = parts.next().filter(|s| !s.is_empty()).ok_or_else(bad)?;
    let dp_rank = parts.next().ok_or_else(bad)?.parse().map_err(|_| bad())?;
    let group_idx = parts.next().ok_or_else(bad)?.parse().map_err(|_| bad())?;
    if parts.next().is_some() {
        return Err(bad());
    }
    Ok(analyzer_serving_clickhouse::Holder {
        instance: instance.to_string(),
        dp_rank,
        group_idx,
    })
}

fn limit(value: Option<u32>, default: u32, max: u32) -> Result<u32, ApiError> {
    match value.unwrap_or(default) {
        n if (1..=max).contains(&n) => Ok(n),
        n => Err(ApiError::new(
            Cause::LimitInvalid,
            format!("limit {n} is outside 1..={max}"),
        )),
    }
}

/// A finite resolved question. The clock is sampled exactly once by its constructor.
#[derive(Debug, Clone, Copy)]
pub(crate) struct WindowRequest {
    window: Window,
    now: f64,
}
impl WindowRequest {
    pub fn window(&self) -> Window {
        self.window
    }
    pub fn now(&self) -> f64 {
        self.now
    }
}
impl WindowRequest {
    fn new(spec: Option<&str>, now: f64) -> Result<Self, ApiError> {
        let window = resolve_window(spec, now)?;
        if !window.start_ms.is_finite()
            || !window.end_ms.is_finite()
            || !(window.end_ms - window.start_ms).is_finite()
        {
            return Err(ApiError::new(
                Cause::WindowInvalid,
                "a window has finite bounds and span",
            ));
        }
        Ok(Self { window, now })
    }
}
pub(crate) async fn stamp_and_window(
    state: &ApiState,
    request: &WindowRequest,
) -> Result<(Stamp, Window), ApiError> {
    let window = request.window;
    let stamp = Stamp::read(state, Some(window), state.declared_ledger_days()).await?;
    Ok((stamp, window))
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WindowQuery {
    window: Option<String>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FiguresQuery {
    window: Option<String>,
    grain: Option<String>,
    dims: Option<String>,
    workload: Option<String>,
    instance: Option<String>,
    holder: Option<String>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BandsQuery {
    window: Option<String>,
    grain: Option<String>,
    dims: Option<String>,
    workload: Option<String>,
    instance: Option<String>,
    holder: Option<String>,
    limit: Option<u32>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HoldersQuery {
    window: Option<String>,
    grain: Option<String>,
    class: Option<String>,
    workload: Option<String>,
    limit: Option<u32>,
    cursor: Option<String>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CoverageQuery {
    window: Option<String>,
    by: Option<String>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RosterQuery {
    window: Option<String>,
    limit: Option<u32>,
    cursor: Option<String>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PlacementQuery {
    window: Option<String>,
    limit: Option<u32>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WhalesQuery {
    window: Option<String>,
    limit: Option<u32>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReplicationQuery {
    window: Option<String>,
    grain: Option<String>,
    workload: Option<String>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct InstanceDuplicationQuery {
    window: Option<String>,
    grain: Option<String>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DayQuery {
    limit: Option<u32>,
    cursor: Option<String>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EvidenceQuery {
    window: Option<String>,
    block: Option<String>,
    holder: Option<String>,
    limit: Option<u32>,
    cursor: Option<String>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BlocksQuery {
    window: Option<String>,
    limit: Option<u32>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DrillQuery {
    window: Option<String>,
    limit: Option<u32>,
    cursor: Option<String>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NoQuery {}

impl WindowQuery {
    pub fn validate(self, now: f64) -> Result<WindowRequest, ApiError> {
        WindowRequest::new(self.window.as_deref(), now)
    }
}

pub(crate) struct FiguresRequest {
    context: WindowRequest,
    grain: Option<Grain>,
    dims: Vec<Dim>,
    scope: analyzer_serving_clickhouse::Scope,
    minute: analyzer_serving_clickhouse::MinuteTier,
}
impl FiguresRequest {
    pub fn context(&self) -> WindowRequest {
        self.context
    }
    pub fn grain(&self) -> Option<Grain> {
        self.grain
    }
    pub fn dims(&self) -> Vec<Dim> {
        self.dims.clone()
    }
    pub fn scope(&self) -> analyzer_serving_clickhouse::Scope {
        self.scope.clone()
    }
    pub fn minute(&self) -> analyzer_serving_clickhouse::MinuteTier {
        self.minute
    }
}
impl FiguresQuery {
    pub fn validate(self, now: f64) -> Result<FiguresRequest, ApiError> {
        self.validate_with_message(
            now,
            "the minute grain serves the fleet, class, workload, model and instance cuts, \
             and an instance or holder scope, from cells kept per rank; a holder or tier \
             cut has no minute table — drop it, scope to the instance or the holder, or \
             use `hour`",
        )
    }
    fn validate_with_message(self, now: f64, message: &str) -> Result<FiguresRequest, ApiError> {
        let dims = dims(self.dims.as_deref())?;
        let scope = analyzer_serving_clickhouse::Scope {
            workload: self.workload,
            instance: self.instance,
            holder: self.holder.as_deref().map(parse_holder).transpose()?,
        };
        let requested = grain(self.grain.as_deref())?;
        let minute = analyzer_serving_clickhouse::MinuteTier::of(&dims, &scope);
        if requested == Some(Grain::Minute)
            && minute == analyzer_serving_clickhouse::MinuteTier::Unavailable
        {
            return Err(ApiError::new(Cause::GrainUnavailable, message));
        }
        Ok(FiguresRequest {
            context: WindowRequest::new(self.window.as_deref(), now)?,
            grain: requested,
            dims,
            scope,
            minute,
        })
    }
}
pub(crate) struct BandsRequest {
    figures: FiguresRequest,
    custom_window: bool,
    limit: usize,
}
impl BandsRequest {
    pub fn figures(&self) -> &FiguresRequest {
        &self.figures
    }
    pub fn custom_window(&self) -> bool {
        self.custom_window
    }
    pub fn limit(&self) -> usize {
        self.limit
    }
}
impl BandsQuery {
    pub fn validate(self, now: f64) -> Result<BandsRequest, ApiError> {
        let limit = limit(self.limit, WHALES_DEFAULT, WHALES_MAX)? as usize;
        let custom_window = self.window.as_deref().is_some_and(|w| w.contains(','));
        let figures = FiguresQuery {
            window: self.window,
            grain: self.grain,
            dims: self.dims,
            workload: self.workload,
            instance: self.instance,
            holder: self.holder,
        }
        .validate_with_message(
            now,
            "the minute grain keeps its banded cells per rank with cache groups and tiers \
             rolled up; a holder or tier cut has no minute table — scope to the instance \
             or the holder, or ask at `hour`",
        )?;
        Ok(BandsRequest {
            figures,
            custom_window,
            limit,
        })
    }
}
pub(crate) struct HoldersRequest {
    context: WindowRequest,
    grain: Option<Grain>,
    filters: analyzer_serving_clickhouse::HolderFilters,
    paging: crate::pagination::Input,
}
impl HoldersRequest {
    pub fn context(&self) -> WindowRequest {
        self.context
    }
    pub fn grain(&self) -> Option<Grain> {
        self.grain
    }
    pub fn filters(&self) -> analyzer_serving_clickhouse::HolderFilters {
        self.filters.clone()
    }
    pub fn paging(&self) -> &crate::pagination::Input {
        &self.paging
    }
}
impl HoldersQuery {
    pub fn validate(self, now: f64) -> Result<HoldersRequest, ApiError> {
        let window = WindowRequest::new(self.window.as_deref(), now)?;
        let requested = grain(self.grain.as_deref())?;
        if requested == Some(Grain::Minute) {
            return Err(ApiError::new(Cause::GrainUnavailable,
                "the minute grain serves the recompute figures at fleet and class cuts; the holders read offers `hour` and `day`"));
        }
        Ok(HoldersRequest {
            context: window,
            grain: requested,
            filters: analyzer_serving_clickhouse::HolderFilters {
                class: class(self.class.as_deref())?,
                workload: self.workload.clone(),
            },
            paging: crate::pagination::Input::new(
                &self,
                self.cursor.as_deref(),
                limit(self.limit, 100, PAGE_LIMIT_MAX)?,
            )?,
        })
    }
}
pub(crate) struct CoverageRequest {
    context: WindowRequest,
    by: analyzer_serving_clickhouse::CoverageBy,
}
impl CoverageRequest {
    pub fn context(&self) -> WindowRequest {
        self.context
    }
    pub fn by(&self) -> analyzer_serving_clickhouse::CoverageBy {
        self.by
    }
}
impl CoverageQuery {
    pub fn validate(self, now: f64) -> Result<CoverageRequest, ApiError> {
        let by = match self.by.as_deref().unwrap_or("fleet") {
            "fleet" => analyzer_serving_clickhouse::CoverageBy::Fleet,
            "instance" => analyzer_serving_clickhouse::CoverageBy::Instance,
            other => {
                return Err(ApiError::new(
                    Cause::DimUnknown,
                    format!("{other:?} is not a coverage grouping; use `fleet` or `instance`"),
                ))
            }
        };
        Ok(CoverageRequest {
            context: WindowRequest::new(self.window.as_deref(), now)?,
            by,
        })
    }
}
pub(crate) struct LimitedRequest {
    context: WindowRequest,
    limit: u32,
}
impl LimitedRequest {
    pub fn context(&self) -> WindowRequest {
        self.context
    }
    pub fn limit(&self) -> u32 {
        self.limit
    }
}
impl PlacementQuery {
    pub fn validate(self, now: f64) -> Result<LimitedRequest, ApiError> {
        Ok(LimitedRequest {
            context: WindowRequest::new(self.window.as_deref(), now)?,
            limit: limit(self.limit, PLACEMENT_PAIRS_DEFAULT, PLACEMENT_PAIRS_MAX)?,
        })
    }
}
impl WhalesQuery {
    pub fn validate(self, now: f64) -> Result<LimitedRequest, ApiError> {
        Ok(LimitedRequest {
            context: WindowRequest::new(self.window.as_deref(), now)?,
            limit: limit(self.limit, WHALES_DEFAULT, WHALES_MAX)?,
        })
    }
}
impl BlocksQuery {
    pub fn validate(self, now: f64) -> Result<LimitedRequest, ApiError> {
        Ok(LimitedRequest {
            context: WindowRequest::new(self.window.as_deref(), now)?,
            limit: limit(self.limit, 50, PAGE_LIMIT_MAX)?,
        })
    }
}

pub(crate) struct ReplicationRequest {
    context: WindowRequest,
    grain: Option<Grain>,
    workload: Option<String>,
    custom_window: bool,
}
impl ReplicationRequest {
    pub fn context(&self) -> WindowRequest {
        self.context
    }
    pub fn grain(&self) -> Option<Grain> {
        self.grain
    }
    pub fn workload(&self) -> Option<String> {
        self.workload.clone()
    }
    pub fn custom_window(&self) -> bool {
        self.custom_window
    }
}
impl ReplicationQuery {
    pub fn validate(self, now: f64) -> Result<ReplicationRequest, ApiError> {
        Ok(ReplicationRequest {
            context: WindowRequest::new(self.window.as_deref(), now)?,
            grain: grain(self.grain.as_deref())?,
            workload: self.workload,
            custom_window: self.window.as_deref().is_some_and(|w| w.contains(',')),
        })
    }
}
pub(crate) struct InstanceDuplicationRequest {
    context: WindowRequest,
    grain: Option<Grain>,
}
impl InstanceDuplicationRequest {
    pub fn context(&self) -> WindowRequest {
        self.context
    }
    pub fn grain(&self) -> Option<Grain> {
        self.grain
    }
}
impl InstanceDuplicationQuery {
    pub fn validate(self, now: f64) -> Result<InstanceDuplicationRequest, ApiError> {
        Ok(InstanceDuplicationRequest {
            context: WindowRequest::new(self.window.as_deref(), now)?,
            grain: grain(self.grain.as_deref())?,
        })
    }
}
pub(crate) struct PagedWindow {
    context: WindowRequest,
    paging: crate::pagination::Input,
}
impl PagedWindow {
    pub fn context(&self) -> WindowRequest {
        self.context
    }
    pub fn paging(&self) -> &crate::pagination::Input {
        &self.paging
    }
}
impl RosterQuery {
    pub fn validate(self, now: f64) -> Result<PagedWindow, ApiError> {
        Ok(PagedWindow {
            context: WindowRequest::new(self.window.as_deref(), now)?,
            paging: crate::pagination::Input::new(
                &self,
                self.cursor.as_deref(),
                limit(self.limit, 100, PAGE_LIMIT_MAX)?,
            )?,
        })
    }
}
impl DrillQuery {
    pub fn validate(self, now: f64) -> Result<PagedWindow, ApiError> {
        Ok(PagedWindow {
            context: WindowRequest::new(self.window.as_deref(), now)?,
            paging: crate::pagination::Input::new(
                &self,
                self.cursor.as_deref(),
                limit(self.limit, 100, PAGE_LIMIT_MAX)?,
            )?,
        })
    }
}

pub(crate) struct DayRequest {
    day: String,
    context: WindowRequest,
    paging: crate::pagination::Input,
}
impl DayRequest {
    pub fn day(&self) -> String {
        self.day.clone()
    }
    pub fn context(&self) -> WindowRequest {
        self.context
    }
    pub fn paging(&self) -> &crate::pagination::Input {
        &self.paging
    }
}
impl DayQuery {
    pub fn validate(self, day: String, now: f64) -> Result<DayRequest, ApiError> {
        Ok(DayRequest {
            context: WindowRequest {
                window: validate_day(&day)?,
                now,
            },
            day,
            paging: crate::pagination::Input::new(
                &self,
                self.cursor.as_deref(),
                limit(self.limit, 50, PAGE_LIMIT_MAX)?,
            )?,
        })
    }
}
pub(crate) struct EvidenceRequest {
    context: WindowRequest,
    subject: analyzer_serving_clickhouse::EvidenceOf,
    paging: crate::pagination::Input,
}
impl EvidenceRequest {
    pub fn context(&self) -> WindowRequest {
        self.context
    }
    pub fn subject(&self) -> analyzer_serving_clickhouse::EvidenceOf {
        self.subject.clone()
    }
    pub fn paging(&self) -> &crate::pagination::Input {
        &self.paging
    }
}
impl EvidenceQuery {
    pub fn validate(self, now: f64) -> Result<EvidenceRequest, ApiError> {
        if self.block.is_some() && self.holder.is_some() {
            return Err(ApiError::new(
                Cause::RequestInvalid,
                "evidence requires exactly one of `block` or `holder`",
            ));
        }
        let subject = if let Some(block) = &self.block {
            analyzer_serving_clickhouse::EvidenceOf::Block(block.clone())
        } else if let Some(holder) = &self.holder {
            analyzer_serving_clickhouse::EvidenceOf::Holder(parse_holder(holder)?)
        } else {
            return Err(ApiError::new(
                Cause::SubjectMissing,
                "evidence is always behind something: pass one of `block` or `holder`",
            ));
        };
        let context = WindowRequest::new(self.window.as_deref(), now)?;
        Ok(EvidenceRequest {
            context,
            subject,
            paging: crate::pagination::Input::new(
                &self,
                self.cursor.as_deref(),
                limit(self.limit, 100, PAGE_LIMIT_MAX)?,
            )?,
        })
    }
}

/// A declared deploy, as the console states it. The recorded-declaration class's one body
/// (`docs/read-api.md`: endpoints may record declarations; no endpoint alters a measurement).
#[derive(Deserialize, utoipa::ToSchema)]
pub(crate) struct AnnotationBody {
    /// The instant the deploy is ABOUT — where the marker sits on the trend. UTC epoch ms;
    /// the declaration is authoritative about its own instant.
    pub at_ms: f64,
    /// The words on the marker. Trimmed; 1..=256 bytes.
    pub label: String,
    /// The declarer's stated lens, or absent. Never a blast radius — the platform infers
    /// nothing from it.
    #[serde(default)]
    pub scope: Option<String>,
}

pub(crate) struct AnnotationRequest(analyzer_serving_clickhouse::DeployAnnotation);
impl AnnotationBody {
    pub fn validate(self, now: f64) -> Result<AnnotationRequest, ApiError> {
        let annotation = analyzer_serving_clickhouse::DeployAnnotation {
            at_ms: self.at_ms,
            label: self.label,
            scope: self.scope,
            source: analyzer_serving_clickhouse::DeploySource::Api,
            declared_at_ms: Some(now),
        };
        annotation
            .validate()
            .map_err(|e| ApiError::new(Cause::AnnotationInvalid, e.to_string()))?;
        Ok(AnnotationRequest(annotation))
    }
}
impl AnnotationRequest {
    pub fn into_annotation(self) -> analyzer_serving_clickhouse::DeployAnnotation {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::float_cmp)] // exact bucket boundaries are the contract
    fn windows_are_resolved_once_and_explicit_edges_remain_exact() {
        let relative = WindowQuery {
            window: Some("3h".into()),
        }
        .validate(123_456.0)
        .unwrap();
        assert_eq!(relative.window.end_ms, 180_000.0);
        assert_eq!(relative.window.start_ms, 180_000.0 - 10_800_000.0);
        assert_eq!(relative.now, 123_456.0);
        let explicit = WindowQuery {
            window: Some(" 1.5, 2.5 ".into()),
        }
        .validate(0.0)
        .unwrap();
        assert_eq!(
            explicit.window,
            Window {
                start_ms: 1.5,
                end_ms: 2.5
            }
        );
        for spec in ["0d", "NaN,3", "1,inf", "3,2", "-1e308,1e308"] {
            assert!(
                WindowQuery {
                    window: Some(spec.into())
                }
                .validate(0.0)
                .is_err(),
                "{spec}"
            );
        }
    }

    #[test]
    fn operation_invariants_are_established_without_a_database() {
        let malformed: HoldersQuery =
            serde_json::from_value(serde_json::json!({"cursor":"not-a-cursor"})).unwrap();
        assert_eq!(
            malformed.validate(0.0).err().unwrap().cause,
            Cause::RequestInvalid
        );
        let bad: HoldersQuery =
            serde_json::from_value(serde_json::json!({"class":"unsupported"})).unwrap();
        assert_eq!(bad.validate(0.0).err().unwrap().cause, Cause::DimUnknown);
        let bad: FiguresQuery =
            serde_json::from_value(serde_json::json!({"grain":"minute", "dims":"holder"})).unwrap();
        assert_eq!(
            bad.validate(0.0).err().unwrap().cause,
            Cause::GrainUnavailable
        );
        let bad: EvidenceQuery =
            serde_json::from_value(serde_json::json!({"block":"b", "holder":"i/0/0"})).unwrap();
        assert_eq!(
            bad.validate(0.0).err().unwrap().cause,
            Cause::RequestInvalid
        );
        assert!(
            serde_json::from_value::<EvidenceQuery>(serde_json::json!({"day":"2026-09-20"}))
                .is_err(),
            "a day's records are drilled by holder or by block"
        );
        let missing = EvidenceQuery::default().validate(0.0).err().unwrap();
        assert_eq!(missing.cause, Cause::SubjectMissing);
        for n in [0, PAGE_LIMIT_MAX + 1] {
            let bad: DrillQuery = serde_json::from_value(serde_json::json!({"limit":n})).unwrap();
            assert_eq!(bad.validate(0.0).err().unwrap().cause, Cause::LimitInvalid);
        }
        assert!(
            serde_json::from_value::<CoverageQuery>(serde_json::json!({"class":"cold"})).is_err()
        );
    }
    #[test]
    fn documented_defaults_and_vocabularies_match_request_constructors() {
        let doc = crate::openapi::document();
        for (path, actual) in [
            (
                "/v1/holders",
                HoldersQuery::default()
                    .validate(0.0)
                    .unwrap()
                    .paging()
                    .limit(),
            ),
            (
                "/v1/roster",
                RosterQuery::default()
                    .validate(0.0)
                    .unwrap()
                    .paging()
                    .limit(),
            ),
            (
                "/v1/days/{day}",
                DayQuery::default()
                    .validate("2026-09-17".into(), 0.0)
                    .unwrap()
                    .paging()
                    .limit(),
            ),
            (
                "/v1/blocks",
                BlocksQuery::default().validate(0.0).unwrap().limit(),
            ),
            (
                "/v1/blocks/{block_id}/holders",
                DrillQuery::default()
                    .validate(0.0)
                    .unwrap()
                    .paging()
                    .limit(),
            ),
            (
                "/v1/residual/placement",
                PlacementQuery::default().validate(0.0).unwrap().limit(),
            ),
            (
                "/v1/residual/whales",
                WhalesQuery::default().validate(0.0).unwrap().limit(),
            ),
        ] {
            let parameter = doc["paths"][path]["get"]["parameters"]
                .as_array()
                .unwrap()
                .iter()
                .find(|p| p["name"] == "limit")
                .unwrap();
            assert_eq!(parameter["schema"]["default"], actual, "{path}");
        }
        for value in GRAINS {
            assert!(grain(Some(value)).is_ok(), "{value}");
        }
        for value in DIMS {
            assert!(dims(Some(value)).is_ok(), "{value}");
        }
        for value in classes() {
            assert!(class(Some(value)).is_ok(), "{value}");
        }
    }
}
