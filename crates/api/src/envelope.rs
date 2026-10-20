// SPDX-License-Identifier: AGPL-3.0-only

//! The envelope — `docs/read-api.md` section 2.
//!
//! **Every 200 has exactly two keys: `stamp` and `data`.** Not a convention a handler remembers,
//! `envelope` combines a resolved request stamp and the operation's typed data.
//! Pagination separately proves the revision is consistent before creating page metadata.
//!
//! Every panel carries its window and the time its figures were derived, and never implies
//! scrape freshness. On the wire that is this.

use crate::error::ApiError;
use crate::ApiState;
use analyzer_analysis::Window;
use serde_json::{json, Value};

/// The freshness stamp.
///
/// **Identical across responses at steady state**, which is what makes canonical-stamp-once
/// rendering possible: the console declares the document's stamp under the title, compares each
/// response's against it, and renders only the deviations.
#[derive(Debug, Clone)]
pub struct Stamp {
    /// The one fleet this API serves, from state — named so a response can never be read
    /// as another fleet's.
    pub fleet_id: String,
    /// **`None` when no window was requested, and that is not the same as an empty one.**
    ///
    /// `/v1/stamp` answers before any tick has been applied and takes no window parameter, so it
    /// has nothing to report here. `stamp_and_window` reads a provisional stamp over a nominal
    /// `unit_window()` purely to learn the frontier; that scaffold window must never ship as a
    /// fact, or a console renders `1970-01-01`.
    ///
    /// Its two siblings in the same payload, `tick` and `raw_horizon`, say `null` when they
    /// have nothing, and so does this.
    pub window: Option<Window>,
    /// When the last tick was applied under this meaning, epoch ms as the core stated it —
    /// never the read's own time. `None` before the first tick.
    pub tick_at_ms: Option<f64>,
    /// Durable namespace revision, withheld while a tick's intent is unapplied or serving is
    /// unsettled. Internal cursor/read binding, not wire data.
    pub revision: Option<u64>,
    /// Repeat compute's classified edge, epoch ms: every store before it is classified.
    pub classified_to_ms: Option<f64>,
    /// Placement's emitted edge, epoch ms: the placement cells are stated before it.
    pub emitted_to_ms: Option<f64>,
    /// Coverage's stated edge, epoch ms: coverage is settled before it.
    pub covered_to_ms: Option<f64>,
    /// The sampled audit's edge, epoch ms: every block it re-derived before it agreed with
    /// what its tick stated.
    pub audited_to_ms: Option<f64>,
    /// The last UTC day every edge has passed by the settle delay, as a day index: through it,
    /// the day-grain outputs are stated.
    pub settled_through_day: Option<i64>,
    /// The join-semantics generation this binary computes under —
    /// `analyzer_analysis::SEMANTICS_VERSION`, the human-legible half of the meaning pair.
    pub semantics_version: &'static str,
    /// The classification-affecting parameters in force, as one readable token — the other
    /// half of the meaning pair. A change is a seam, never summed across.
    pub discriminator: String,
    /// Where the held raw records begin, engine-domain ms — MEASURED from the facts, never
    /// declared. `None` while none are held.
    pub raw_from_ms: Option<f64>,
    /// The configured raw retention horizon, in days: the declared number beside the
    /// measured one, with no inference between them.
    pub declared_raw_days: u32,
}

impl Stamp {
    /// The newest day holding classified stores: the day of the classified edge, the anchor of
    /// every relative window. `None` before any store was classified.
    #[must_use]
    pub fn classified_day(&self) -> Option<i64> {
        self.classified_to_ms.map(|ms| day_index(ms - 1.0))
    }

    /// Read the stamp for `window`.
    ///
    /// Two reads, both already owned by the DAO: the measured raw horizon and the edges the
    /// last applied tick stated. **`raw_from_ms` is measured, never declared** —
    /// `declared_raw_days` sits beside it as the configured horizon, two labelled numbers with
    /// no inference between them, which is the licence-footer pattern applied to retention.
    ///
    /// # Errors
    /// Engine failure.
    pub async fn read(
        state: &ApiState,
        window: Option<Window>,
        declared_raw_days: u32,
    ) -> Result<Self, ApiError> {
        let fleet_id = state.fleet_id().to_string();
        let (raw_from_ms, edges, durable_revision) = state
            .read(move |dao| {
                let span = dao.raw_span()?;
                Ok((span.map(|s| s.start_ms), dao.edges()?, dao.revision()?))
            })
            .await?;
        Ok(Self {
            fleet_id,
            window,
            revision: durable_revision,
            // As the core stated it, never `now`: substituting the read's own time would be
            // exactly the scrape-freshness implication read-api section 5.6 forbids.
            tick_at_ms: edges.last_tick_at_ms,
            classified_to_ms: edges.classified_to_ms,
            emitted_to_ms: edges.emitted_to_ms,
            covered_to_ms: edges.covered_to_ms,
            audited_to_ms: edges.audited_to_ms,
            settled_through_day: edges.settled_through_day,
            semantics_version: analyzer_analysis::SEMANTICS_VERSION,
            discriminator: analyzer_analysis::Discriminator::of(state.params())
                .as_str()
                .to_string(),
            raw_from_ms,
            declared_raw_days,
        })
    }

    /// The stamp as the wire carries it: `fleet_id`, `window`, `tick`, the meaning pair,
    /// `raw_horizon` and `analyzer_version`, days rendered `YYYY-MM-DD`.
    #[must_use]
    pub fn to_json(&self) -> Value {
        json!({
            "fleet_id": self.fleet_id,
            "window": self.window.as_ref().map(window_json),
            "tick": {
                "at_ms": self.tick_at_ms,
                "classified_to_ms": self.classified_to_ms,
                "classified_day": self.classified_day().map(day_string),
                "emitted_to_ms": self.emitted_to_ms,
                "covered_to_ms": self.covered_to_ms,
                "audited_to_ms": self.audited_to_ms,
                "settled_through_day": self.settled_through_day.map(day_string),
            },
            "semantics_version": self.semantics_version,
            "discriminator": self.discriminator,
            "raw_horizon": {
                "from_ms": self.raw_from_ms,
                "from_day": self.raw_from_ms.map(|ms| day_string(day_index(ms))),
                "declared_days": self.declared_raw_days,
            },
            "analyzer_version": env!("CARGO_PKG_VERSION"),
        })
    }
}

/// A window, in both forms.
///
/// **`end_ms` is exclusive; `end_day` is the last day covered.** Both are carried because a
/// consumer differencing the millisecond bounds and one reading the day labels must not arrive at
/// different lengths — which is exactly the off-by-one this pair exists to prevent.
#[must_use]
pub(crate) fn window_json(w: &Window) -> Value {
    json!({
        "start_ms": w.start_ms,
        "end_ms": w.end_ms,
        "start_day": day_string(day_index(w.start_ms)),
        "end_day": day_string(day_index((w.end_ms - 1.0).max(w.start_ms))),
    })
}

/// Days since the epoch.
#[must_use]
pub fn day_index(ms: f64) -> i64 {
    analyzer_analysis::emitted::day_index(ms)
}

/// A day index as `YYYY-MM-DD`.
#[must_use]
pub(crate) fn day_string(days: i64) -> String {
    let (y, m, d) = analyzer_analysis::emitted::civil_from_days(days);
    format!("{y:04}-{m:02}-{d:02}")
}

pub(crate) fn envelope<T: serde::Serialize>(
    stamp: &Stamp,
    data: T,
) -> axum::Json<crate::wire::Envelope<T>> {
    axum::Json(crate::wire::Envelope {
        stamp: stamp.to_json(),
        data,
    })
}

#[cfg(test)]
mod tests {
    use super::{day_index, day_string, window_json};
    use analyzer_analysis::Window;

    #[test]
    fn the_day_round_trip_survives_leap_years_and_the_epoch() {
        for (days, text) in [
            (0, "1970-01-01"),
            (-1, "1969-12-31"),
            (20_668, "2026-08-03"),
            // 2024 and 2000 are leap years; 2100 is not. A naive /4 rule gets the third wrong,
            // and it is the Gregorian trap that separates a real implementation from days / 365.
            (19_782, "2024-02-29"),
            (11_016, "2000-02-29"),
            (47_541, "2100-03-01"),
        ] {
            assert_eq!(day_string(days), text, "day {days}");
        }
    }

    #[test]
    fn a_window_states_both_forms_and_they_agree_on_its_length() {
        // The off-by-one the pair exists to prevent: a consumer differencing the ms bounds and
        // one reading the day labels must get the same fourteen days.
        let w = Window {
            start_ms: 1_784_592_000_000.0,
            end_ms: 1_785_801_600_000.0,
        };
        let v = window_json(&w);
        assert_eq!(v["start_day"], "2026-07-21");
        assert_eq!(
            v["end_day"], "2026-08-03",
            "the last day COVERED, not the bound"
        );
        let by_ms = day_index(w.end_ms) - day_index(w.start_ms);
        assert_eq!(by_ms, 14);
    }
    /// **The stamp before any tick reports absence, and does not invent a window.**
    ///
    /// `/v1/stamp` answers before anything has been classified and takes no window parameter, so
    /// the nominal epoch-day scaffold `stamp_and_window` builds must not reach the payload.
    #[test]
    fn a_stamp_before_any_tick_reports_no_window_rather_than_the_epoch() {
        let stamp = super::Stamp {
            fleet_id: "f".to_string(),
            window: None,
            revision: Some(0),
            tick_at_ms: None,
            classified_to_ms: None,
            emitted_to_ms: None,
            covered_to_ms: None,
            audited_to_ms: None,
            settled_through_day: None,
            semantics_version: "js-7",
            discriminator: "d".to_string(),
            raw_from_ms: None,
            declared_raw_days: 30,
        };
        let v = stamp.to_json();
        assert!(
            v["window"].is_null(),
            "an unrequested window must be absent, not an epoch day: {}",
            v["window"]
        );
        assert!(
            !v.to_string().contains("1970"),
            "no part of a stamp before any tick may render as an epoch date: {v}"
        );
        // Its two siblings have always answered this way; the point is that all three agree.
        assert!(v["tick"]["at_ms"].is_null());
        assert!(v["raw_horizon"]["from_day"].is_null());
    }
}
