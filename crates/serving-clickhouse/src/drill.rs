// SPDX-License-Identifier: AGPL-3.0-only

//! The drill reads — one day, and the records behind a figure.
//!
//! **The records are bounded by where they come from, and SAY SO in the return type.** No table
//! holds an event: a block's records are re-derived from the log partitioned by block, within
//! its retention, and a holder's are its ring, its newest stores. A drill that silently returns
//! nothing past that reach reads as "no problem here", so neither read
//! returns an empty vector for a subject outside it: it returns the reach, which states the
//! bound, and the caller renders a retention fact rather than an absence of activity.

use crate::Error;
use crate::{Covered, Dao, Dim, Grain, Holder, HolderFilters, HolderRow, Page};
use analyzer_analysis::emitted::{civil_from_days, days_from_civil};
use analyzer_analysis::{Window, DAY_MS};

/// Whether a drill's source reaches a subject, and how far back it reaches if not.
///
/// **Measured, never declared** — the bound is the oldest instant the source actually holds
/// (the log's oldest held hour, or the ring's oldest store), not a configured retention. The
/// configured horizon is what retention is SUPPOSED to be, and a refusal reasoning from it would
/// state a scope the data does not have.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RawReach {
    /// Raw still covers the subject; the read ran.
    Covers,
    /// The subject predates the oldest record held. Carries the bound so the refusal can state
    /// it rather than merely assert one exists.
    Outside {
        /// The oldest instant the source holds, or `None` when it holds nothing at all — which is
        /// a different sentence and must not render as "too old".
        raw_from_ms: Option<f64>,
    },
}

impl RawReach {
    /// Whether raw still covers the subject: `true` means the read ran and its rows are the answer.
    #[must_use]
    pub fn covers(self) -> bool {
        matches!(self, Self::Covers)
    }
}

/// One UTC day, decomposed.
#[derive(Debug, Clone, PartialEq)]
pub struct DayDetail {
    /// The day, as a UTC date string.
    pub day: String,
    /// The day's whole-day window, engine-domain ms — what every figure below was read over.
    pub window: Window,
    /// Mass per classification bucket. A class with no events is ABSENT rather than zero — only
    /// a renderer knows it is printing a complete table, and only it should supply the zero.
    pub by_class: Vec<(String, Covered<f64>)>,
    /// The day's holders, worst first.
    pub holders: Vec<HolderRow>,
    /// How many holders the day holds in total, and where a next page would start.
    ///
    /// How many holders the day's table holds: `holders_page` computes it, and a response that
    /// dropped it would carry a top-50 with no page block at all — no total, no cursor, nothing
    /// saying a cut had happened.
    pub holders_total: usize,
    /// Where the next holders page starts; `None` when `holders` ends the day's table.
    pub holders_next: Option<crate::HolderCursor>,
    /// The day's own coverage, instance-time weighted.
    pub coverage: Option<Covered<f64>>,
    /// Whether event-grain evidence is still reachable for this day. **Carried rather than
    /// inferred from an empty drill**, which is the whole point.
    pub evidence: RawReach,
}

/// What the evidence read is about. Exactly one subject, because "records behind this" is always
/// behind something. A day's records are drilled by holder or by block: every store of a day is
/// every block of the day re-derived.
#[derive(Debug, Clone, PartialEq)]
pub enum EvidenceOf {
    /// One holder, over a window.
    Holder(Holder),
    /// One block, over a window.
    Block(String),
}

/// One record, as the level-2 table renders it.
///
/// Every field is the store as repeat compute classified it: re-derived from the log for a block,
/// held in the ring for a holder.
#[derive(Debug, Clone, PartialEq)]
pub struct EvidenceRecord {
    /// Stored holder identity, used by the continuation even when the renderer omits it.
    pub holder: Holder,
    /// When the store happened, engine-domain milliseconds.
    pub at_ms: f64,
    /// The engine's own id for the block stored.
    pub block_id: String,
    /// The store's classification bucket — one of the six `Class::as_str` spellings.
    pub class: String,
    /// Tokens the store carried — the mass this one record adds to the figure it is behind.
    pub n_tokens: u64,
    /// Milliseconds since this identity's most recent OBSERVED prior close on this holder,
    /// per event. `None` = no observed prior close within the examined history.
    pub gap_ms: Option<f64>,
    /// The epoch scoping the block's identity. `None` where the producer declared none.
    pub key_epoch: Option<u32>,
    /// The window-level coverage stamped onto this row by the tick that classified it.
    pub coverage: f64,
}

/// Complete continuation key for evidence across holders and identity epochs.
#[derive(Debug, Clone, PartialEq)]
pub struct EvidenceCursor {
    /// Store instant, epoch milliseconds.
    pub at_ms: f64,
    /// Block identifier within the holder.
    pub block_id: String,
    /// Holder identity breaks ties across the fleet.
    pub holder: Holder,
    /// Absent epoch sorts before every declared epoch.
    pub key_epoch: Option<u32>,
}

impl EvidenceRecord {
    /// The complete stored key, suitable for resuming strictly after this record.
    #[must_use]
    pub fn cursor(&self) -> EvidenceCursor {
        EvidenceCursor {
            at_ms: self.at_ms,
            block_id: self.block_id.clone(),
            holder: self.holder.clone(),
            key_epoch: self.key_epoch,
        }
    }
}

/// The records behind a figure, or the reason there are none to show.
#[derive(Debug, Clone, PartialEq)]
pub struct Evidence {
    /// What the records are behind — the subject the read was asked for, echoed back.
    pub subject: EvidenceOf,
    /// The window read: the caller's.
    pub window: Window,
    /// Whether raw reached the subject. `Outside` is why `records` is empty, and states the bound.
    pub reach: RawReach,
    /// Where the records came from: the holder's ring, or the block re-derived from the log.
    pub read_from: crate::ReadFrom,
    /// Empty ONLY when `reach` is [`RawReach::Covers`] — in which case it genuinely means the
    /// records were looked for and none matched.
    pub records: Vec<EvidenceRecord>,
    /// True when the read stopped at `limit` and more records exist behind it.
    pub truncated: bool,
}

impl Dao<'_> {
    /// Everything one UTC day says about itself.
    ///
    /// # Errors
    /// Engine failure, an unparseable row, or a `day` that is not a `YYYY-MM-DD` UTC date.
    pub fn day_detail(
        &self,
        day: &str,
        now_ms: Option<f64>,
        limit: u32,
        after: Option<&crate::HolderCursor>,
    ) -> Result<DayDetail, Error> {
        let window = day_window(day)?;

        let by_class = self
            .waste_by(&window, Grain::Day, &[Dim::Class], &crate::Scope::default())?
            .into_iter()
            .map(|row| {
                let class = row
                    .dims
                    .iter()
                    .find(|(d, _)| *d == Dim::Class)
                    .map_or_else(String::new, |(_, v)| v.clone());
                (class, row.tokens)
            })
            .collect();

        let holders = self.holders_page(
            &window,
            now_ms,
            Grain::Day,
            &HolderFilters::default(),
            &Page {
                limit,
                after: after.cloned(),
            },
        )?;

        // Fleet grain: one row for the day, or none where coverage was never stated. The `None`
        // is load-bearing — a day with no coverage row was not observed at 0 %, it was not
        // examined, and those render differently.
        let coverage = self
            .coverage(&window, crate::CoverageBy::Fleet)?
            .into_iter()
            .next()
            .map(|row| Covered {
                value: row.fraction,
                register: ">=",
                coverage: row.fraction,
                coverage_kind: "instance-time",
            });

        Ok(DayDetail {
            day: day.to_string(),
            window,
            by_class,
            holders_total: holders.total_rows,
            holders_next: holders.next,
            holders: holders.rows,
            coverage,
            evidence: self.raw_reach(&window)?,
        })
    }

    /// The records behind a figure, bounded by where they come from: a holder's ring, or a
    /// block re-derived from the log. The reach is stated beside them, not inferred from an
    /// empty result: one says the instrument looked and found nothing, the other that there is
    /// nothing left to look at.
    ///
    /// # Errors
    /// The working state or the log could not be read.
    pub fn evidence(
        &self,
        subject: &EvidenceOf,
        window: &Window,
        limit: u32,
    ) -> Result<Evidence, Error> {
        self.evidence_page(subject, window, limit, None)
    }

    /// [`Dao::evidence`], resumable, on `(at_ms, block_id, holder, key_epoch)`, the complete
    /// order: strictly after, so a boundary landing inside a same-instant group neither repeats
    /// nor drops.
    ///
    /// # Errors
    /// The working state or the log could not be read.
    pub fn evidence_page(
        &self,
        subject: &EvidenceOf,
        window: &Window,
        limit: u32,
        after: Option<&EvidenceCursor>,
    ) -> Result<Evidence, Error> {
        let reach = self.reach_of(subject, window)?;
        let (facts, read_from) = match subject {
            EvidenceOf::Holder(h) => (
                analyzer_pipeline::keyed::ring_of(&self.spine.lake(), h)?,
                crate::ReadFrom::Ring,
            ),
            EvidenceOf::Block(b) => (self.rederived(b, window)?, crate::ReadFrom::Log),
        };
        let order = |r: &EvidenceRecord| {
            (
                r.at_ms.to_bits(),
                r.block_id.clone(),
                r.holder.instance.clone(),
                r.holder.dp_rank,
                r.holder.group_idx,
                r.key_epoch.map_or(-1, i64::from),
            )
        };
        let mut records: Vec<EvidenceRecord> = facts
            .into_iter()
            .filter(|f| f.at_ms >= window.start_ms && f.at_ms < window.end_ms)
            .map(|f| EvidenceRecord {
                holder: f.holder,
                at_ms: f.at_ms,
                block_id: f.block,
                class: f.class.as_str().to_string(),
                n_tokens: u64::try_from(f.n_tokens).unwrap_or(0),
                gap_ms: f.gap_ms,
                key_epoch: f.key_epoch,
                coverage: f.coverage,
            })
            .collect();
        records.sort_by_key(order);
        if let Some(c) = after {
            let at = (
                c.at_ms.to_bits(),
                c.block_id.clone(),
                c.holder.instance.clone(),
                c.holder.dp_rank,
                c.holder.group_idx,
                c.key_epoch.map_or(-1, i64::from),
            );
            records.retain(|r| order(r) > at);
        }
        let truncated = records.len() > limit as usize;
        records.truncate(usize::try_from(limit).unwrap_or(usize::MAX));
        Ok(Evidence {
            read_from,
            subject: subject.clone(),
            window: *window,
            reach,
            records,
            truncated,
        })
    }

    /// Whether the log partitioned by block reaches `window`: what a block's records and a
    /// day's drills are re-derived from. Public — the drill endpoints state the bound on their
    /// responses rather than leaving a consumer to infer it from an empty list.
    ///
    /// # Errors
    /// The working state could not be read.
    pub fn raw_reach_of(&self, window: &Window) -> Result<RawReach, Error> {
        self.raw_reach(window)
    }

    /// The reach a read of `subject` over `window` has: the holder's ring's for a holder, the
    /// log's for a block.
    ///
    /// # Errors
    /// The working state could not be read.
    pub fn reach_of(&self, subject: &EvidenceOf, window: &Window) -> Result<RawReach, Error> {
        match subject {
            EvidenceOf::Holder(h) => {
                let from = analyzer_pipeline::keyed::ring_of(&self.spine.lake(), h)?
                    .first()
                    .map(|f| f.at_ms);
                Ok(reach(from, window))
            }
            EvidenceOf::Block(_) => self.raw_reach(window),
        }
    }

    /// A window is covered when it OVERLAPS what the log holds: a fourteen-day window on a
    /// fleet an hour old answers for the hour, with the bound beside it, and only a window
    /// entirely before the oldest held hour is refused.
    fn raw_reach(&self, window: &Window) -> Result<RawReach, Error> {
        Ok(reach(self.log_reach_from_ms()?, window))
    }
}

/// Whether a source holding records from `from` reaches `window`.
fn reach(from: Option<f64>, window: &Window) -> RawReach {
    if from.is_some_and(|f| f < window.end_ms) {
        RawReach::Covers
    } else {
        RawReach::Outside { raw_from_ms: from }
    }
}

/// `YYYY-MM-DD` to the half-open millisecond window of that UTC day.
///
/// **Parsed rather than passed through to SQL**, which is what keeps a date parameter from being
/// a place a caller's string reaches a statement. The engine never sees this text.
///
/// Public because it is the ONE definition of what a legal day is: the API layer validates its
/// `day` parameters through this same function, so a malformed date is refused there with its
/// own cause instead of surfacing here as an engine error — and the two layers cannot disagree
/// about which strings are days.
///
/// # Errors
/// A string that is not a `YYYY-MM-DD` UTC date.
pub fn day_window(day: &str) -> Result<Window, Error> {
    let bad = || Error::Refused(format!("{day:?} is not a YYYY-MM-DD UTC date"));
    let mut parts = day.split('-');
    let y: i64 = parts.next().ok_or_else(bad)?.parse().map_err(|_| bad())?;
    let m: u32 = parts.next().ok_or_else(bad)?.parse().map_err(|_| bad())?;
    let d: u32 = parts.next().ok_or_else(bad)?.parse().map_err(|_| bad())?;
    if parts.next().is_some() || !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return Err(bad());
    }
    // A day that does not exist (`02-30`) is refused, never normalised onto a neighbour.
    let days = days_from_civil(y, m, d);
    if civil_from_days(days) != (y, m, d) {
        return Err(bad());
    }
    #[allow(clippy::cast_precision_loss)] // day indices are order 20 000
    Ok(Window {
        start_ms: days as f64 * DAY_MS,
        end_ms: (days + 1) as f64 * DAY_MS,
    })
}

#[cfg(test)]
mod tests {
    use super::day_window;
    use analyzer_analysis::emitted::days_from_civil;
    use analyzer_analysis::DAY_MS;

    #[test]
    fn the_epoch_and_a_leap_day_land_where_they_should() {
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(days_from_civil(1969, 12, 31), -1);
        // 2024 is a leap year; 2100 is not. A naive /4 rule gets the second wrong.
        assert_eq!(
            days_from_civil(2024, 3, 1) - days_from_civil(2024, 2, 28),
            2,
            "2024-02-29 exists"
        );
        assert_eq!(
            days_from_civil(2100, 3, 1) - days_from_civil(2100, 2, 28),
            1,
            "2100-02-29 does not"
        );
    }

    // Exact by construction -- see the note on the same assertion in `changed`: an epoch-ms day
    // boundary is an integer well inside f64, and a tolerance here would mask an off-by-one.
    #[allow(clippy::float_cmp)]
    #[test]
    fn a_day_window_is_one_day_wide_and_half_open() {
        let w = day_window("2026-08-03").expect("parses");
        assert_eq!(w.end_ms - w.start_ms, DAY_MS);
        // the value docs/read-api.md section 2.1's worked example carries
        assert_eq!(w.end_ms, 1_785_801_600_000.0, "2026-08-04T00:00:00Z");
    }

    #[test]
    fn a_malformed_date_is_refused_rather_than_reaching_a_statement() {
        for bad in [
            "2026-08",
            "2026-08-03-01",
            "not-a-date",
            "2026-13-01",
            "2026-08-00",
            "2026-08-03' OR '1'='1",
        ] {
            assert!(day_window(bad).is_err(), "{bad:?} must be refused");
        }
    }
}
