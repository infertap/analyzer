// SPDX-License-Identifier: AGPL-3.0-only

//! The full holders table — shape 1 at holder grain, paged, with its own denominator.
//!
//! **Why this is a method rather than the API calling [`Dao::waste_by`] and dividing.** Every row
//! carries `share_of_fleet_total`, and a share whose denominator came from a second query is a
//! share of a different measurement — the two reads straddle any pass that lands between them.
//! One read, one denominator, and the ratio cannot be built any other way.

use crate::Error;
use crate::{Covered, Dao, Dim, Grain, Holder, RunStaleness, WasteRow};
use analyzer_analysis::Window;

/// What to narrow the table to. Every field is optional and absent means "no filter" — never a
/// default value, which would silently answer a narrower question than the one asked.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HolderFilters {
    /// One of the six classification buckets.
    pub class: Option<String>,
    /// A declared workload label. `unattributed` is a legal value and selects the absent bucket
    /// as a named slice, never as a gap.
    pub workload: Option<String>,
}

/// Where a page starts and how long it is.
#[derive(Debug, Clone, PartialEq)]
pub struct Page {
    /// Rows per page. The API bounds it to `1..=PAGE_LIMIT_MAX` before it reaches here.
    pub limit: u32,
    /// Exclusive: the last row of the previous page. `None` starts at the top.
    pub after: Option<HolderCursor>,
}

/// A position in the holders table's own sort order — mass descending, then the holder triple.
///
/// **The tie-break is not decoration.** Sorting by mass alone leaves rows of equal mass in
/// whatever order the engine returned them, and a page boundary landing inside such a group would
/// repeat or drop rows across pages. The triple is total, so the order is.
///
/// **This carries no stamp, deliberately.** The read layer does not know when the tick ran; the
/// API binds this position to the stamp it served the first page under and refuses a cursor
/// presented against a newer one (`docs/read-api.md` section 8). Putting the stamp here would mean
/// two layers agreeing on a serialization instead of one owning it.
#[derive(Debug, Clone, PartialEq)]
pub struct HolderCursor {
    /// The last row's recompute mass — the sort's first key, descending.
    pub tokens: f64,
    /// The last row's holder triple — the tie-break that makes the position total.
    pub holder: Holder,
}

/// One holder's row in the table.
#[derive(Debug, Clone, PartialEq)]
pub struct HolderRow {
    /// The cache this row is about: the `(instance, dp_rank, group_idx)` triple.
    pub holder: Holder,
    /// Declared on the segment (spec 2.7). Absent reads as `undeclared` — the producer
    /// declared nothing, a different absence from a config-mapping miss.
    pub workload: Option<String>,
    /// The rank's tap-process staleness verdict, judged at read time — the roster's own
    /// derivation, joined by instance (newest run wins). **State qualifies the rank now;
    /// coverage qualifies this row's figures over the window; neither implies the other**
    /// — the sentence the console's table states, now with a field that can honor it. An
    /// instance the roster never graded reads `NoBasis`, never a synthesized value.
    pub state: RunStaleness,
    /// **Observed, never declared.** Absent reads as `unobserved`, which is a different claim
    /// from `unattributed` and the two are never interchangeable.
    pub model: Option<String>,
    /// Store events classified to this holder in the window, filters applied.
    pub events: u64,
    /// Recompute mass over the window, with the coverage it was measured at.
    pub tokens: Covered<f64>,
    /// This holder's mass over the fleet's, both from the read below.
    ///
    /// **A figure, so it carries coverage** — it is a fixed-method derivation from measurements,
    /// and a share stated without the confidence of its
    /// numerator is the naked number one axis over. The coverage is the numerator's: the share
    /// is only as observed as the mass it is a share of.
    pub share_of_fleet_total: Covered<f64>,
}

/// One page of the holders table, and the denominator its shares were taken against.
#[derive(Debug, Clone, PartialEq)]
pub struct HoldersPage {
    /// This page's holders, worst first — at most [`Page::limit`] of them.
    pub rows: Vec<HolderRow>,
    /// The window's total across every holder, filters applied. **Carried rather than left to be
    /// summed from `rows`**, which would give the page's total and not the fleet's.
    pub fleet_total: Covered<f64>,
    /// How many holders the filtered table holds, so a footer can say "top 10 of 468" without a
    /// second read.
    pub total_rows: usize,
    /// `None` when this page is the last.
    pub next: Option<HolderCursor>,
}

impl Dao<'_> {
    /// One page of the holders table, worst first.
    ///
    /// **The whole filtered table is read, then sorted and sliced in memory, and the bound that
    /// makes that honest is stated rather than assumed.** At the ≤500-GPU envelope this is order
    /// hundreds to low thousands of rows: the holder-grain rollup is 100–200 K rows a day across
    /// every bucket and dimension, of which one window's holder-grain aggregate is a small slice.
    ///
    /// It buys two things a `LIMIT` in SQL cannot. The denominator is the same read as the rows,
    /// which is this method's entire reason to exist. And the sort is total (mass, then the
    /// holder triple), which a paged aggregate would have to express as a `HAVING` over a tuple
    /// comparison against an aggregate — reachable, but a query shape nobody can review.
    ///
    /// **Revisit trigger, written here so it is not rediscovered:** tier-2 fleet scale, the same
    /// trigger the hourly retention decision carries. Past the
    /// envelope this becomes a paged aggregate with a separately-read denominator, and the
    /// denominator's read must then be pinned to the same pass as the rows.
    ///
    /// # Errors
    /// Engine failure, or an unparseable row.
    pub fn holders_page(
        &self,
        window: &Window,
        now_ms: Option<f64>,
        grain: Grain,
        filters: &HolderFilters,
        page: &Page,
    ) -> Result<HoldersPage, Error> {
        // Workload and model ride along as dimensions rather than as a second lookup: they are
        // rollup keys, so grouping by them costs nothing and a per-holder join would be a second
        // measurement of the same thing.
        // The holders table keeps filtering in the FOLD rather than in SQL: it also needs the
        // unfiltered fleet total as its share denominator, so a scoped read here would shrink
        // the denominator with the rows. Scope belongs to the entity pages; this is the fleet
        // list.
        let rows = self.waste_by(
            window,
            grain,
            &[Dim::Holder, Dim::Workload, Dim::Model, Dim::Class],
            &crate::Scope::default(),
        )?;

        // Fold the buckets and the class split away: the table is one row per holder over the
        // whole window. The weighted coverage pair is summed and divided ONCE at the end, never
        // averaged per bucket and re-averaged — which is the mistake the rollup banks
        // the token-weighted pair (`sum(tokens × day fraction)`, summed once) makes impossible.
        let mut folded: std::collections::BTreeMap<Holder, Folded> =
            std::collections::BTreeMap::new();
        for row in &rows {
            if !matches(row, filters) {
                continue;
            }
            let Some(holder) = holder_of(row) else {
                continue;
            };
            let entry = folded.entry(holder).or_default();
            entry.events += row.events;
            entry.tokens += row.tokens.value;
            entry.coverage_wsum += row.tokens.value * row.tokens.coverage;
            // First non-empty wins and they cannot disagree: both are rollup keys, so every row
            // for one holder in one window carries the same pair.
            if entry.workload.is_none() {
                entry.workload = dim_of(row, Dim::Workload);
            }
            if entry.model.is_none() {
                entry.model = dim_of(row, Dim::Model);
            }
        }

        let fleet_tokens: f64 = folded.values().map(|f| f.tokens).sum();
        let fleet_wsum: f64 = folded.values().map(|f| f.coverage_wsum).sum();
        let total_rows = folded.len();

        let mut all: Vec<(Holder, Folded)> = folded.into_iter().collect();
        // Mass descending, then the holder triple ascending. Total, so paging is stable.
        all.sort_by(|(ha, a), (hb, b)| b.tokens.total_cmp(&a.tokens).then_with(|| ha.cmp(hb)));

        // **Resumed by holder IDENTITY, never by mass.** The triple is the total tie-break and is
        // unique in this map, so it locates the position exactly; comparing the float would be
        // both unnecessary and wrong, since a re-pass can move a mass by a token and there is no
        // margin that is right for every fleet.
        //
        // A cursor whose holder is no longer in the table resumes at the end, which is an empty
        // tail rather than a wrong page. That is deliberate but it is NOT the staleness defence:
        // the API compares the cursor's stamp against the current one and refuses with
        // `cursor_stale` (`docs/read-api.md` section 8), because a holder that merely MOVED would
        // otherwise resume from a position that no longer means what it did.
        let (slice, _) = page_of(&all, page.after.as_ref().map(|c| &c.holder), page.limit);

        // The staleness verdict per instance, from the roster's OWN derivation — one added
        // read, never a second opinion. Newest run wins (a holder's state is its tap process's,
        // and a restarted engine is a new process); an ungraded instance is `NoBasis`.
        let mut state_by_instance: std::collections::BTreeMap<String, (f64, RunStaleness)> =
            std::collections::BTreeMap::new();
        for run in self.roster(window, now_ms)? {
            match state_by_instance.entry(run.instance.clone()) {
                std::collections::btree_map::Entry::Vacant(slot) => {
                    slot.insert((run.last_seen_ms, run.staleness));
                }
                std::collections::btree_map::Entry::Occupied(mut slot) => {
                    if run.last_seen_ms > slot.get().0 {
                        slot.insert((run.last_seen_ms, run.staleness));
                    }
                }
            }
        }

        let rows = slice
            .iter()
            .map(|(holder, f)| HolderRow {
                holder: holder.clone(),
                state: state_by_instance
                    .get(&holder.instance)
                    .map_or(RunStaleness::NoBasis, |(_, s)| *s),
                workload: f.workload.clone(),
                model: f.model.clone(),
                events: f.events,
                tokens: covered(f.tokens, f.coverage_wsum),
                share_of_fleet_total: Covered {
                    // A zero denominator carries no share claim at all — absent, not 100 %, the
                    // same reading every other figure here applies to one.
                    value: if fleet_tokens > 0.0 {
                        f.tokens / fleet_tokens
                    } else {
                        0.0
                    },
                    register: ">=",
                    coverage: covered(f.tokens, f.coverage_wsum).coverage,
                    coverage_kind: "token-weighted",
                },
            })
            .collect();

        Ok(HoldersPage {
            rows,
            fleet_total: covered(fleet_tokens, fleet_wsum),
            total_rows,
            next: page_of(&all, page.after.as_ref().map(|c| &c.holder), page.limit)
                .1
                .map(|(holder, f)| HolderCursor {
                    tokens: f.tokens,
                    holder: holder.clone(),
                }),
        })
    }
}

#[derive(Default)]
struct Folded {
    events: u64,
    tokens: f64,
    coverage_wsum: f64,
    workload: Option<String>,
    model: Option<String>,
}

fn covered(tokens: f64, wsum: f64) -> Covered<f64> {
    Covered {
        value: tokens,
        register: ">=",
        coverage: if tokens > 0.0 { wsum / tokens } else { 0.0 },
        coverage_kind: "token-weighted",
    }
}

fn dim_of(row: &WasteRow, dim: Dim) -> Option<String> {
    row.dims
        .iter()
        .find(|(d, _)| *d == dim)
        .map(|(_, v)| v.clone())
        .filter(|v| !v.is_empty() && v != "\\N")
}

fn holder_of(row: &WasteRow) -> Option<Holder> {
    // `Dim::Holder` expands to three columns and `waste_by` zips one value per DIMENSION, so the
    // triple arrives as one already-joined value. Splitting it back is this method's business and
    // nobody else's — which is why `Dim::Holder` is one dimension rather than three.
    let raw = row.dims.iter().find(|(d, _)| *d == Dim::Holder)?.1.clone();
    let mut parts = raw.split('\t');
    let instance = parts.next()?.to_string();
    let dp_rank = parts.next()?.parse().ok()?;
    let group_idx = parts.next()?.parse().ok()?;
    Some(Holder {
        instance,
        dp_rank,
        group_idx,
    })
}

fn matches(row: &WasteRow, filters: &HolderFilters) -> bool {
    let has = |dim: Dim, want: &Option<String>| match want {
        None => true,
        Some(w) => dim_of(row, dim).as_deref() == Some(w.as_str()),
    };
    has(Dim::Class, &filters.class) && has(Dim::Workload, &filters.workload)
}

/// Cut one page out of a sorted set, and name where the next one starts.
///
/// **Extracted as a free function so the SLICING is unit-testable without an engine.** It was
/// inline in `holders_page` after a read, which meant the only thing exercising it was an
/// integration golden — and a golden is the wrong instrument for the edges here: a cursor that no
/// longer exists, a cursor on the last row, a limit past the remainder, an empty set. Those are
/// four assertions against a vector, not four database fixtures.
///
/// **Resumed by IDENTITY, exclusive.** `after` names the last row of the previous page, so this
/// one starts at the next. A cursor whose subject is gone resumes at the END rather than the
/// beginning: an empty tail is a wrong-but-bounded answer, where restarting at row one would
/// silently re-serve the whole table as if it were page two. The API's stamp check is what
/// actually catches that case; this is the behaviour underneath it.
/// One page and the row a caller resumes from, over a sorted set keyed by [`Holder`].
type Cut<'a, T> = (&'a [(Holder, T)], Option<&'a (Holder, T)>);

fn page_of<'a, T>(all: &'a [(Holder, T)], after: Option<&Holder>, limit: u32) -> Cut<'a, T> {
    let start = match after {
        None => 0,
        Some(h) => all
            .iter()
            .position(|(candidate, _)| candidate == h)
            .map_or(all.len(), |i| i + 1),
    };
    let start = all.len().min(start);
    let end = all.len().min(start.saturating_add(limit as usize));
    let slice = &all[start..end];
    // `Some` only when rows remain BEHIND this page AND this page returned something to resume
    // from. On the last page it is `None`, which is what ends a walk — a cursor still issued past
    // the end is an infinite scroll.
    //
    // **The `end > 0` guard is not defensive padding: without it this PANICS on an empty page**
    // (`all[end - 1]` underflows at `end == 0`), which the zero-limit case reaches. Unreachable
    // through the API, which caps `limit` at 1..=500 — and therefore invisible to every
    // integration test, which is why the pure unit test found it and they did not.
    let next = (end > 0 && end < all.len()).then(|| &all[end - 1]);
    (slice, next)
}

#[cfg(test)]
mod page_tests {
    use super::{page_of, Holder};

    fn set(n: u32) -> Vec<(Holder, u32)> {
        (0..n)
            .map(|i| {
                (
                    Holder {
                        instance: format!("i{i}"),
                        dp_rank: 0,
                        group_idx: 0,
                    },
                    i,
                )
            })
            .collect()
    }

    fn ids(rows: &[(Holder, u32)]) -> Vec<String> {
        rows.iter().map(|(h, _)| h.instance.clone()).collect()
    }

    #[test]
    fn the_first_page_starts_at_the_top_and_names_its_successor() {
        let all = set(5);
        let (page, next) = page_of(&all, None, 2);
        assert_eq!(ids(page), ["i0", "i1"]);
        assert_eq!(next.map(|(h, _)| h.instance.as_str()), Some("i1"));
    }

    #[test]
    fn a_cursor_resumes_strictly_after_its_subject() {
        // Inclusive resumption would repeat a row on every page boundary — the defect a reader
        // sees as a duplicate and cannot distinguish from real data.
        let all = set(5);
        let (page, _) = page_of(&all, Some(&all[1].0), 2);
        assert_eq!(ids(page), ["i2", "i3"]);
    }

    #[test]
    fn the_last_page_names_no_successor() {
        // What ends a walk. A cursor still issued past the end is an infinite scroll.
        let all = set(3);
        let (page, next) = page_of(&all, Some(&all[0].0), 10);
        assert_eq!(ids(page), ["i1", "i2"]);
        assert!(next.is_none());
    }

    #[test]
    fn a_limit_past_the_remainder_returns_the_remainder() {
        let all = set(3);
        let (page, next) = page_of(&all, None, 99);
        assert_eq!(ids(page), ["i0", "i1", "i2"]);
        assert!(next.is_none());
    }

    #[test]
    fn a_vanished_cursor_yields_an_empty_tail_rather_than_restarting() {
        // The important one. Falling back to the TOP would re-serve the whole table as page two,
        // which reads as data rather than as an error. Empty is wrong-but-bounded, and the API's
        // stamp check is what catches the case properly.
        let all = set(3);
        let gone = Holder {
            instance: "departed".into(),
            dp_rank: 0,
            group_idx: 0,
        };
        let (page, next) = page_of(&all, Some(&gone), 10);
        assert!(page.is_empty());
        assert!(next.is_none());
    }

    #[test]
    fn an_empty_set_pages_without_panicking() {
        let all = set(0);
        let (page, next) = page_of(&all, None, 10);
        assert!(page.is_empty() && next.is_none());
    }

    #[test]
    fn a_zero_limit_returns_nothing_and_issues_no_cursor() {
        // **This case PANICKED when written** — `all[end - 1]` underflows on an empty page. It is
        // unreachable through the API, which caps `limit` at 1..=500, and therefore invisible to
        // every integration test: the pure function is where a total function can be shown to be
        // total, rather than relying on a validation two layers up that a future caller might
        // bypass.
        let all = set(3);
        let (page, next) = page_of(&all, None, 0);
        assert!(page.is_empty());
        assert!(
            next.is_none(),
            "an empty page has no last row to resume from"
        );
    }
}
