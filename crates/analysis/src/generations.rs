// SPDX-License-Identifier: AGPL-3.0-only

//! The generation seams (`markers`): where the meaning figures are stated under changes, from
//! the day ranges each meaning (a semantics version and a discriminator) classified.
//!
//! A baseline for the first generation, a TRANSITION where one generation's coverage hands off
//! to the next, and a coverage label where two generations describe the same days, which
//! history forks rather than steps. The seams are a function of which days each generation
//! classified, never of when anything ran.

use crate::module::Row;

/// One generation's coverage: its meaning pair and the first and last day it classified, as
/// epoch milliseconds of the days' starts.
#[derive(Debug, Clone, PartialEq)]
pub struct Generation {
    /// The semantics version the generation classified under.
    pub semantics_version: String,
    /// The discriminator of the parameters it classified under.
    pub discriminator: String,
    /// The start of the first day it classified, epoch milliseconds.
    pub first_ms: f64,
    /// The start of the last day it classified, epoch milliseconds.
    pub last_ms: f64,
}

impl Generation {
    /// The order the seams are stated in: by first day, then last, then the meaning pair.
    fn order(a: &Self, b: &Self) -> std::cmp::Ordering {
        a.first_ms
            .total_cmp(&b.first_ms)
            .then(a.last_ms.total_cmp(&b.last_ms))
            .then_with(|| {
                (&a.semantics_version, &a.discriminator)
                    .cmp(&(&b.semantics_version, &b.discriminator))
            })
    }
}

/// The seam rows of `generations`, in [`Generation`]'s order.
#[must_use]
pub fn seam_rows(generations: &[Generation]) -> Vec<Row> {
    let mut generations: Vec<&Generation> = generations.iter().collect();
    generations.sort_by(|a, b| Generation::order(a, b));
    // A baseline for the first generation, a TRANSITION where coverage hands off, and a coverage
    // label where two generations describe the same buckets. A generation whose value repeats the
    // one in force writes nothing: the seam is the change, not the presence.
    let mut rows: Vec<Row> = Vec::new();
    for (kind, subject, axis) in [
        ("param_change", "analysis parameters", 1usize),
        ("semantics_change", "join semantics", 0usize),
    ] {
        // The generation in force and how far its coverage reaches. The FRONTIER rather than the
        // previous row, because ranges nest: a generation covering days 3–4 inside another's 1–5
        // does not end the outer one, and the next handoff is from whatever actually reached
        // furthest.
        let mut frontier: Option<(&str, f64)> = None;
        for g in &generations {
            let current: &str = if axis == 0 {
                &g.semantics_version
            } else {
                &g.discriminator
            };
            let (first, last) = (g.first_ms, g.last_ms);
            let describe = |value: &str| {
                if axis == 0 {
                    value.to_string()
                } else {
                    crate::Discriminator::observed(value).describe()
                }
            };
            let (label, from) = match frontier {
                None => (
                    format!("{subject} established: {}", describe(current)),
                    None,
                ),
                Some((held, _)) if held == current => {
                    frontier = Some((current, last.max(frontier.map_or(last, |f| f.1))));
                    continue;
                }
                // Disjoint: the previous generation's coverage ends before this one's begins, so
                // the trend really does step here and both sides are nameable.
                Some((held, reach)) if first > reach => (
                    format!(
                        "{subject} changed: {} -> {}",
                        describe(held),
                        describe(current)
                    ),
                    Some(held),
                ),
                // Overlapping: both describe the same days. No step, no direction — the history
                // forks and the reader chooses. `from_value` stays NULL so this does not read as
                // a transition to anything that walks the chain.
                Some(_) => (
                    format!("{subject} also covers these buckets: {}", describe(current)),
                    None,
                ),
            };
            rows.push(
                crate::emitted::Marker {
                    at_ms: first,
                    kind,
                    label: &label,
                    provenance: "recorded",
                    from_value: from,
                    to_value: Some(current),
                    source: "analyzer",
                    scope: None,
                    declared_at_ms: None,
                }
                .row(false),
            );
            frontier = Some((current, frontier.map_or(last, |f| f.1.max(last))));
        }
    }

    rows
}

#[cfg(test)]
mod tests {
    use super::seam_rows;
    use crate::module::NULL_TEXT;
    use crate::DAY_MS;

    fn g(semantics: &str, discriminator: &str, first: f64, last: f64) -> super::Generation {
        super::Generation {
            semantics_version: semantics.into(),
            discriminator: discriminator.into(),
            first_ms: first * DAY_MS,
            last_ms: last * DAY_MS,
        }
    }

    fn labels(rows: &[crate::module::Row]) -> Vec<String> {
        rows.iter().map(|r| r.key[2].text()).collect()
    }

    #[test]
    fn a_handoff_is_a_change_and_an_overlap_is_a_fork() {
        let base = seam_rows(&[g("js-6", "s1", 1.0, 3.0)]);
        assert_eq!(base.len(), 2, "one baseline per axis");
        let changed = seam_rows(&[g("js-6", "s1", 1.0, 3.0), g("js-6", "s2", 4.0, 6.0)]);
        let stated = labels(&changed);
        assert!(stated
            .iter()
            .any(|l| l.starts_with("analysis parameters changed")));
        assert!(changed
            .iter()
            .any(|r| !r.measures[1].text().is_empty() && r.measures[1].text() != NULL_TEXT));
        let forked = seam_rows(&[g("js-6", "s1", 1.0, 5.0), g("js-6", "s2", 3.0, 6.0)]);
        let fork = forked
            .iter()
            .find(|r| {
                r.key[2]
                    .text()
                    .starts_with("analysis parameters also covers")
            })
            .expect("a fork");
        assert_eq!(
            fork.measures[1].text(),
            NULL_TEXT,
            "a fork names no from value"
        );
    }
}
