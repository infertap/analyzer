// SPDX-License-Identifier: AGPL-3.0-only

//! The standing differential audit: evict resolution, re-derived and diffed.
//!
//! The tap resolves evict identity at capture time through a map built at store time.
//! `block_id` rides both stores and evicts, so the raw stream
//! contains everything needed to re-derive that resolution independently: join each evict to
//! its most recent prior store on `(instance_id, dp_rank, group_idx, block_id)` and compare
//! the store's `content_id` against the one the evict actually carried. Any disagreement is
//! an alarm naming the instance.
//!
//! This is a **permanent runtime detector for the capture-time-resolution defect class**,
//! not a migration test. Each tick folds its records on a sample of engine ids through
//! [`EvictAudit`], and the daemon surfaces the grades in self-health (`evict_resolution`) and the
//! metric export; [`audit_from_events`] grades a whole window the same way. Three independent defect
//! families all land here: bridge-class bugs (a map that resolves wrongly or forgets what it
//! promised), collision announcements (a merged identity claims two parents, and its evicts
//! disagree with its stores), and unobserved event shapes (a clear whose assumed scope
//! over- or under-released shows up as evicts the re-derivation grades differently).
//!
//! ## What the diff can and cannot claim
//!
//! Every audited evict lands in exactly one class, and the classes must partition — an audit
//! that cannot account for every evict refuses to report rather than reporting around the
//! gap.
//!
//! - **agreed** — the evict carries what the latest prior store carried, including both
//!   carrying nothing: a store that could not chain makes an evict that cannot resolve, and
//!   matching absence is agreement.
//! - **mismatched** — both sides carry an identity and they differ. Within one engine
//!   process the same engine hash names the same tokens, so the re-derivation and the tap
//!   cannot honestly disagree; a mismatch is the defect class this audit exists to catch,
//!   and it is the silent-wrong direction — identity joins over the affected instance are
//!   quietly corrupt. Alarms hard.
//! - **underresolved** — the evict carries nothing where the store, written by the **same
//!   tap run**, carries an identity. The tap held the promise and lost it: capacity refusal
//!   (declared via `identity_refused`), an over-released clear,
//!   or a bridge defect. Declared-missing rather than silent-wrong, so it warns rather than
//!   alarms — but it never passes silently.
//! - **indeterminate** — the evidence cannot grade the evict: the joined store is from a
//!   different tap run (a fresh bridge legitimately resolves nothing it did not see stored),
//!   from an unattributed segment (provenance unknown, so "same run" cannot be claimed), or
//!   the latest store itself carries no identity while the evict does (the tap's value came
//!   from an earlier store whose entry legitimately outlived a non-chaining re-store).
//! - **unjoined** — no prior store in the window at all: residency predating observation or
//!   the retention horizon. Nothing to diff, and counted so the audit's own coverage is
//!   declared rather than implied.
//!
//! The join is strictly *prior* (`store.at_ms < evict.at_ms`), never same-instant: a batch
//! stamps one timestamp on every event it carries, and an evict-before-store inside one
//! batch would otherwise be graded against a store the tap had not yet seen. The cost is
//! honest: blocks stored and evicted within a single batch fall to `unjoined` instead of
//! being audited against an order the timestamps cannot establish.
//!
//! Clock discipline: engine `at_ms` on both sides and nothing else — `incarnation` is
//! identity, not time (query inventory Q8 in `docs/conformance.md`).

/// One instance's audit, over the window it was computed for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceAudit {
    /// The instance every audited evict here named — the alarm's subject.
    pub instance: String,
    /// Evicts examined (the classes below partition this exactly).
    pub audited: u64,
    /// Evicts carrying what the latest prior store carried, both carrying nothing included.
    pub agreed: u64,
    /// Evicts whose identity differs from the latest prior store's, both present: the alarm class.
    pub mismatched: u64,
    /// Evicts carrying nothing where the same tap run's store carried an identity: a lost promise.
    pub underresolved: u64,
    /// Evicts the evidence cannot grade: a different run, an unattributed segment, or a store
    /// with no identity while the evict has one.
    pub indeterminate: u64,
    /// Evicts with no strictly prior store in the window — nothing to diff, counted for coverage.
    pub unjoined: u64,
}

/// The audit across a fleet: per-instance rows, in instance order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AuditOutcome {
    /// One row per instance with an audited evict, in instance order.
    pub instances: Vec<InstanceAudit>,
}

impl AuditOutcome {
    /// Evicts examined fleet-wide — every instance's `audited`, summed.
    #[must_use]
    pub fn audited(&self) -> u64 {
        self.instances.iter().map(|i| i.audited).sum()
    }

    /// Fleet-wide `mismatched`, summed over the instances.
    #[must_use]
    pub fn mismatched(&self) -> u64 {
        self.instances.iter().map(|i| i.mismatched).sum()
    }

    /// Fleet-wide `underresolved`, summed over the instances.
    #[must_use]
    pub fn underresolved(&self) -> u64 {
        self.instances.iter().map(|i| i.underresolved).sum()
    }

    /// Fleet-wide `indeterminate`, summed over the instances.
    #[must_use]
    pub fn indeterminate(&self) -> u64 {
        self.instances.iter().map(|i| i.indeterminate).sum()
    }

    /// Fleet-wide `unjoined`, summed over the instances.
    #[must_use]
    pub fn unjoined(&self) -> u64 {
        self.instances.iter().map(|i| i.unjoined).sum()
    }

    /// The instances a disagreement names, with their counts — the alarm's payload.
    #[must_use]
    pub fn naming(&self, count: impl Fn(&InstanceAudit) -> u64) -> String {
        self.instances
            .iter()
            .filter(|i| count(i) > 0)
            .map(|i| format!("{} ({})", i.instance, count(i)))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// The re-derivation, folded from the Ordered Event IR — the store-free path.
///
/// **The `ASOF LEFT JOIN` in plain terms**: each evict pairs with the most recent store on the
/// full holder scope plus `block_id` whose stamp is STRICTLY earlier. Strictly, never same-instant,
/// because a batch stamps one timestamp on every event it carries — an evict-before-store inside
/// one batch would otherwise be graded against a store the tap had not yet seen. The cost is
/// honest: blocks stored and evicted within a single batch fall to `unjoined` rather than being
/// audited against an order the timestamps cannot establish.
///
/// **The five classes must partition**, and the caller enforces it: an audit that cannot account
/// for every evict refuses to report rather than reporting around the gap.
#[must_use]
pub fn audit_from_events(ir: &[crate::events::Event]) -> AuditOutcome {
    use std::collections::BTreeMap;

    // The join key is the full holder scope plus the ENGINE id — never the resolved identity,
    // since what this audit tests is precisely whether that resolution was right.
    fn key(e: &crate::events::Event) -> Option<(&str, u32, u32, &str)> {
        let (dp, grp) = e.holder_dims();
        Some((e.instance.as_str(), dp, grp, e.block_id.as_deref()?))
    }

    let mut stores: BTreeMap<(&str, u32, u32, &str), Vec<&crate::events::Event>> = BTreeMap::new();
    for ev in ir
        .iter()
        .filter(|e| e.kind == crate::events::Kind::Store && e.block_id.is_some())
    {
        if let Some(k) = key(ev) {
            stores.entry(k).or_default().push(ev);
        }
    }
    for group in stores.values_mut() {
        // Total, where the join's tie-break among equal stamps is the engine's own.
        group.sort_by(|a, b| {
            a.at_ms
                .total_cmp(&b.at_ms)
                .then_with(|| a.content_id.cmp(&b.content_id))
                .then_with(|| a.incarnation.cmp(&b.incarnation))
        });
    }

    // The reset boundaries. spec 2.3 makes each an IDENTITY boundary — a reader MUST NOT
    // match a `block_id` across one — so a pre-boundary store is not a join candidate for a
    // post-boundary evict. The corpus fixture pinning this graded the unguarded join's answer
    // a false lost-promise alarm on its first run. The loader has already ordered the record
    // at its engine-clock boundary (`boundary_ms` rides in as `at_ms`), and close-before-
    // stores makes the boundary instant itself belong to the fresh cache. A reset is a
    // PROCESS boundary: global over group and tier, and over every rank where the record
    // names none — the same coverage `scope_covers` gives the interval fold.
    let resets: Vec<&crate::events::Event> = ir
        .iter()
        .filter(|e| e.kind == crate::events::Kind::HolderReset)
        .collect();
    let separated = |store: &crate::events::Event, evict: &crate::events::Event| {
        let (dp, _) = evict.holder_dims();
        resets.iter().any(|r| {
            r.instance == evict.instance
                && r.dp_rank.is_none_or(|d| d == dp)
                && store.at_ms < r.at_ms
                && evict.at_ms >= r.at_ms
        })
    };

    let mut per_instance: BTreeMap<&str, InstanceAudit> = BTreeMap::new();
    for ev in ir
        .iter()
        .filter(|e| e.kind == crate::events::Kind::Evict && e.block_id.is_some())
    {
        let row = per_instance
            .entry(ev.instance.as_str())
            .or_insert_with(|| empty_row(&ev.instance));
        row.audited += 1;

        let prior = key(ev).and_then(|k| stores.get(&k)).and_then(|g| {
            g.iter()
                .rev()
                .find(|s| s.at_ms < ev.at_ms && !separated(s, ev))
        });

        let Some(store) = prior else {
            row.unjoined += 1;
            continue;
        };

        grade(
            row,
            (ev.content_id.as_deref(), ev.incarnation.as_str()),
            (store.content_id.as_deref(), store.incarnation.as_str()),
        );
    }

    AuditOutcome {
        instances: per_instance.into_values().collect(),
    }
}

/// Grade one joined evict into `row`, from the evict's and its store's identity and run.
fn grade(
    row: &mut InstanceAudit,
    (ec, einc): (Option<&str>, &str),
    (sc, sinc): (Option<&str>, &str),
) {
    match (ec, sc) {
        // Matching absence IS agreement: a store that could not chain makes an evict that
        // cannot resolve, and both carrying nothing is the honest pair.
        (None, None) => row.agreed += 1,
        (Some(e), Some(s)) if e == s => row.agreed += 1,
        (Some(_), Some(_)) => row.mismatched += 1,
        // The tap held a promise and lost it — but only claimable where the SAME run wrote
        // both. A fresh bridge legitimately resolves nothing it did not see stored.
        (None, Some(_)) if einc == sinc && !einc.is_empty() => row.underresolved += 1,
        // Everything else is unprovable: a different run, an unattributed segment, or a
        // store carrying no identity while the evict does (the tap's value came from an
        // earlier store whose entry outlived a non-chaining re-store).
        _ => row.indeterminate += 1,
    }
}

fn empty_row(instance: &str) -> InstanceAudit {
    InstanceAudit {
        instance: instance.to_string(),
        audited: 0,
        agreed: 0,
        mismatched: 0,
        underresolved: 0,
        indeterminate: 0,
        unjoined: 0,
    }
}

/// The holder scope and engine id a store and its evicts join on.
type JoinKey = (String, u32, u32, String);
/// The latest store on a key: its instant, identity and run, and where an evict closed the
/// residency it opened.
#[derive(Debug, Clone)]
struct Latest {
    at: f64,
    content: Option<String>,
    run: String,
    closed: Option<f64>,
}

/// The same audit as [`audit_from_events`], folded as records arrive: what a running deployment
/// takes each tick over a sample of engine ids.
///
/// It keeps the latest store per holder and engine id for the ids `keep` admits, so a store and
/// its evicts are always graded together; the sample is drawn on the engine id because the
/// resolved identity is what the audit tests. Fed one window's records at once, it grades as
/// [`audit_from_events`] does. Fed tick by tick, a record behind the latest store on its key
/// cannot be ordered against it: a store older than the latest is not kept, and an evict at or
/// before the latest store is `unjoined`. A reset drops the stores before it on its scope, and
/// [`EvictAudit::forget_before`] the stores past the horizons the caller keeps: a store still
/// open for as long as an evict of it may yet come, a closed one for as long as a second evict
/// of it may.
#[derive(Debug, Default)]
pub struct EvictAudit {
    latest: std::collections::BTreeMap<JoinKey, Latest>,
    graded: std::collections::BTreeMap<String, InstanceAudit>,
}

impl EvictAudit {
    /// Fold `events`, the stores, evicts and resets on the engine ids `keep` admits.
    pub fn observe(&mut self, events: &[crate::events::Event], keep: &dyn Fn(&str) -> bool) {
        use crate::events::Kind;
        let rank = |k: Kind| match k {
            Kind::HolderReset => 0,
            Kind::Evict => 1,
            _ => 2,
        };
        let mut relevant: Vec<&crate::events::Event> = events
            .iter()
            .filter(|e| match e.kind {
                Kind::HolderReset => true,
                Kind::Store | Kind::Evict => e.block_id.as_deref().is_some_and(keep),
                // A clear names no engine id, and a silence is the analyzer's, not the engine's.
                Kind::Clear | Kind::Silence => false,
            })
            .collect();
        relevant.sort_by(|a, b| {
            a.at_ms
                .total_cmp(&b.at_ms)
                .then_with(|| rank(a.kind).cmp(&rank(b.kind)))
        });
        for ev in relevant {
            let (dp, grp) = ev.holder_dims();
            match ev.kind {
                Kind::HolderReset => self.latest.retain(|(instance, d, _, _), latest| {
                    !(instance == &ev.instance
                        && ev.dp_rank.is_none_or(|r| r == *d)
                        && latest.at < ev.at_ms)
                }),
                Kind::Store => {
                    let key = (
                        ev.instance.clone(),
                        dp,
                        grp,
                        ev.block_id.clone().unwrap_or_default(),
                    );
                    let seen = Latest {
                        at: ev.at_ms,
                        content: ev.content_id.clone(),
                        run: ev.incarnation.clone(),
                        closed: None,
                    };
                    let newer = self.latest.get(&key).is_none_or(|old| {
                        old.at
                            .total_cmp(&seen.at)
                            .then_with(|| old.content.cmp(&seen.content))
                            .then_with(|| old.run.cmp(&seen.run))
                            .is_le()
                    });
                    if newer {
                        self.latest.insert(key, seen);
                    }
                }
                Kind::Evict | Kind::Clear | Kind::Silence => {
                    let key = (
                        ev.instance.clone(),
                        dp,
                        grp,
                        ev.block_id.clone().unwrap_or_default(),
                    );
                    let row = self
                        .graded
                        .entry(ev.instance.clone())
                        .or_insert_with(|| empty_row(&ev.instance));
                    row.audited += 1;
                    match self.latest.get_mut(&key).filter(|s| s.at < ev.at_ms) {
                        None => row.unjoined += 1,
                        Some(latest) => {
                            grade(
                                row,
                                (ev.content_id.as_deref(), ev.incarnation.as_str()),
                                (latest.content.as_deref(), latest.run.as_str()),
                            );
                            latest.closed =
                                Some(latest.closed.map_or(ev.at_ms, |c| c.max(ev.at_ms)));
                        }
                    }
                }
            }
        }
    }

    /// Forget the stores still open from before `open_before_ms`, and those an evict closed
    /// before `closed_before_ms`: an evict after them is `unjoined`.
    pub fn forget_before(&mut self, open_before_ms: f64, closed_before_ms: f64) {
        self.latest.retain(|_, latest| match latest.closed {
            None => latest.at >= open_before_ms,
            Some(closed) => closed >= closed_before_ms,
        });
    }

    /// The stores kept.
    #[must_use]
    pub fn kept(&self) -> usize {
        self.latest.len()
    }

    /// The evicts graded since the last call.
    pub fn take(&mut self) -> AuditOutcome {
        AuditOutcome {
            instances: std::mem::take(&mut self.graded).into_values().collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{audit_from_events, EvictAudit};
    use crate::events::{Event, Kind};

    /// A stream with every case the join has: ties at one instant, two runs, identities present
    /// and absent, and resets on one rank and on every rank.
    fn stream() -> Vec<Event> {
        let mut state: u64 = 0x9e37_79b9_7f4a_7c15;
        let mut next = |n: u64| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (state >> 33) % n
        };
        (0..4_000)
            .map(|_| {
                let kind = match next(40) {
                    0 => Kind::HolderReset,
                    1..=21 => Kind::Store,
                    _ => Kind::Evict,
                };
                let reset = kind == Kind::HolderReset;
                #[allow(clippy::cast_precision_loss)] // small integers
                let at_ms = next(1_500) as f64;
                Event {
                    kind,
                    instance: format!("i{}", next(3)),
                    dp_rank: if reset && next(2) == 0 {
                        None
                    } else {
                        Some(u32::try_from(next(2)).expect("small"))
                    },
                    group_idx: Some(0),
                    block_id: (!reset).then(|| format!("b{}", next(12))),
                    content_id: (!reset && next(4) != 0).then(|| format!("c{}", next(3))),
                    key_epoch: None,
                    tier: None,
                    backend_id: None,
                    at_ms,
                    locality: None,
                    n_tokens: 16,
                    parent_id: None,
                    parent_unknown: false,
                    delivery_id: None,
                    incarnation: format!("run-{}", next(2)),
                    seq: None,
                    reused: false,
                    workload_class: None,
                }
            })
            .collect()
    }

    #[test]
    fn the_streamed_audit_grades_as_the_window_audit_whole_or_in_time_order() {
        let events = stream();
        let want = audit_from_events(&events);
        assert!(want.mismatched() > 0 && want.underresolved() > 0 && want.unjoined() > 0);

        let mut whole = EvictAudit::default();
        whole.observe(&events, &|_| true);
        assert_eq!(whole.take(), want);

        // Tick by tick, each tick's records later than the last's: the same grades.
        let mut ticks = EvictAudit::default();
        for t in 0..15 {
            #[allow(clippy::cast_precision_loss)] // small integers
            let (from, to) = (f64::from(t) * 100.0, f64::from(t + 1) * 100.0);
            let tick: Vec<Event> = events
                .iter()
                .filter(|e| e.at_ms >= from && e.at_ms < to)
                .cloned()
                .collect();
            ticks.observe(&tick, &|_| true);
        }
        let mut merged = ticks.take();
        merged.instances.sort_by(|a, b| a.instance.cmp(&b.instance));
        assert_eq!(merged, want);
    }

    #[test]
    fn a_sample_grades_only_its_engine_ids_and_forgets_past_the_horizon() {
        let events = stream();
        let keep = |b: &str| b == "b3";
        let mut sampled = EvictAudit::default();
        sampled.observe(&events, &keep);
        let only: Vec<Event> = events
            .iter()
            .filter(|e| e.kind == Kind::HolderReset || e.block_id.as_deref() == Some("b3"))
            .cloned()
            .collect();
        assert_eq!(sampled.take(), audit_from_events(&only));
        assert!(sampled.kept() > 0);
        sampled.forget_before(f64::INFINITY, f64::INFINITY);
        assert_eq!(sampled.kept(), 0);
    }
}
