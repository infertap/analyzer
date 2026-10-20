// SPDX-License-Identifier: AGPL-3.0-only

//! The sampled audit (`keyed::audit`): over a whole run every tick's sampled blocks agree with
//! what the tick stated and the audit edge follows the classified edge, and a stated fact the
//! re-fold does not state is found and named.

mod keyed_log;

use analyzer_analysis::emitted::ClassifiedFact;
use analyzer_analysis::{Params, Window};
use analyzer_pipeline::keyed::{
    audit, Driver, Seeds, SegmentSource, SpineLayout, Store, Watermark, Written,
};
use keyed_log::{segments, stream, Chunks, Log};

/// A driver over `log` on two lanes of each operator.
fn driver(log: &Log, params: &Params) -> (Driver<Seeds>, Store) {
    let (driver, store) = keyed_log::open_driver(&log.lake, params).expect("open");
    let driver = driver
        .with_layout(SpineLayout {
            partitions: 8,
            retention_days: 7,
        })
        .with_evict_sample(1);
    (driver, store)
}

/// An evict audit's grades, fleet-wide: audited, agreed, mismatched, underresolved,
/// indeterminate and unjoined.
fn grades(o: &analyzer_analysis::differential::AuditOutcome) -> (u64, u64, u64, u64, u64, u64) {
    (
        o.audited(),
        o.instances.iter().map(|i| i.agreed).sum(),
        o.mismatched(),
        o.underresolved(),
        o.indeterminate(),
        o.unjoined(),
    )
}

#[test]
fn every_tick_s_sample_agrees_and_a_changed_fact_is_named() {
    let params = Params::default();
    let all = stream(6.0, 30);
    let parts = segments(&all);
    let log = Log::new("audit");
    let chunks = Chunks::default();
    let source = SegmentSource::new(&log.lake, &chunks);
    let working = log.lake.working().expect("a working state");
    let skew = params.delta_skew_ms;
    let (mut driver, mut store) = driver(&log, &params);
    let end = all[all.len() - 1].at_ms + skew + 1.0;
    let ticks: Vec<Watermark> = parts
        .iter()
        .map(|p| Watermark::At(p.last().map_or(0.0, |e| e.at_ms) - 150_000.0))
        .chain([Watermark::At(end)])
        .collect();
    let (mut sampled, mut compared) = (0, 0);
    let mut last: Option<(Vec<ClassifiedFact>, Window)> = None;
    let mut evicts = (0, 0, 0, 0, 0, 0);
    for (seq, watermark) in ticks.into_iter().enumerate() {
        if let Some(part) = parts.get(seq) {
            log.commit(&chunks, i64::try_from(seq).expect("small"), part.clone());
        }
        let before = driver.repeat().classified_to_ms();
        working.begin(&log.lake).expect("begin");
        let run = driver
            .tick(&log.lake, &source, &params, watermark)
            .expect("tick");
        assert!(
            run.audit.disagreements.is_empty(),
            "tick {seq}: {}",
            run.audit.disagreements[0]
        );
        assert_eq!(
            driver.audit().audited_to_ms,
            driver.repeat().classified_to_ms(),
            "tick {seq}: the audit edge left the classified edge"
        );
        sampled += run.audit.blocks;
        let g = grades(&run.evicts);
        evicts = (
            evicts.0 + g.0,
            evicts.1 + g.1,
            evicts.2 + g.2,
            evicts.3 + g.3,
            evicts.4 + g.4,
            evicts.5 + g.5,
        );
        compared += run.audit.stores;
        if let (Some(start), Some(end)) = (before, driver.repeat().classified_to_ms()) {
            if !run.cells.facts.is_empty() {
                last = Some((
                    run.cells.facts.clone(),
                    Window {
                        start_ms: start,
                        end_ms: end,
                    },
                ));
            }
        }
        driver
            .save(&log.lake, &mut store, seq as u64 + 1)
            .expect("save");
        log.lake.end_unit(working.position()).expect("end");
    }
    assert!(sampled >= 8, "only {sampled} blocks sampled");
    assert!(compared > 20, "only {compared} stores compared");
    assert_eq!(driver.audit().disagreed_at_ms, None);
    // Every engine id sampled, the ticks' evict grades are the window audit's over the run.
    let whole = analyzer_analysis::differential::audit_from_events(&all);
    assert!(whole.audited() > 20, "only {} evicts", whole.audited());
    assert_eq!(evicts, grades(&whole));

    // The last window's facts as stated, but for one store's class: the audit names it.
    let (mut facts, window) = last.expect("a tick classified stores past an edge");
    let changed = facts[0].clone();
    facts[0].class = if changed.class == analyzer_analysis::classify::Class::Cold {
        analyzer_analysis::classify::Class::Etr
    } else {
        analyzer_analysis::classify::Class::Cold
    };
    let found = audit(
        &log.lake,
        (&facts, window),
        std::slice::from_ref(&changed.block),
        (&params, &driver.roster().cells().models()),
        &Written::default(),
    )
    .expect("audit");
    assert_eq!(found.disagreements.len(), 1, "{:?}", found.disagreements);
    let d = &found.disagreements[0];
    assert_eq!(
        (&d.block, &d.holder, d.at_ms.to_bits()),
        (&changed.block, &changed.holder, changed.at_ms.to_bits())
    );
    assert_eq!(
        d.refolded.as_ref().map(|r| r.class.as_str()),
        Some(changed.class.as_str())
    );
}
