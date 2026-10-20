// SPDX-License-Identifier: AGPL-3.0-only

//! The roster on its own clock (`keyed::RosterFeed`): fed commit by commit, it holds the runs
//! one tick over the whole log holds, a run's declarations and restarts included.

mod keyed_log;

use analyzer_analysis::events::{Event, Kind};
use analyzer_analysis::DAY_MS;
use analyzer_pipeline::keyed::RosterFeed;
use keyed_log::{Chunks, Log};

fn store(instance: &str, at_ms: f64) -> Event {
    let mut e = Event::new(Kind::Store, instance, at_ms);
    e.block_id = Some("b".into());
    e
}

#[test]
fn a_roster_fed_commit_by_commit_holds_what_one_fold_does() {
    let log = Log::new("roster");
    let chunks = Chunks::default();
    let mut feed = RosterFeed::default();
    let start = DAY_MS * 100.0;
    // Two engines; the second restarts halfway, a new run on the same instance, and declares
    // its model only in its second segment.
    let commits = [
        (("run-a", "i1", ""), 0, 0.0),
        (("run-b", "i2", ""), 0, 10.0),
        (("run-a", "i1", ""), 1, 300.0),
        (("run-b", "i2", "llama"), 1, 310.0),
        (("run-c", "i2", "llama"), 0, 600.0),
        (("run-a", "i1", ""), 2, 900.0),
    ];
    for (run, seq, at_s) in commits {
        let at = start + at_s * 1000.0;
        log.commit_run(
            &chunks,
            run,
            seq,
            vec![store(run.1, at), store(run.1, at + 60_000.0)],
        );
        feed.tick(&log.lake).expect("tick");
    }

    let mut whole = RosterFeed::default();
    whole.tick(&log.lake).expect("one tick over the log");
    assert_eq!(whole.cells().runs.len(), 3, "three runs spoke");
    assert_eq!(feed.cells().runs, whole.cells().runs, "the runs");
    assert_eq!(feed.cells().rows(), whole.cells().rows(), "the rows");
    assert_eq!(
        feed.position(),
        whole.position(),
        "both read through the last commit"
    );
    assert_eq!(
        feed.cells().runs["run-b"].declared_model.as_deref(),
        Some("llama"),
        "a later segment's declaration reaches its run"
    );
}
