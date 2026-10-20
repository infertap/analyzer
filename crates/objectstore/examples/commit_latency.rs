// SPDX-License-Identifier: AGPL-3.0-only

//! Commit latency of conditional writes through the object-store port.
//!
//! Measures what a journal kept in the object store would pay per commit, through the same
//! port, retry and timeout settings the daemon uses. One JSON line per mode on stdout.
//!
//! ```text
//! cargo run --release -p analyzer-objectstore --example commit_latency -- \
//!     --url s3://bucket/prefix --n 300 --data-objects 4 --data-size 65536 --writers 4 --seconds 30
//! ```
//!
//! A URL without a scheme is a directory ([`DirStore`]); anything else is an S3-API URL
//! resolved through the provider chain (`AWS_ENDPOINT`, `AWS_ALLOW_HTTP` and the static
//! `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` pair are read from the environment, as the
//! daemon reads them).
//!
//! Modes:
//! - `put`: unconditional write of a small object (the baseline).
//! - `create`: conditional create of a new key.
//! - `cas`: one writer advancing one head key by its last version. `refused` here counts
//!   ambiguous commits: a retried request that found its own write.
//! - `read`: versioned read of the head (the restart path).
//! - `commit`: `data-objects` unconditional writes in parallel, then one `cas` on the head.
//! - `contend`: `writers` writers racing read-then-write on one head for `seconds`.

use analyzer_objectstore::{Conditional, DirStore, ObjectStore, Precondition, S3Store};
use std::time::{Duration, Instant};

struct Args {
    url: String,
    n: usize,
    size: usize,
    data_objects: usize,
    data_size: usize,
    writers: usize,
    seconds: u64,
    modes: Vec<String>,
    label: String,
}

fn args() -> Args {
    let mut a = Args {
        url: String::new(),
        n: 200,
        size: 1024,
        data_objects: 4,
        data_size: 64 * 1024,
        writers: 4,
        seconds: 30,
        modes: ["put", "create", "cas", "read", "commit", "contend"]
            .map(String::from)
            .to_vec(),
        label: String::new(),
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut val = || it.next().unwrap_or_else(|| panic!("{flag} needs a value"));
        match flag.as_str() {
            "--url" => a.url = val(),
            "--n" => a.n = val().parse().expect("--n"),
            "--size" => a.size = val().parse().expect("--size"),
            "--data-objects" => a.data_objects = val().parse().expect("--data-objects"),
            "--data-size" => a.data_size = val().parse().expect("--data-size"),
            "--writers" => a.writers = val().parse().expect("--writers"),
            "--seconds" => a.seconds = val().parse().expect("--seconds"),
            "--modes" => a.modes = val().split(',').map(String::from).collect(),
            "--label" => a.label = val(),
            other => panic!("unknown flag {other}"),
        }
    }
    assert!(!a.url.is_empty(), "--url is required");
    a
}

fn open(url: &str) -> Box<dyn ObjectStore> {
    if url.contains("://") {
        Box::new(S3Store::open(url, None).expect("open store"))
    } else {
        Box::new(DirStore::new(url))
    }
}

#[derive(Default)]
struct Sample {
    ms: Vec<f64>,
    refused: usize,
    errors: usize,
    first_error: Option<String>,
}

impl Sample {
    fn time<T>(&mut self, f: impl FnOnce() -> T) -> T {
        let t = Instant::now();
        let out = f();
        self.ms.push(t.elapsed().as_secs_f64() * 1000.0);
        out
    }
    fn error(&mut self, e: &dyn std::fmt::Display) {
        self.errors += 1;
        self.first_error.get_or_insert_with(|| e.to_string());
    }
    fn json(mut self, mode: &str, a: &Args, extra: &str) -> String {
        self.ms.sort_by(f64::total_cmp);
        // Nearest-rank percentile, in whole percent.
        let q = |percent: usize| {
            self.ms
                .get((self.ms.len().saturating_sub(1) * percent + 50) / 100)
                .copied()
                .unwrap_or(f64::NAN)
        };
        let mean = self.ms.iter().sum::<f64>() / as_f64(self.ms.len().max(1));
        format!(
            "{{\"label\":{:?},\"url\":{:?},\"mode\":{mode:?},\"n\":{},\"p50_ms\":{:.2},\"p90_ms\":{:.2},\"p99_ms\":{:.2},\"max_ms\":{:.2},\"mean_ms\":{mean:.2},\"refused\":{},\"errors\":{},\"first_error\":{:?}{extra}}}",
            a.label,
            a.url,
            self.ms.len(),
            q(50),
            q(90),
            q(99),
            q(100),
            self.refused,
            self.errors,
            self.first_error.unwrap_or_default(),
        )
    }
}

fn body(len: usize, seed: usize) -> Vec<u8> {
    (0..len)
        .map(|i| u8::try_from((i * 31 + seed * 7) % 251).unwrap_or(0))
        .collect()
}

/// A count as a float, for rates and means over counts far below 2^52.
fn as_f64(n: usize) -> f64 {
    f64::from(u32::try_from(n).unwrap_or(u32::MAX))
}

fn run_put(store: &dyn ObjectStore, run: &str, a: &Args) -> Sample {
    let mut s = Sample::default();
    for i in 0..a.n {
        let b = body(a.size, i);
        if let Err(e) = s.time(|| store.put(&format!("{run}/put/{i}"), &b)) {
            s.error(&e);
        }
    }
    s
}

fn run_create(store: &dyn ObjectStore, run: &str, a: &Args) -> Sample {
    let mut s = Sample::default();
    for i in 0..a.n {
        let b = body(a.size, i);
        match s.time(|| store.put_if(&format!("{run}/create/{i}"), &b, &Precondition::Absent)) {
            Ok(Conditional::Written(_)) => {}
            Ok(Conditional::Refused) => s.refused += 1,
            Ok(other) => s.error(&format!("unexpected outcome {other:?}")),
            Err(e) => s.error(&e),
        }
    }
    s
}

/// Advance `key` `n` times from its current state, one writer.
fn run_cas(store: &dyn ObjectStore, key: &str, a: &Args) -> Sample {
    let mut s = Sample::default();
    let mut condition = match store.get_versioned(key) {
        Ok(Some((_, v))) => Precondition::Matches(v),
        Ok(None) => Precondition::Absent,
        Err(e) => {
            s.error(&e);
            return s;
        }
    };
    for i in 0..a.n {
        let b = body(a.size, i);
        match s.time(|| store.put_if(key, &b, &condition)) {
            Ok(Conditional::Written(v)) => condition = Precondition::Matches(v),
            Ok(Conditional::Refused) => {
                s.refused += 1;
                match store.get_versioned(key) {
                    Ok(Some((_, v))) => condition = Precondition::Matches(v),
                    Ok(None) => condition = Precondition::Absent,
                    Err(e) => s.error(&e),
                }
            }
            Ok(other) => s.error(&format!("unexpected outcome {other:?}")),
            Err(e) => s.error(&e),
        }
    }
    s
}

fn run_read(store: &dyn ObjectStore, key: &str, a: &Args) -> Sample {
    let mut s = Sample::default();
    for _ in 0..a.n {
        if let Err(e) = s.time(|| store.get_versioned(key)) {
            s.error(&e);
        }
    }
    s
}

/// Data objects written in parallel, one sibling store per object, then one head advance.
fn run_commit(store: &dyn ObjectStore, run: &str, a: &Args) -> Sample {
    let mut s = Sample::default();
    let siblings: Vec<Box<dyn ObjectStore>> = (0..a.data_objects)
        .map(|_| store.sibling().expect("sibling"))
        .collect();
    let head = format!("{run}/commit/head");
    let mut condition = Precondition::Absent;
    for i in 0..a.n {
        let t = Instant::now();
        let failed: Vec<String> = std::thread::scope(|sc| {
            let handles: Vec<_> = siblings
                .iter()
                .enumerate()
                .map(|(k, sib)| {
                    let b = body(a.data_size, i * 131 + k);
                    let key = format!("{run}/commit/data/{i}/{k}");
                    sc.spawn(move || sib.put(&key, &b).err().map(|e| e.to_string()))
                })
                .collect();
            handles
                .into_iter()
                .filter_map(|h| h.join().expect("writer"))
                .collect()
        });
        if let Some(e) = failed.first() {
            s.error(e);
            continue;
        }
        let manifest = body(a.size, i);
        match store.put_if(&head, &manifest, &condition) {
            Ok(Conditional::Written(v)) => condition = Precondition::Matches(v),
            Ok(Conditional::Refused) => s.refused += 1,
            Ok(other) => {
                s.error(&format!("unexpected outcome {other:?}"));
                continue;
            }
            Err(e) => {
                s.error(&e);
                continue;
            }
        }
        s.ms.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    s
}

/// `writers` writers racing read-then-write on one head. Latency is per successful
/// commit, including the reads and refused attempts that preceded it.
fn run_contend(store: &dyn ObjectStore, run: &str, a: &Args) -> (Sample, String) {
    let head = format!("{run}/contend/head");
    let until = Instant::now() + Duration::from_secs(a.seconds);
    let siblings: Vec<Box<dyn ObjectStore>> = (0..a.writers)
        .map(|_| store.sibling().expect("sibling"))
        .collect();
    let results: Vec<Sample> = std::thread::scope(|scope| {
        let handles: Vec<_> = siblings
            .iter()
            .enumerate()
            .map(|(writer, sib)| {
                let head = head.clone();
                scope.spawn(move || {
                    let mut sample = Sample::default();
                    let mut round = 0;
                    while Instant::now() < until {
                        let started = Instant::now();
                        loop {
                            let condition = match sib.get_versioned(&head) {
                                Ok(Some((_, version))) => Precondition::Matches(version),
                                Ok(None) => Precondition::Absent,
                                Err(e) => {
                                    sample.error(&e);
                                    break;
                                }
                            };
                            let next = body(a.size, writer * 100_000 + round);
                            match sib.put_if(&head, &next, &condition) {
                                Ok(Conditional::Written(_)) => {
                                    sample.ms.push(started.elapsed().as_secs_f64() * 1000.0);
                                    break;
                                }
                                Ok(Conditional::Refused) => sample.refused += 1,
                                Ok(other) => {
                                    sample.error(&format!("unexpected outcome {other:?}"));
                                    break;
                                }
                                Err(e) => {
                                    sample.error(&e);
                                    break;
                                }
                            }
                            if Instant::now() >= until {
                                break;
                            }
                        }
                        round += 1;
                    }
                    sample
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().expect("writer"))
            .collect()
    });
    let per_writer: Vec<usize> = results.iter().map(|r| r.ms.len()).collect();
    let mut all = Sample::default();
    for r in results {
        all.ms.extend(r.ms);
        all.refused += r.refused;
        all.errors += r.errors;
        if all.first_error.is_none() {
            all.first_error = r.first_error;
        }
    }
    let rate = as_f64(all.ms.len()) / as_f64(usize::try_from(a.seconds).unwrap_or(usize::MAX));
    (
        all,
        format!(
            ",\"writers\":{},\"seconds\":{},\"commits_per_s\":{rate:.2},\"per_writer_commits\":{per_writer:?}",
            a.writers, a.seconds
        ),
    )
}

fn main() {
    let a = args();
    let store = open(&a.url);
    let run = format!(
        "commit-latency/{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_millis()
    );
    let head = format!("{run}/cas/head");
    let sizes = format!(",\"size\":{}", a.size);
    for mode in &a.modes {
        let line = match mode.as_str() {
            "put" => run_put(store.as_ref(), &run, &a).json(mode, &a, &sizes),
            "create" => run_create(store.as_ref(), &run, &a).json(mode, &a, &sizes),
            "cas" => run_cas(store.as_ref(), &head, &a).json(mode, &a, &sizes),
            "read" => run_read(store.as_ref(), &head, &a).json(mode, &a, &sizes),
            "commit" => run_commit(store.as_ref(), &run, &a).json(
                mode,
                &a,
                &format!(
                    "{sizes},\"data_objects\":{},\"data_size\":{}",
                    a.data_objects, a.data_size
                ),
            ),
            "contend" => {
                let (s, extra) = run_contend(store.as_ref(), &run, &a);
                s.json(mode, &a, &format!("{sizes}{extra}"))
            }
            other => panic!("unknown mode {other}"),
        };
        println!("{line}");
    }
}
