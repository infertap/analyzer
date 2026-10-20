// SPDX-License-Identifier: AGPL-3.0-only

//! Conditional writes: the contract every store that implements them must keep, checked on
//! the directory store here and on a live bucket when one is configured.
//!
//! ```text
//! INFERTAP_TEST_S3_URL=s3://bucket/prefix cargo test -p analyzer-objectstore --test conditional -- --ignored
//! ```

use analyzer_objectstore::{
    check_conditional_writes, Conditional, DirStore, ObjectStore, Precondition, S3Store,
    StaticCredentials,
};

fn scratch(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "infertap-conditional-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("scratch dir");
    dir
}

/// The contract, against any store: the one `check_conditional_writes` states.
fn contract(store: &dyn ObjectStore, key: &str) {
    if let Err(e) = check_conditional_writes(store, key) {
        panic!("{e}");
    }
}

/// Writers racing read-then-write on one counter: every written increment is kept.
fn no_lost_update(stores: Vec<Box<dyn ObjectStore + Send>>, key: &str, rounds: usize) {
    let writers = stores.len();
    let written: usize = std::thread::scope(|s| {
        let handles: Vec<_> = stores
            .into_iter()
            .map(|store| {
                s.spawn(move || {
                    let mut mine = 0;
                    for _ in 0..rounds {
                        loop {
                            let (n, condition) = match store.get_versioned(key).expect("read") {
                                None => (0_u64, Precondition::Absent),
                                Some((body, v)) => (
                                    String::from_utf8(body)
                                        .expect("utf8")
                                        .parse()
                                        .expect("counter"),
                                    Precondition::Matches(v),
                                ),
                            };
                            let next = (n + 1).to_string();
                            match store
                                .put_if(key, next.as_bytes(), &condition)
                                .expect("write")
                            {
                                Conditional::Written(_) => {
                                    mine += 1;
                                    break;
                                }
                                Conditional::Refused => {}
                                other => panic!("unexpected outcome {other:?}"),
                            }
                        }
                    }
                    mine
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().expect("writer")).sum()
    });
    assert_eq!(
        written,
        writers * rounds,
        "every writer finishes its rounds"
    );
}

/// A store that writes whatever the precondition says: the S3-compatible failure the check
/// exists for.
struct IgnoresPreconditions(DirStore);

impl ObjectStore for IgnoresPreconditions {
    fn location(&self) -> String {
        self.0.location()
    }
    fn list(
        &self,
        prefix: &str,
    ) -> Result<Vec<analyzer_objectstore::ObjectMeta>, analyzer_objectstore::Error> {
        self.0.list(prefix)
    }
    fn sibling(&self) -> Result<Box<dyn ObjectStore>, analyzer_objectstore::Error> {
        self.0.sibling()
    }
    fn list_after(
        &self,
        prefix: &str,
        after: &str,
    ) -> Result<Vec<analyzer_objectstore::ObjectMeta>, analyzer_objectstore::Error> {
        self.0.list_after(prefix, after)
    }
    fn get(&self, key: &str) -> Result<Vec<u8>, analyzer_objectstore::Error> {
        self.0.get(key)
    }
    fn get_versioned(
        &self,
        key: &str,
    ) -> Result<Option<(Vec<u8>, analyzer_objectstore::Version)>, analyzer_objectstore::Error> {
        self.0.get_versioned(key)
    }
    fn put(&self, key: &str, body: &[u8]) -> Result<(), analyzer_objectstore::Error> {
        self.0.put(key, body)
    }
    fn put_if(
        &self,
        key: &str,
        body: &[u8],
        _condition: &Precondition,
    ) -> Result<Conditional, analyzer_objectstore::Error> {
        self.0.put(key, body)?;
        let (_, version) = self.0.get_versioned(key)?.expect("just written");
        Ok(Conditional::Written(version))
    }
    fn delete(&self, key: &str) -> Result<(), analyzer_objectstore::Error> {
        self.0.delete(key)
    }
}

#[test]
fn a_store_that_ignores_preconditions_fails_the_check() {
    let root = scratch("ignores");
    let err = check_conditional_writes(&IgnoresPreconditions(DirStore::new(&root)), "head")
        .expect_err("a store that writes every conditional write cannot fence writers");
    assert!(err.contains("If-None-Match"), "{err}");
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn the_directory_store_keeps_the_contract() {
    let root = scratch("contract");
    contract(&DirStore::new(&root), "journal/head");
    std::fs::remove_dir_all(root).ok();
}

#[test]
fn the_directory_store_loses_no_update_under_racing_writers() {
    let root = scratch("race");
    let stores: Vec<Box<dyn ObjectStore + Send>> = (0..4)
        .map(|_| Box::new(DirStore::new(&root)) as Box<dyn ObjectStore + Send>)
        .collect();
    no_lost_update(stores, "journal/counter", 25);
    let (body, _) = DirStore::new(&root)
        .get_versioned("journal/counter")
        .expect("read")
        .expect("exists");
    assert_eq!(body, b"100", "four writers, twenty-five increments each");
    std::fs::remove_dir_all(root).ok();
}

#[test]
#[ignore = "live: INFERTAP_TEST_S3_URL=s3://bucket/prefix"]
fn a_live_bucket_keeps_the_contract_and_loses_no_update() {
    let Ok(url) = std::env::var("INFERTAP_TEST_S3_URL") else {
        eprintln!("skipped: INFERTAP_TEST_S3_URL not set — this test needs a live bucket");
        return;
    };
    let run = format!(
        "conditional-selftest/{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_millis()
    );
    // The static credential the battery supplies, when it does; the provider chain otherwise.
    let creds = std::env::var("INFERTAP_TEST_S3_KEY")
        .ok()
        .map(|access_key_id| StaticCredentials {
            access_key_id,
            secret_access_key: std::env::var("INFERTAP_TEST_S3_SECRET").unwrap_or_default(),
            session_token: None,
        });
    let open = || S3Store::open(&url, creds.as_ref()).expect("a store");
    let store = open();
    contract(&store, &format!("{run}/head"));
    let stores: Vec<Box<dyn ObjectStore + Send>> = (0..4)
        .map(|_| Box::new(open()) as Box<dyn ObjectStore + Send>)
        .collect();
    no_lost_update(stores, &format!("{run}/counter"), 5);
    let (body, _) = store
        .get_versioned(&format!("{run}/counter"))
        .expect("read")
        .expect("exists");
    assert_eq!(body, b"20", "four writers, five increments each");
}
