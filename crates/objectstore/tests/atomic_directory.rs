// SPDX-License-Identifier: AGPL-3.0-only

//! A local mutable head has whole-object visibility during replacement.
use analyzer_objectstore::{DirStore, ObjectStore};

#[test]
fn readers_observe_only_complete_objects_during_replacement() {
    let root = std::env::temp_dir().join(format!("atomic-directory-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let store = DirStore::new(&root);
    let first = vec![17; 4096];
    let second = vec![91; 65536];
    store.put("head", &first).unwrap();
    std::thread::scope(|scope| {
        let write = scope.spawn(|| {
            for i in 0..100 {
                store
                    .put("head", if i % 2 == 0 { &second } else { &first })
                    .unwrap();
            }
        });
        for _ in 0..2000 {
            let bytes = store.get("head").unwrap();
            assert!(
                bytes == first || bytes == second,
                "reader observed a partial replacement"
            );
        }
        write.join().unwrap();
    });
    assert_eq!(
        store.list("").unwrap().len(),
        1,
        "successful writes leave no staging objects"
    );
    std::fs::remove_dir_all(root).unwrap();
}
