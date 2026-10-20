// SPDX-License-Identifier: AGPL-3.0-only

//! The console pin, checked from the other side.
//!
//! `build.rs` verifies the staged tree against `console/PIN.toml` before it embeds anything, so
//! a wrong bundle cannot reach a binary. What that cannot check is **its own hash definition**:
//! the build trusts a Rust implementation, while `scripts/stage-console.sh` and every human
//! bumping the pin trust a shell one. Two implementations of one definition is a duplication worth
//! naming, and this is what keeps them honest — the same tree through both, compared.
//!
//! Without it the failure is quiet and awkward: staging would accept bytes the build then
//! refuses, or worse, agree on a hash for two different trees.

/// Every assertion here is conditional on a bundle being staged. A clean checkout legitimately has
/// none — that is what keeps `console_not_configured` reachable — and `cargo test` must pass
/// there. CI stages before it builds, which is where these actually run.
fn staged() -> bool {
    analyzer_api::console::present()
}

#[test]
fn the_shell_hash_and_the_build_hash_are_the_same_definition() {
    if !staged() {
        return;
    }
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let script = root.join("../../scripts/console-hash.sh");
    let tree = root.join("console-dist");

    let output = std::process::Command::new("bash")
        .arg(&script)
        .arg(&tree)
        .output()
        .expect("running console-hash.sh");
    assert!(
        output.status.success(),
        "console-hash.sh failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let from_shell = String::from_utf8(output.stdout)
        .expect("utf8")
        .trim()
        .to_string();

    let (_, from_build) = analyzer_api::console::stamp().expect("a staged build carries a stamp");
    assert_eq!(
        from_shell, from_build,
        "the hash `stage-console.sh` verifies and the hash `build.rs` verifies must be the same \
         number. They are computed by two implementations of one definition, and a disagreement \
         means staging accepts bytes the build refuses — or, worse, that they agree on a hash for \
         two different trees"
    );
}

#[test]
fn the_embedded_bundle_is_the_one_the_pin_names() {
    if !staged() {
        return;
    }
    let pin = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../console/PIN.toml"),
    )
    .expect("console/PIN.toml");
    let (version, hash) = analyzer_api::console::stamp().expect("stamp");

    // `build.rs` already refuses a mismatch, so this cannot fail while that holds. It is here for
    // the same reason the content-type test is: it states the property the build enforces, so
    // deleting the enforcement does not leave the property unasserted.
    assert!(
        pin.contains(&format!("content_hash = \"{hash}\"")),
        "the binary carries {hash}, which console/PIN.toml does not name"
    );
    assert!(
        pin.contains(&format!("version = \"{version}\"")),
        "the binary carries {version}, which console/PIN.toml does not name"
    );
}

#[test]
fn the_bundle_carries_no_source_maps() {
    if !staged() {
        return;
    }
    // Staging strips them, and this is what notices if that ever stops happening: 1.3 MB of a
    // 1.9 MB dist, and a readable copy of the console's source, in every customer's binary.
    let leaked: Vec<_> = analyzer_api::console::assets()
        .iter()
        .filter(|a| {
            std::path::Path::new(a.path)
                .extension()
                .is_some_and(|e| e.eq_ignore_ascii_case("map"))
        })
        .map(|a| a.path)
        .collect();
    assert!(
        leaked.is_empty(),
        "source maps reached the binary: {leaked:?}"
    );
}

#[test]
fn the_document_is_embedded_because_every_deep_link_falls_back_to_it() {
    if !staged() {
        return;
    }
    // A bundle without `index.html` embeds fine and then answers every route with a 404 that
    // reads as a broken console. The SPA fallback has one target and this is it.
    assert!(
        analyzer_api::console::assets()
            .iter()
            .any(|a| a.path == "index.html"),
        "the staged bundle carries no index.html"
    );
}
