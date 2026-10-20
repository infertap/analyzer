// SPDX-License-Identifier: AGPL-3.0-only

//! Compile the console bundle into the binary.
//!
//! **Why generated code rather than `rust-embed` or `include_dir`.** Both add proc-macro
//! dependency surface to a `deny.toml`-gated workspace, and neither is `ServeDir` — we would
//! still hand-write content types, `ETag`s and cache headers on the other side. Generating the
//! table is about sixty lines and it is where the pin belongs anyway: this file is the last
//! moment before the bytes become the product, so it is the right place to refuse the wrong ones.
//!
//! **An absent bundle generates an EMPTY table rather than failing.** A clean analyzer checkout
//! must `cargo build` with no console staged and no Node anywhere — that is what keeps
//! `console_not_configured` reachable. Prometheus needs its `builtinassets` build tag because Go's `embed` errors on a missing
//! directory; generating the table ourselves means we need no tag, no cargo feature, and no
//! second code path.
//!
//! **A RELEASE build must not be able to forget.** `INFERTAP_REQUIRE_CONSOLE=1` turns the absent
//! case into a build failure, and the release workflow sets it. Without that, the one mistake this
//! design cannot otherwise catch — shipping a binary whose console was never staged — looks
//! exactly like a successful build.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

/// Extension to content type. **Closed, and an unknown extension FAILS THE BUILD.**
///
/// The alternative is a fallback to `application/octet-stream`, which a browser renders as a
/// download prompt rather than as a page — a failure that reaches a customer looking like a
/// broken console rather than like a missing table entry. A new asset kind is a one-line diff in
/// review, which is the cheapest place for it to be noticed.
const CONTENT_TYPES: &[(&str, &str)] = &[
    ("html", "text/html; charset=utf-8"),
    ("js", "text/javascript; charset=utf-8"),
    ("css", "text/css; charset=utf-8"),
    ("json", "application/json"),
    ("svg", "image/svg+xml"),
    ("png", "image/png"),
    ("webp", "image/webp"),
    ("ico", "image/vnd.microsoft.icon"),
    ("woff2", "font/woff2"),
    ("txt", "text/plain; charset=utf-8"),
];

fn main() {
    let manifest = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("manifest dir"));
    let staged = manifest.join("console-dist");
    let pin = manifest.join("../../console/PIN.toml");

    println!("cargo:rerun-if-changed=console-dist");
    println!("cargo:rerun-if-changed=../../console/PIN.toml");
    println!("cargo:rerun-if-env-changed=INFERTAP_REQUIRE_CONSOLE");

    let required = std::env::var("INFERTAP_REQUIRE_CONSOLE").as_deref() == Ok("1");
    let out = PathBuf::from(std::env::var("OUT_DIR").expect("out dir"));

    let files = collect(&staged);
    if files.is_empty() {
        assert!(
            !required,
            "INFERTAP_REQUIRE_CONSOLE=1 but no console bundle is staged at {}. Run \
             `scripts/stage-console.sh <console-checkout-or-tarball>` first. A release build must \
             not be able to produce a binary with no console in it.",
            staged.display()
        );
        std::fs::write(out.join("console_assets.rs"), empty_table()).expect("write table");
        return;
    }

    let (version, pinned_hash) = read_pin(&pin);
    let actual = tree_hash(&staged, &files);
    assert!(
        actual == pinned_hash,
        "the staged console does not match console/PIN.toml.\n  pinned ({version}): \
         {pinned_hash}\n  staged:              {actual}\nIf this is a deliberate console bump, \
         move the pin in the same commit that takes the new bundle — the pin moving is the \
         reviewable diff."
    );

    refuse_the_development_rig(&staged, &files);

    std::fs::write(
        out.join("console_assets.rs"),
        table(&staged, &files, &version, &actual),
    )
    .expect("write table");
}

/// **A bundle carrying the console's development rig must never be compiled into this binary.**
///
/// A console built in development mode draws its figures from a synthetic corpus and never calls
/// this crate's API. Its content hash still matches the pin, `version` still reports it and every
/// asset still serves: nothing that is green has an opinion about what the bundle MEANS, so this
/// check does. The console excludes the corpus from a production build and checks its own
/// `dist`; this checks again at the boundary the bytes cross to become a shipped binary, because a
/// pin verifies that the bundle is the one EXPECTED, never that what was expected is fit to ship.
/// Staging and embedding are different moments, which is also why the hash is checked twice.
///
/// Case-insensitive: the rig's words are lower-case in source and upper-cased by CSS.
fn refuse_the_development_rig(root: &Path, files: &[PathBuf]) {
    const MARKERS: [&str; 3] = ["static rig", "no daemon attached", "synthetic data"];
    for rel in files {
        let ext = rel.extension().and_then(|e| e.to_str()).unwrap_or("");
        if !matches!(ext, "js" | "css" | "html") {
            continue;
        }
        let Ok(body) = std::fs::read_to_string(root.join(rel)) else {
            continue;
        };
        let body = body.to_lowercase();
        for marker in MARKERS {
            assert!(
                !body.contains(marker),
                "the staged console carries its DEVELOPMENT RIG ({marker:?} in {}). That bundle \
                 renders synthetic figures and never calls this API, which is what v1.0.0 shipped. \
                 Rebuild the console with `npm run build` (production mode drops `src/dev`) and \
                 re-stage.",
                rel.display()
            );
        }
    }
}

/// Every file under `root`, relative and sorted. Sorted because the generated table's order is
/// part of the build's output and a filesystem's directory order is not stable across machines.
fn collect(root: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    walk(root, root, &mut found);
    found.sort();
    found
}

fn walk(root: &Path, dir: &Path, found: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            walk(root, &path, found);
        } else if let Ok(rel) = path.strip_prefix(root) {
            found.push(rel.to_path_buf());
        }
    }
}

/// The same manifest hash `scripts/console-hash.sh` computes, in Rust. Two implementations of one
/// definition, which is a duplication worth naming: the script is what a human and CI run before
/// the build, this is what the build itself trusts, and neither can be the other. The shape is
/// simple enough to state in one line — `<relative path>\0<sha256 hex>\n` per file, sorted, hashed
/// whole — and a test compares the two against the same tree.
fn tree_hash(root: &Path, files: &[PathBuf]) -> String {
    use sha2::{Digest as _, Sha256};
    let mut manifest = Vec::new();
    for rel in files {
        let bytes = std::fs::read(root.join(rel)).expect("read staged file");
        manifest.extend_from_slice(rel.to_string_lossy().as_bytes());
        manifest.push(0);
        manifest.extend_from_slice(hex(&Sha256::digest(&bytes)).as_bytes());
        manifest.push(b'\n');
    }
    hex(&Sha256::digest(&manifest))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut acc, b| {
        let _ = write!(acc, "{b:02x}");
        acc
    })
}

/// `version` and `content_hash` out of the pin. Deliberately not a TOML parser: two scalar keys
/// do not justify a build-dependency, and a malformed pin fails loudly here rather than parsing
/// into something plausible.
fn read_pin(path: &Path) -> (String, String) {
    let text =
        std::fs::read_to_string(path).unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));
    let value = |key: &str| {
        text.lines()
            .find_map(|line| {
                let line = line.trim();
                let rest = line.strip_prefix(key)?.trim_start().strip_prefix('=')?;
                rest.trim()
                    .strip_prefix('"')?
                    .split('"')
                    .next()
                    .map(str::to_string)
            })
            .unwrap_or_else(|| panic!("{} states no {key}", path.display()))
    };
    (value("version"), value("content_hash"))
}

fn empty_table() -> String {
    "\
/// No console was staged when this binary was built, so `console_not_configured` is reachable.
/// The staged console's version, or `None` when no console was staged at build time.
pub const CONSOLE_VERSION: Option<&str> = None;
/// The staged console's content hash, or `None` when no console was staged at build time.
pub const CONSOLE_HASH: Option<&str> = None;
/// Every file of the staged console, embedded; empty when no console was staged.
pub static CONSOLE_ASSETS: &[Asset] = &[];
"
    .to_string()
}

fn table(root: &Path, files: &[PathBuf], version: &str, hash: &str) -> String {
    use sha2::{Digest as _, Sha256};

    let mut out = format!(
        "/// The staged console's version, as console/PIN.toml pins it.\n\
         pub const CONSOLE_VERSION: Option<&str> = Some({version:?});\n\
         /// The staged console's content hash, verified against the pin at build time.\n\
         pub const CONSOLE_HASH: Option<&str> = Some({hash:?});\n\
         /// Every file of the staged console, embedded with its content type and digest.\n\
         pub static CONSOLE_ASSETS: &[Asset] = &[\n"
    );
    for rel in files {
        let path = rel.to_string_lossy().replace('\\', "/");
        let absolute = root.join(rel);
        let extension = rel
            .extension()
            .map(|e| e.to_string_lossy().to_lowercase())
            .unwrap_or_default();
        let content_type = CONTENT_TYPES
            .iter()
            .find(|(ext, _)| *ext == extension)
            .map_or_else(
                || {
                    panic!(
                        "console asset {path} has extension {extension:?}, which has no content \
                         type in build.rs. Serving it as application/octet-stream would render a \
                         download prompt where a page belongs — add the type deliberately."
                    )
                },
                |(_, ct)| *ct,
            );
        let bytes = std::fs::read(&absolute).expect("read staged file");
        // The ETag is the file's own digest, computed once here rather than per request. Vite
        // content-hashes the names under `/assets/`, so those never change identity without
        // changing name; `index.html` keeps a stable name and is exactly the file that needs one.
        let etag = format!("\"{}\"", &hex(&Sha256::digest(&bytes))[..32]);
        let _ = writeln!(
            out,
            "    Asset {{ path: {path:?}, body: include_bytes!({:?}), content_type: \
             {content_type:?}, etag: {etag:?} }},",
            absolute.to_string_lossy()
        );
    }
    out.push_str("];\n");
    out
}
