#!/usr/bin/env python3
"""Generate THIRD-PARTY-NOTICES for the shipped analyzer binary.

**Hand-rolled, on the console's own precedent.** `console/scripts/check-licences.mjs` says it
plainly: "the alternative is a dependency whose job is to read the dependencies, which is a
strange thing to add to a repo with an allowlist culture". `cargo-about` would do this, and it
would be a fourth pinned CI tool plus a template language, to walk a graph `cargo metadata`
already hands us as JSON.

**What it covers, and why the boundary is where it is.** The closure reachable from the
`infertap-analyzer` binary through NORMAL and BUILD edges. Dev-dependencies are excluded because
they are not redistributed — a test harness is not in the artifact. Build-dependencies are
INCLUDED even though most are proc-macros that contribute no linked code, because over-attributing
costs a paragraph and under-attributing is the failure this file exists to prevent.

**The dev exclusion currently removes NOTHING, and that is worth saying rather than implying.**
Measured 2026-08-15: the closure is 175 crates with or without dev edges, because every
dev-dependency this workspace declares — `tokio`, `tower`, `serde_json`, `sha2` — is also a normal
dependency somewhere in the tree. The filter is correct and inert. It starts mattering the first
time a test reaches for something the product does not, which is exactly when nobody would
remember to add it.

Two things this does not cover, both deliberate and both named in the output so a reader is not
left to assume completeness:

  - **ClickHouse**, redistributed unmodified in every artifact. It is not a cargo dependency, so
    no graph walk finds it. Its notice is written here explicitly.
  - **The console's npm tree**, compiled into the binary since the embed landed. Those packages
    live in the console repo's `node_modules`, which this repo does not have and should not need.
    The console emits its own notices; the release workflow concatenates them.

Usage:  scripts/third-party-notices.py [--check] > THIRD-PARTY-NOTICES
        --check compares against the committed file and exits 1 on drift.
"""

import json
import subprocess
import sys
from pathlib import Path

# Files a crate conventionally puts its licence text in. Ordered so the canonical name wins when
# a crate ships several; `NOTICE` last because Apache-2.0 makes it supplementary to the licence
# rather than a substitute for it.
LICENCE_FILES = ("LICENSE", "LICENCE", "COPYING", "UNLICENSE", "NOTICE")

# Redistributed, and NOT a cargo dependency — so nothing in the graph walk below would ever find
# it. The analyzer ships the pinned binary unmodified inside the VM bundle and the image, which is
# redistribution and carries the Apache-2.0 notice obligation in full.
CLICKHOUSE_NOTICE = """\
ClickHouse
    Version:    {version}
    Licence:    Apache-2.0
    Homepage:   https://clickhouse.com
    Source:     https://github.com/ClickHouse/ClickHouse

    Redistributed unmodified as the analyzer's storage engine. The analyzer generates its
    configuration and supervises its lifecycle; the binary itself is upstream's, byte for byte,
    verified on fetch against the SHA-512 in CLICKHOUSE_TGZ_SHA512.

    Licensed under the Apache License, Version 2.0. You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
"""


def metadata() -> dict:
    out = subprocess.run(
        ["cargo", "metadata", "--format-version", "1"],
        capture_output=True,
        check=True,
        text=True,
    )
    return json.loads(out.stdout)


def shipped_closure(meta: dict) -> list[dict]:
    """Every third-party package the analyzer binary's build reaches."""
    by_id = {p["id"]: p for p in meta["packages"]}
    workspace = set(meta["workspace_members"])
    edges = {n["id"]: n for n in meta["resolve"]["nodes"]}

    root = next(
        (i for i in workspace if by_id[i]["name"] == "infertap-analyzer"),
        None,
    )
    if root is None:
        sys.exit("no infertap-analyzer package in this workspace")

    seen: set[str] = set()
    queue = [root]
    while queue:
        current = queue.pop()
        if current in seen:
            continue
        seen.add(current)
        for dep in edges[current]["deps"]:
            # `kind: null` is a normal dependency; "build" ships generated code; "dev" does not
            # ship at all. An empty `dep_kinds` predates the field — treat it as normal.
            kinds = {k.get("kind") for k in dep.get("dep_kinds", [])} or {None}
            if kinds & {None, "build"}:
                queue.append(dep["pkg"])

    third_party = [by_id[i] for i in seen if i not in workspace]
    return sorted(third_party, key=lambda p: (p["name"].lower(), p["version"]))


def licence_text(package: dict) -> str:
    """The crate's own licence text, from the source cargo already has on disk."""
    root = Path(package["manifest_path"]).parent
    found = []
    for entry in sorted(root.iterdir()):
        if not entry.is_file():
            continue
        if entry.name.upper().startswith(LICENCE_FILES):
            try:
                found.append((entry.name, entry.read_text(encoding="utf-8", errors="replace")))
            except OSError:
                continue
    return "\n".join(f"    --- {name} ---\n{text}" for name, text in found)


def render(packages: list[dict], clickhouse_version: str) -> str:
    lines = [
        "THIRD-PARTY NOTICES",
        "",
        "The InferTap analyzer is licensed under the GNU AGPL-3.0-only (see LICENSE). It is built",
        "with, and redistributes, the third-party components below, which remain under their own",
        "licences.",
        "",
        "This file covers:",
        "  - every Rust crate reachable from the analyzer binary through normal and build",
        "    dependencies (dev-dependencies are not redistributed and are excluded)",
        "  - ClickHouse, redistributed unmodified as the storage engine",
        "",
        "It does NOT cover the JavaScript packages compiled into the embedded console. Those are",
        "enumerated by the console's own notices, which the release workflow appends below this",
        "line before an artifact ships. A bundle carrying this file WITHOUT that section is",
        "incomplete and must not be delivered.",
        "",
        "=" * 78,
        "",
        CLICKHOUSE_NOTICE.format(version=clickhouse_version),
        "=" * 78,
        "",
        f"Rust crates ({len(packages)}):",
        "",
    ]
    for package in packages:
        lines.append(f"{package['name']} {package['version']}")
        lines.append(f"    Licence:    {package.get('license') or 'see source'}")
        if package.get("repository"):
            lines.append(f"    Repository: {package['repository']}")
        text = licence_text(package)
        if text:
            lines.append(text)
        else:
            # Named rather than silently omitted: a crate whose licence text is not in its
            # published source is a thing somebody should look at once, not a gap that reads as
            # completeness.
            lines.append(
                "    (no licence file in the published crate; the SPDX identifier above is the"
            )
            lines.append("     declaration from its Cargo.toml)")
        lines.append("")
    return "\n".join(lines) + "\n"


def main() -> None:
    root = Path(__file__).resolve().parent.parent
    clickhouse_version = (root / "CLICKHOUSE_VERSION").read_text().strip()
    rendered = render(shipped_closure(metadata()), clickhouse_version)

    if "--check" in sys.argv:
        committed = root / "THIRD-PARTY-NOTICES"
        if not committed.exists():
            sys.exit("THIRD-PARTY-NOTICES is not committed; run this script and commit its output")
        if committed.read_text() != rendered:
            sys.exit(
                "THIRD-PARTY-NOTICES is stale. The dependency tree moved and the notices did "
                "not.\nRegenerate: scripts/third-party-notices.py > THIRD-PARTY-NOTICES"
            )
        return

    sys.stdout.write(rendered)


if __name__ == "__main__":
    main()
