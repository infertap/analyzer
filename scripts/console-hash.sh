#!/usr/bin/env bash
# The content hash of a console dist tree, and the ONE definition of it.
#
# Three things compute this and they must not disagree: `stage-console.sh` when it verifies what
# it staged, `crates/api/build.rs` when it verifies what it is about to embed, and a human
# bumping `console/PIN.toml`. A hash spelled two ways is a pin that passes on one side and fails
# on the other.
#
# The shape: a manifest of `<relative path>\0<sha256 of contents>` over every file, sorted by
# path under a fixed collation, hashed as a whole. Per-file digests rather than concatenated
# bytes so the manifest is readable when something disagrees — `--manifest` prints it — and
# sorted so a filesystem's directory order cannot change the answer.
#
# It covers exactly the files that get EMBEDDED. Staging strips source maps before this runs, so
# a map appearing or vanishing cannot move the hash of a bundle whose served bytes are identical.
set -euo pipefail

dir=${1:?usage: console-hash.sh <dist-dir> [--manifest]}
mode=${2:-}

manifest() {
  cd "$dir"
  # `LC_ALL=C` fixes the collation: a sort that depends on the builder's locale is a hash that
  # depends on the builder's locale.
  find . -type f | LC_ALL=C sort | while IFS= read -r f; do
    printf '%s\0%s\n' "${f#./}" "$(shasum -a 256 "$f" | cut -d' ' -f1)"
  done
}

if [ "$mode" = "--manifest" ]; then
  manifest
else
  manifest | shasum -a 256 | cut -d' ' -f1
fi
