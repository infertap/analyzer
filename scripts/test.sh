#!/usr/bin/env bash
# Shared local/CI correctness tiers. Cargo remains the test runner and target inventory.
set -euo pipefail
cd "$(dirname "$0")/.."

usage() {
  cat >&2 <<'EOF'
usage: scripts/test.sh fast [crate | workspace]
       scripts/test.sh engine <pipeline | api | daemon | module-example | serving-clickhouse | conformance> [integration-target]
       scripts/test.sh full
       scripts/test.sh release

fast:    engine-free tests; defaults to the workspace
engine:  ignored tests against the pinned engine (daemon defaults to loop; conformance is the
         spec's reader corpus)
full:    workspace fast tests plus every engine scope above
release: all ignored workspace tests; requires the release workflow's live bucket

Examples: scripts/test.sh fast analysis
          scripts/test.sh engine pipeline keyed_equivalence
          scripts/test.sh engine api goldens
For individual test filters, use Cargo directly; see docs/testing.md.
EOF
  exit 2
}

tier="${1:-}"
scope="${2:-workspace}"
[ "$#" -le 3 ] || usage
case "$tier" in
  fast)
    [ "$#" -le 2 ] || usage
    if [ "$scope" = workspace ]; then
      cargo test --workspace --locked
    else
      [[ "$scope" =~ ^[a-z][a-z0-9_-]*$ ]] || usage
      [ -f "crates/$scope/Cargo.toml" ] || usage
      # The package name from the manifest: `cli` is `infertap-analyzer`, not `analyzer-cli`.
      cargo test -p "$(sed -n 's/^name = "\(.*\)"/\1/p' "crates/$scope/Cargo.toml" | head -1)" --locked
    fi
    ;;
  engine)
    case "$scope" in pipeline|api|daemon|module-example|serving-clickhouse|conformance) ;; *) usage ;; esac
    if [ "$scope" = conformance ]; then
      [ "$#" -eq 2 ] || usage
      cargo test -p infertap-analyzer --locked --test reader_corpus -- --ignored
      exit 0
    fi
    # Bash 3 (macOS) treats empty arrays as unset under nounset; expand them conditionally.
    target=()
    if [ "$#" -eq 3 ]; then
      [[ "$3" =~ ^[a-z][a-z0-9_]*$ ]] || usage
      # Cargo validates the target against its manifest, including automatically found tests.
      target=(--test "$3")
    elif [ "$scope" = daemon ]; then
      target=(--test loop)
    fi
    threads=()
    [ "$scope" = pipeline ] || threads=(--test-threads=1)
    cargo test -p "analyzer-$scope" --locked ${target[@]+"${target[@]}"} -- --ignored ${threads[@]+"${threads[@]}"}
    ;;
  full)
    [ "$#" -eq 1 ] || usage
    "$PWD/scripts/test.sh" fast
    "$PWD/scripts/test.sh" engine pipeline
    "$PWD/scripts/test.sh" engine module-example
    "$PWD/scripts/test.sh" engine api
    "$PWD/scripts/test.sh" engine daemon
    "$PWD/scripts/test.sh" engine serving-clickhouse
    "$PWD/scripts/test.sh" engine conformance
    ;;
  release)
    [ "$#" -eq 1 ] || usage
    # Every failure in one run: a battery that stopped at its first red binary would hide the rest.
    cargo test --workspace --locked --no-fail-fast -- --ignored --test-threads=1
    ;;
  *) usage ;;
esac
