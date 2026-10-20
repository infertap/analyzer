#!/usr/bin/env bash
# Fast development checks. A crate argument scopes compilation; --full adds integration.
set -uo pipefail
cd "$(dirname "$0")/.."

usage() {
  echo "usage: scripts/preflight.sh [crate | --full]" >&2
  exit 2
}
[ "$#" -le 1 ] || usage
scope="${1:-workspace}"
packages=(--workspace)
case "$scope" in
  workspace|--full) ;;
  *)
    [[ "$scope" =~ ^[a-z][a-z0-9_-]*$ ]] || usage
    [ -f "crates/$scope/Cargo.toml" ] || usage
    # The package name from the manifest: `cli` builds `infertap-analyzer`, not `analyzer-cli`.
    packages=(-p "$(sed -n 's/^name = "\(.*\)"/\1/p' "crates/$scope/Cargo.toml" | head -1)")
    ;;
esac
fail=0
# Print timings and preserve each failed step in its own log.
step() {
  local name="$1"
  printf '%-46s' "$name"; shift
  local t0=$SECONDS
  local label; label=$(printf '%s' "$name" | tr -c 'a-zA-Z0-9' '-')
  local out="${PREFLIGHT_LOG_DIR:-/tmp}/preflight-$$-${label}.log"
  if "$@" >"$out" 2>&1; then
    printf 'ok      %3ds\n' "$((SECONDS - t0))"
    rm -f "$out"
  else
    printf 'FAILED  %3ds\n' "$((SECONDS - t0))"; fail=1
    # **The HEAD and the TAIL, because they catch different failures — and printing only the
    # head hid every test failure this gate ever had.** A compile error appears FIRST; a failing
    # test appears LAST, after every passing one. `sed -n '1,25p'` therefore showed
    # `Compiling...` and a page of `... ok` while the assertion that actually failed scrolled
    # past unseen. A 62-minute run reported `FAILED` and nothing about what.
    printf '    ── first 20 lines ──\n'
    sed -n '1,20p' "$out" | sed 's/^/    /'
    printf '    ── last 40 lines ──\n'
    tail -40 "$out" | sed 's/^/    /'
    # Kept, not deleted: an excerpt is a summary, and the whole thing should be one `less` away.
    printf '    ── full output: %s ──\n' "$out"
  fi
}

step "test runner regression checks"               python3 scripts/tests/test_test_runner.py
step "scripts/spdx-check.sh"                        scripts/spdx-check.sh
step "cargo fmt --all --check"                     cargo fmt --all --check
step "clippy ($scope)"                            cargo clippy "${packages[@]}" --all-targets --locked -- -D warnings
if [ "$scope" = --full ]; then
  step "all PR correctness suites" scripts/test.sh full
else
  step "fast tests ($scope)" scripts/test.sh fast "$scope"
fi

echo
printf 'total %ds\n' "$SECONDS"
if [ "$fail" -eq 0 ]; then
  echo "preflight: green ($scope) — run scripts/prepush.sh before pushing"
else
  echo "preflight: RED ($scope)"
fi
exit "$fail"
