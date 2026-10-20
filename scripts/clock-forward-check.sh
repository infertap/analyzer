#!/usr/bin/env bash
# The wall-clock check: the suite must pass with the system clock a month ahead.
#
# Every fixture is REBASED from the wall clock (`crates/harness/src/lib.rs:
# rebase_offset`, or a `now_ms()` of its own), never pinned to an absolute day, so nothing here
# should notice the date. Two tests once did — one failed on a calendar boundary, one under
# load — and the habit that keeps that from recurring is running the suite under a shifted
# clock now and then. This script is that run.
#
# **It moves the machine's clock, so it is for a disposable machine** — CI's `clock-forward`
# job runs it on a fresh runner. It is not for a development box: libfaketime cannot stand in,
# because the pinned engine is a static binary that no preloaded library reaches, and a run
# where the tests are a month ahead of the engine fails for that split, not for a fixture. One
# clock for every process, or no verdict.
#
# Only the `date` call is elevated (`sudo`, passwordless on a runner); the tests run as the
# invoking user. Running the whole suite as root inherits root's file-descriptor limit, and
# a spill can open more files than that allows: a red about `ulimit`, not about the calendar.
#
#   scripts/clock-forward-check.sh          +30 days (the default)
#   scripts/clock-forward-check.sh 400      any number of days
#
# Linux only (`date -s`). Judged by EXIT CODE.
set -uo pipefail
cd "$(dirname "$0")/.."
days="${1:-30}"

case "$(uname -s)" in
  Linux) ;;
  *) echo "clock-forward-check: sets the system clock with GNU date; run it on a Linux runner" >&2; exit 2 ;;
esac
if ! sudo -n true 2>/dev/null; then
  echo "clock-forward-check: needs passwordless sudo to set the clock; meant for a disposable machine" >&2
  exit 2
fi
if [ ! -x .tools/clickhouse ] && [ -z "${CLICKHOUSE_BIN:-}" ]; then
  echo "clock-forward-check: no pinned engine at .tools/clickhouse and CLICKHOUSE_BIN is unset" >&2
  exit 2
fi

# Everything the run needs from the network happens BEFORE the clock moves: a month of skew
# is inside every certificate's validity, but there is no reason to test that too.
if ! fetched=$(cargo fetch --locked 2>&1); then
  printf '%s\n' "$fetched" >&2
  echo "clock-forward-check: cargo fetch --locked failed (a stale Cargo.lock is the usual cause)" >&2
  exit 2
fi

# Compile on the real clock as well, so the build's file stamps do not sit in the future
# for the next run on this checkout. Only the test EXECUTION happens under the shift.
cargo test --workspace --no-run >/dev/null 2>&1 || { echo "clock-forward-check: the suite does not build" >&2; exit 2; }
cargo test -p analyzer-pipeline --no-run >/dev/null 2>&1

before=$(date +%s)
target=$(( before + days * 86400 ))
sudo -n date -u -s "@$target" >/dev/null || { echo "clock-forward-check: could not set the clock" >&2; exit 2; }
echo "clock-forward-check: the clock now reads $(date -u +%Y-%m-%dT%H:%M:%SZ), $days day(s) ahead"

fail=0
step() {
  printf '%-52s' "$1"; shift
  local t0=$SECONDS
  if "$@" >/tmp/clock-forward.$$ 2>&1; then
    printf 'ok      %3ds\n' "$((SECONDS - t0))"
  else
    printf 'FAILED  %3ds\n' "$((SECONDS - t0))"; fail=1
    tail -40 /tmp/clock-forward.$$ | sed 's/^/    /'
  fi
}
step "cargo test --workspace   (clock +${days}d)"     scripts/test.sh fast
step "pipeline battery         (clock +${days}d)"     scripts/test.sh engine pipeline
rm -f /tmp/clock-forward.$$
[ "$fail" -eq 0 ] && echo "clock-forward-check: green — nothing noticed the date" \
                  || echo "clock-forward-check: RED — a test read the calendar"
exit "$fail"
