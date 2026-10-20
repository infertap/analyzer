#!/bin/sh
# The container's entrypoint.
#
# **Why a shell entrypoint.** The risk a shell entrypoint carries is real — configuration hidden in a shell, so the
# container takes different arguments than the systemd unit and a support answer written for one
# does not hold for the other. This script does not do that. It defaults exactly two values, both
# of which are WRONG BY DEFAULT in a container and in no other deployment, and passes everything
# else through untouched.
#
# The two: `--api-addr` and `--metrics-addr` both default to loopback in the binary, which is
# correct on a VM (reaching them from elsewhere should be a decision) and meaningless inside a
# network namespace, where loopback is reachable only from the same container. A port-forward or a
# Service would find nothing listening, with no error anywhere — the silent-failure shape this
# codebase keeps finding and closing. Correcting it here rather than in the chart means every way
# of running the image gets it, not just the one that reads the values file.
#
# Override with `INFERTAP_API_ADDR` / `INFERTAP_METRICS_ADDR`, or by passing the flags yourself:
# an explicit flag always wins, because the defaults are only appended when absent.
#
# Any other subcommand — `preflight`, `token`, `version`, `diag`, `restore` — runs untouched. That
# is what makes `kubectl exec … token list` and an init-container `preflight` work with the same
# image, which is the point of not baking `run` in.
set -eu

: "${INFERTAP_API_ADDR:=0.0.0.0:8088}"
: "${INFERTAP_METRICS_ADDR:=0.0.0.0:9363}"

analyzer=/usr/local/bin/infertap-analyzer

if [ "$#" -eq 0 ] || [ "$1" != "run" ]; then
    exec "$analyzer" "$@"
fi
shift

# Append only what the caller did not state. `--api-addr off` is a legitimate thing to pass and
# must survive, which is why this tests for the FLAG rather than for a value.
#
# The needle is shifted off before the scan: iterating `$@` with `$1` still in it compares the
# needle against itself on the first pass and reports every flag as present. And the test is a
# full `if` rather than `[ … ] && return 0`, because under `set -e` a false test at the end of an
# `&&` list is a failing command, which exits the script instead of continuing the loop.
has_flag() {
    needle=$1
    shift
    for arg in "$@"; do
        if [ "$arg" = "$needle" ]; then
            return 0
        fi
    done
    return 1
}

set -- run "$@"
if ! has_flag --api-addr "$@"; then
    set -- "$@" --api-addr "$INFERTAP_API_ADDR"
fi
if ! has_flag --metrics-addr "$@"; then
    set -- "$@" --metrics-addr "$INFERTAP_METRICS_ADDR"
fi

exec "$analyzer" "$@"
