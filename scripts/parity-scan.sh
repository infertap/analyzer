#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
#
# The engine parity scan: the analyzer's own schema, loaded into the pinned engine, and the
# engine behaviours the product relies on asserted against the real tables
# (scripts/ch-parity-scan.sql). A newer engine that changes one of those behaviours fails here,
# not in a deployment. `scripts/fetch-engine.sh` puts the pinned binary in `.tools/`; CI runs
# this same script on the same pin.
set -euo pipefail
cd "$(dirname "$0")/.."
PIN=$(cat CLICKHOUSE_VERSION)
BIN="${CLICKHOUSE_BIN:-.tools/clickhouse}"
ACTUAL=$("$BIN" local --version | grep -oE '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+')
if [ "$ACTUAL" != "$PIN" ]; then
  echo "parity-scan: binary is $ACTUAL, pin is $PIN — refusing to scan the wrong build" >&2
  exit 1
fi
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
# The schema as a fresh appliance applies it: no replication family, the default horizons.
sed -e 's/{engine_family}//g' \
    -e 's/{ledger_retention_days}/14/g' \
    -e 's/{rollup_retention_years}/5/g' \
    -e 's/{hour_retention_days}/365/g' \
    -e 's/{minute_retention_days}/7/g' \
    -e 's/{instance_detail_retention_days}/14/g' \
    crates/serving-clickhouse/migrations/0001_schema.sql > "$WORK/schema.sql"
if grep -q '{[a-z_]*}' "$WORK/schema.sql"; then
  echo "parity-scan: the schema carries a placeholder this script does not render:" >&2
  grep -o '{[a-z_]*}' "$WORK/schema.sql" | sort -u >&2
  exit 1
fi
cat "$WORK/schema.sql" scripts/ch-parity-scan.sql > "$WORK/scan.sql"
# stdin MUST be closed: clickhouse-local reads stdin as a query source even with --queries-file
# and blocks until EOF, which under a CI pipe is an indefinite hang.
"$BIN" local --path "$WORK/db" --queries-file "$WORK/scan.sql" < /dev/null
