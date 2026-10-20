#!/usr/bin/env bash
# Fetch the pinned ClickHouse engine into `.tools/clickhouse`, hash-verified — the one
# command a fresh checkout needs before the engine-backed tests (`cargo test -p
# analyzer-pipeline -- --ignored`, `scripts/preflight.sh`) can run. CI calls this same
# script, so a contributor's box and the runner fetch the same bytes the same way.
#
# The pin is `CLICKHOUSE_VERSION`; the digest of each platform's release asset is in
# `CLICKHOUSE_SHA256`, one line per asset in `sha256sum -c` form. A download whose digest
# is not the pinned one is refused, never installed. Idempotent: a binary already at the
# path that reports the pinned version is kept.
#
# `CLICKHOUSE_BIN` overrides the path everywhere the tests and scripts look, so a box with
# its own build of the pinned version needs nothing from here.
set -euo pipefail
cd "$(dirname "$0")/.."

pin=$(cat CLICKHOUSE_VERSION)
dest=.tools/clickhouse

if [ -x "$dest" ] && "$dest" --version 2>/dev/null | grep -q " version ${pin} "; then
  echo "fetch-engine: ${dest} is ${pin} already"
  exit 0
fi

os=$(uname -s)
arch=$(uname -m)
case "${os}/${arch}" in
  Linux/x86_64)   asset="clickhouse-common-static-${pin}-amd64.tgz"; kind=tgz ;;
  Linux/aarch64)  asset="clickhouse-common-static-${pin}-arm64.tgz"; kind=tgz ;;
  Darwin/arm64)   asset="clickhouse-macos-aarch64"; kind=bin ;;
  Darwin/x86_64)  asset="clickhouse-macos"; kind=bin ;;
  *)
    echo "fetch-engine: no pinned build for ${os}/${arch}; build or install ClickHouse ${pin}" \
         "yourself and point CLICKHOUSE_BIN at it" >&2
    exit 1 ;;
esac

want=$(awk -v a="$asset" '$2 == a { print $1 }' CLICKHOUSE_SHA256)
if [ -z "$want" ]; then
  echo "fetch-engine: CLICKHOUSE_SHA256 pins no digest for ${asset}" >&2
  exit 1
fi

url="https://github.com/ClickHouse/ClickHouse/releases/download/v${pin}-lts/${asset}"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
echo "fetch-engine: ${url}"
curl -sSfL --retry 3 --max-time 900 -o "${tmp}/${asset}" "$url"

if command -v sha256sum >/dev/null 2>&1; then
  got=$(sha256sum "${tmp}/${asset}" | cut -d' ' -f1)
else
  got=$(shasum -a 256 "${tmp}/${asset}" | cut -d' ' -f1)
fi
if [ "$got" != "$want" ]; then
  echo "fetch-engine: ${asset} downloaded with digest ${got}; the pin is ${want} — refusing to install it" >&2
  exit 1
fi

mkdir -p .tools
case "$kind" in
  tgz)
    tar xzf "${tmp}/${asset}" -C "$tmp"
    mv "${tmp}/clickhouse-common-static-${pin}/usr/bin/clickhouse" "$dest"
    ;;
  bin)
    mv "${tmp}/${asset}" "$dest"
    chmod +x "$dest"
    ;;
esac
"$dest" --version
echo "fetch-engine: installed ${dest} (${asset}, sha256 ${got})"
