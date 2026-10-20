#!/usr/bin/env bash
# Put a verified console bundle where `crates/api/build.rs` will embed it.
#
# **The hash is the pin; this script is only a transport.** `console/PIN.toml` names a version and
# a content hash, and nothing here is trusted to produce the right bytes — whatever arrives is
# hashed and compared before it is allowed to stay. That is what lets the SOURCE of the bytes
# change later (a local build today, a signed release tarball once the console has a release
# pipeline) without anything else in this repo moving.
#
# Two sources, same verification:
#
#   scripts/stage-console.sh ~/workspace/console          # a local checkout's dist/
#   scripts/stage-console.sh /path/to/console-1.0.0.tgz   # a release tarball
#
# Source maps are stripped here rather than at embed time, so the staged tree IS the embedded
# tree and one hash covers both. They are 1.3 MB of a 1.9 MB dist and a readable copy of
# the console's source; they belong in a support artifact, never in a customer's binary.
set -euo pipefail
cd "$(dirname "$0")/.."

source=${1:?usage: stage-console.sh <console-checkout|dist-tarball>}
staged=crates/api/console-dist
pin=console/PIN.toml

want_version=$(grep -E '^version' "$pin" | head -1 | cut -d'"' -f2)
want_hash=$(grep -E '^content_hash' "$pin" | head -1 | cut -d'"' -f2)

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

if [ -d "$source" ]; then
  dist="$source/dist"
  [ -d "$dist" ] || { echo "stage-console: $dist does not exist — run \`npm run build\` in $source" >&2; exit 1; }
  cp -R "$dist/." "$work/"
elif [ -f "$source" ]; then
  tar -xzf "$source" -C "$work" --strip-components=1
else
  echo "stage-console: $source is neither a console checkout nor a tarball" >&2
  exit 1
fi

find "$work" -name '*.map' -delete

got=$(scripts/console-hash.sh "$work")
if [ "$got" != "$want_hash" ]; then
  echo "stage-console: REFUSED — content hash does not match $pin" >&2
  echo "  pinned  ($want_version): $want_hash" >&2
  echo "  staged:                  $got" >&2
  echo >&2
  echo "If this is a deliberate console bump, update $pin in the same commit that takes the" >&2
  echo "new bundle — the pin moving is the reviewable diff. If it is not, the bytes are wrong." >&2
  exit 1
fi

rm -rf "$staged"
mkdir -p "$staged"
cp -R "$work/." "$staged/"
echo "stage-console: $want_version staged at $staged ($got)"
