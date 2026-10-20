#!/usr/bin/env bash
# The public-API guard: every library crate's public surface is a committed baseline under
# contract/public-api/, diffed here. A change to what a partner can link — a renamed `pub fn`,
# a widened signature, an item that went `pub(crate)` — is a reviewable diff in the commit
# that causes it, never a drift. Same discipline as contract/openapi.json, one layer down.
#
#   scripts/public-api.sh check    diff the current surface against the baselines (CI's job)
#   scripts/public-api.sh update   regenerate the baselines after an INTENDED change
#
# Needs `cargo-public-api` (pinned below) and a nightly toolchain: the surface is read from
# rustdoc's JSON output, which only nightly emits. The crates themselves stay on the pinned
# stable toolchain; nightly builds nothing that ships.
#
# Judged by EXIT CODE, like every gate here.
set -uo pipefail
cd "$(dirname "$0")/.."

PUBLIC_API_VERSION=0.52.0
CRATES=(analysis segments objectstore lake ingest pipeline serving-clickhouse api daemon module-example)
BASE=contract/public-api
mode="${1:-check}"
PUBLIC_API_TOOLCHAIN=$(cat contract/public-api/TOOLCHAIN)

if ! cargo public-api --version 2>/dev/null | grep -q "$PUBLIC_API_VERSION"; then
  echo "public-api: needs cargo-public-api $PUBLIC_API_VERSION:" \
       "cargo install cargo-public-api --version $PUBLIC_API_VERSION --locked" >&2
  exit 2
fi
if ! rustup run "$PUBLIC_API_TOOLCHAIN" rustc --version >/dev/null 2>&1; then
  echo "public-api: needs a nightly toolchain for rustdoc JSON:" \
       "rustup toolchain install $PUBLIC_API_TOOLCHAIN --profile minimal" >&2
  exit 2
fi

surface() {
  # RUSTFLAGS cleared on purpose: CI exports `-D warnings`, and a lint nightly adds that stable
  # does not have would turn a surface read into a build failure about something else.
  # `-ss` omits blanket and auto-trait impls, which are noise that moves with the compiler.
  # Pin rustdoc with the baselines so compiler changes cannot alter their rendering.
  RUSTFLAGS= cargo +"$PUBLIC_API_TOOLCHAIN" public-api -p "analyzer-$1" -ss --color never 2>/dev/null
}

fail=0
mkdir -p "$BASE"
for c in "${CRATES[@]}"; do
  current="$(surface "$c")" || { echo "public-api: could not read the surface of $c" >&2; exit 2; }
  file="$BASE/$c.txt"
  if [ "$mode" = update ]; then
    printf '%s\n' "$current" > "$file"
    echo "public-api: wrote $file"
    continue
  fi
  if [ ! -f "$file" ]; then
    echo "public-api: no baseline for $c at $file — run: scripts/public-api.sh update" >&2
    fail=1; continue
  fi
  if ! diff -u "$file" <(printf '%s\n' "$current") >/tmp/public-api.$$ 2>&1; then
    fail=1
    echo "public-api: the public surface of crates/$c CHANGED against $file:"
    sed 's/^/    /' /tmp/public-api.$$
    cat <<EOF
    A line starting with '-' left the public API (a breaking change for anyone linking the
    crate); a line starting with '+' joined it (new API to support from now on). If this is
    intended, regenerate the baseline and commit it beside the change:
        scripts/public-api.sh update && git add $BASE
    If it is not, the item probably wants pub(crate) (see ARCHITECTURE.md, the facades).
EOF
  fi
done
rm -f /tmp/public-api.$$
[ "$fail" -eq 0 ] && echo "public-api: green — the surface matches the baselines"
exit "$fail"
