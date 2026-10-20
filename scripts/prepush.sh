#!/usr/bin/env bash
# Comprehensive PR correctness suites, documentation and generated contracts.
set -uo pipefail
cd "$(dirname "$0")/.."

fail=0

# preflight FIRST and on its own: it is the cheaper gate and the one that fails more often, so a
# broken build should not wait behind a rustdoc pass to say so. Run bare and read its exit —
# never through a pipe, which gates on the LAST command in the chain rather than on the gate.
./scripts/preflight.sh --full
preflight_status=$?
[ "$preflight_status" -eq 0 ] || fail=1

echo
# **`RUSTDOCFLAGS: -Dwarnings` is not optional and is the easy half to get wrong.** CI sets it
# (.github/workflows/ci.yml), so a doc WARNING is an ERROR there. Running plain `cargo doc` proves
# nothing about the pipeline for exactly the reason plain `cargo clippy` does not — the same lesson
# one tool over.
printf '%-46s' "cargo doc --workspace --no-deps -Dw"
t0=$SECONDS
if RUSTDOCFLAGS=-Dwarnings cargo doc --workspace --no-deps >/tmp/prepush.$$ 2>&1; then
  printf 'ok      %3ds\n' "$((SECONDS - t0))"
else
  printf 'FAILED  %3ds\n' "$((SECONDS - t0))"; fail=1
  sed -n '1,25p' /tmp/prepush.$$ | sed 's/^/    /'
fi
rm -f /tmp/prepush.$$

echo
# **The public surface, for the same reason as the docs.** CI's `public-api` job diffs every
# crate's built surface against `contract/public-api/*.txt`, and a `pub` item added, removed or
# re-signed without a re-blessed baseline reddens mainline AFTER the push. The check is cheap beside the doc build and belongs where it protects something: the push. An
# intended change re-blesses with `scripts/public-api.sh update` and commits the baseline.
printf '%-46s' "scripts/public-api.sh"
t0=$SECONDS
if ./scripts/public-api.sh >/tmp/prepush.$$ 2>&1; then
  printf 'ok      %3ds\n' "$((SECONDS - t0))"
else
  printf 'FAILED  %3ds\n' "$((SECONDS - t0))"; fail=1
  sed -n '1,40p' /tmp/prepush.$$ | sed 's/^/    /'
fi
rm -f /tmp/prepush.$$

echo
# **The third-party notices, for the same reason again.** CI's `deny` job checks that
# THIRD-PARTY-NOTICES covers every crate that ships; a dependency added without regenerating the
# file reddens mainline after the push. Seconds to run; regenerate with `scripts/third-party-notices.py > THIRD-PARTY-NOTICES` and commit the file.
printf '%-46s' "scripts/third-party-notices.py --check"
t0=$SECONDS
if ./scripts/third-party-notices.py --check >/tmp/prepush.$$ 2>&1; then
  printf 'ok      %3ds\n' "$((SECONDS - t0))"
else
  printf 'FAILED  %3ds\n' "$((SECONDS - t0))"; fail=1
  sed -n '1,20p' /tmp/prepush.$$ | sed 's/^/    /'
fi
rm -f /tmp/prepush.$$

echo
# **The OpenAPI document, for the same reason a fourth time.** CI's `contract` job diffs
# contract/openapi.json against what the api crate generates, and the wire types' doc comments
# ARE that document's descriptions — so a documented field reddens mainline after the push.
# Regenerate with
# `cargo run -p analyzer-api --bin dump-openapi > contract/openapi.json` and commit the diff;
# the console is built against this document.
printf '%-46s' "contract/openapi.json is current"
t0=$SECONDS
if cargo run -q --locked -p analyzer-api --bin dump-openapi >/tmp/prepush.$$.json 2>/tmp/prepush.$$ \
   && diff -u contract/openapi.json /tmp/prepush.$$.json >>/tmp/prepush.$$ 2>&1; then
  printf 'ok      %3ds\n' "$((SECONDS - t0))"
else
  printf 'FAILED  %3ds\n' "$((SECONDS - t0))"; fail=1
  sed -n '1,30p' /tmp/prepush.$$ | sed 's/^/    /'
fi
rm -f /tmp/prepush.$$ /tmp/prepush.$$.json

echo
if [ "$fail" -eq 0 ]; then
  echo "prepush: green — safe to push"
else
  echo "prepush: RED — do not push"
fi
exit "$fail"
