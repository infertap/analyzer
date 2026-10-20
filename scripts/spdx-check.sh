#!/usr/bin/env bash
# Every Rust source file carries the licence as its FIRST line:
#     // SPDX-License-Identifier: AGPL-3.0-only
# so a file copied out of the tree still says what it is licensed under. Judged by exit
# code, run by CI's `check` job and by scripts/preflight.sh. Lists every offender, then
# fails once.
set -uo pipefail
cd "$(dirname "$0")/.."
want='// SPDX-License-Identifier: AGPL-3.0-only'
missing=0
while IFS= read -r f; do
  [ -f "$f" ] || continue # tracked files may be deleted in the working tree
  if [ "$(head -n 1 "$f")" != "$want" ]; then
    echo "spdx-check: $f: line 1 must be \`$want\`" >&2
    missing=1
  fi
done < <(git ls-files --cached --others --exclude-standard '*.rs')
[ "$missing" -eq 0 ] && echo "spdx-check: every .rs file carries the header"
exit "$missing"
