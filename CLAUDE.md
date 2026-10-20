# Working in this repo

`AGENTS.md` is the short list for any coding agent. `CONTRIBUTING.md` holds the rules for
everyone. This file adds the two local gates and what they do not cover.

## Writing

One instruction per sentence. Use the active voice. Use one word for one meaning, and the
same word for the same thing every time. Keep sentences short. Docs state what the code is,
not what it replaced.

## Two gates: `scripts/preflight.sh` while working, `scripts/prepush.sh` before every push

`preflight.sh [crate]` is the inner loop: formatting, SPDX headers, runner checks, clippy with
warnings denied, and the engine-free tests. Omit the crate for workspace coverage. Run
integration targets with `scripts/test.sh engine <crate> [target]`; `docs/testing.md` lists
the focused commands and their limits.

`prepush.sh` is the push gate: `preflight.sh --full` (pipeline, API and daemon loop
integration included), `cargo doc --workspace --no-deps` with `RUSTDOCFLAGS=-Dwarnings`, and
`scripts/public-api.sh`, the crate-surface baseline. Push only on its green.

Both gates are judged by exit code. Never judge a check by grepping compiler output: a grep is
a filter that has to be right, and an exit code is the compiler's own verdict. CI runs clippy
with `-D warnings`, so a lint that is a warning locally is an error there.

`cargo doc` is in the push gate and not in the inner loop because rustdoc uses a third set of
flags: a third full build on every inner-loop run makes a gate nobody runs.

## After pushing, read CI

    gh run list --limit 1
    gh run view <id> --json jobs -q '.jobs[]|"\(.conclusion) \(.name)"'

"Tests pass locally" and "the pipeline passes" are different claims. Read the jobs after
every push.

## What preflight does not cover

The `parity`, `audit`, `deny` and `sbom` jobs need the pinned engine binary and network
installs, so they are CI-only. If one of them fails, read its log rather than assuming the
local gate would have caught it.

## Adding a dependency

`cargo deny` gates the allowlist in CI. A new crate fails the `deny` job until it is added
deliberately. That is the mechanism working, not an obstacle to route around.
