# For coding agents

This file is for any AI coding agent working in this repository. It is short on purpose:
the rules are in `CONTRIBUTING.md`, and they apply to an agent exactly as they apply to a
person.

- Read `CONTRIBUTING.md` first, then `ARCHITECTURE.md` for the map and `GLOSSARY.md` for
  the words.
- One task per commit. Run `scripts/preflight.sh` while working and `scripts/prepush.sh`
  before a push; both are judged by exit code, never by reading compiler output.
- Behaviour is what the pins say. Never re-bless a golden to make a diff go away; a golden
  that moves is a finding to explain in the commit.
- The static audit (`crates/cli/tests/static_audit.rs`) tells you the exact edit when it
  goes red. Make that edit; do not route around the check.
- A contribution from an agent is a contribution: it goes through a pull request, a
  maintainer's review and the CLA like any other.
