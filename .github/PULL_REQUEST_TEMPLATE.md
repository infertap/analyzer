## What this changes

<!-- One task per pull request. Say what changed and why; the commit message carries the detail. -->

## Behaviour

<!-- Pick one and say how you know. -->

- [ ] Behaviour-preserving: the pins and the battery are unchanged and green.
- [ ] Behaviour changes: which golden or contract moved, and why that is the intended change.

## Checks

- [ ] `scripts/prepush.sh` is green (fmt, clippy with warnings denied, the tests, the engine-backed battery, the docs).
- [ ] A changed public surface has its baseline regenerated (`scripts/public-api.sh update`) in the same commit.
- [ ] A new dependency is on the static audit's allowlist, in `deny.toml`, and in the regenerated `THIRD-PARTY-NOTICES`.
- [ ] Documentation that describes what changed is updated in the same commit.
- [ ] I have signed the CLA (`CLA.md`), or I am a listed maintainer.
