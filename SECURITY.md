# Security

## Reporting a vulnerability

Do not open a public issue for a vulnerability. Report it privately:

- Email `cort@infertap.ai` with `SECURITY` in the subject line. Include the version
  (`infertap-analyzer --version`), what the analyzer was doing, how to reproduce, and what
  an attacker gains.
- GitHub's private vulnerability reporting ("Report a vulnerability" under the Security tab)
  is the same channel and reaches the same people.

You will get an acknowledgement within three business days. We aim to confirm or refute the
report within ten, and to ship a fix in the next release, with a coordinated disclosure date
agreed with you — ninety days from the report at most, sooner when a fix is out. We credit
reporters in the release notes unless they ask otherwise.

## Scope

- The analyzer: the `infertap-analyzer` binary, its release bundle, the container image and
  the Helm chart in this repository, and the read API it serves.
- The pinned ClickHouse engine it redistributes: a vulnerability there is reported upstream
  and tracked here as a pin bump.
- The tap and the console are separate repositories with their own policies.

## Supported versions

The latest `1.x` release receives security fixes. Older releases do not; upgrade.

## What we already do

Dependencies are audited on every push (`cargo audit`, `cargo deny`); release artifacts
are signed and carry a software bill of materials; the engine binary is fetched by pinned
hash; the analyzer serves its API on loopback by default and its bootstrap token is written
to a file, never to the journal. `ARCHITECTURE.md` describes the boundaries.
