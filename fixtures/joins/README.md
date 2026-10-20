# Join fixture corpus

Worked examples of `docs/classification-semantics.md`, one file per rule, each carrying a
primary case and its pair: the same shape with the one difference that flips the answer.
Implementations conform to these fixtures, never the reverse. Each file's `source` names the
section it pins.

Block ids are opaque strings; the analyzer treats identity as opaque. Instants are engine-domain
milliseconds from an arbitrary base, rebased to a recent day when a test ingests them. `params`
overrides the defaults of §10 per fixture or per case.

Expectations: `classifications` maps a record's `ref` to one of the six classes of §4
(`re_admit`, `RE`, `ETR`, `post_clear`, `cold`, `indeterminate`); `eviction_waste_tokens` and
`post_clear_tokens` are token masses; `note` is prose for the reader.

Three suites read this directory:

- `crates/pipeline/tests/fixtures_corpus.rs` asserts every expectation through real ingest and
  the classifier, and fails on an expectation key it does not grade.
- `crates/pipeline/tests/derived_snapshot.rs` pins the derived document of every case under
  `conformance/derived/`, and fails on a snapshot no case produces.
- `crates/pipeline/tests/differential_guards.rs` checks the partition law over every case:
  every audited evict lands in exactly one class.

When a fixture changes, the snapshot changes with it (`UPDATE_DERIVED_SNAPSHOTS=1`), and the
commit says why.
