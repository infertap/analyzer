# Keyed parity records

These files are evidence, not input. Each one records what the classification pass published
over one gait fixture (`fixtures/gait/`): the class and band cells and each source's daily
coverage, instants as offsets from the fixture's base day. The keyed driver is held to them by
`crates/pipeline/tests/keyed_equivalence.rs`. The pass itself is no longer in this tree.

## Provenance

The records were produced by the pass at commit `c7aadb1` ("test: what the pass published over
every gait fixture is committed, so the parity guard outlives it"). They are the last commit to
run the pass and the driver side by side. Do not edit them by hand. Do not regenerate them from
any later commit, because no later commit has the pass.

Three mechanical changes were made since, each a transform over every file that moved no
figure the pass stated:

1. When the served cells lost the `stamped_events` measure, which was zero on every row, that
   measure was dropped from the class and band cells' rows.
2. When the `tier` column left the class and band tables, having been `\N` on every row the
   pass ever wrote, `tier=\N` was dropped from every cell key.
3. When the examined history became an hour, a prior close an hour or more back came to decide
   nothing, so a store the pass banded `1h-1d` or `>1d` reads `cold` with no gap. Those stores'
   cells moved to `cold`/`none`, and the same events and tokens moved in the unbanded tables.
   Only G5 had such a store. The guard reads the files as they are.

## Reproducing them

Check out `c7aadb1`, then run the guard with recording on. This needs the pinned engine
(`docs/testing.md`):

    git checkout c7aadb1
    KEYED_PARITY_RECORD=1 cargo test -p analyzer-pipeline --test keyed_equivalence -- --ignored

The run writes every file here again. A diff after that run means the evidence changed.
