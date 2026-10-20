# The lake

Everything the analyzer keeps lives in the bucket, under its own root beside the shipper's dated
tree. The box holds a working copy and caches; losing the box loses neither input nor output.
This is the map; each part's module documentation in `crates/lake` carries the detail.

```
<prefix><yyyy-mm-dd>/<hh>/...                          the tap's segments (the shipper's tree)
<prefix>_analyzer/namespaces/<ns>/lineages/<id>/
    ledger/shards/<shard>/head, lease, days/<day>/...  the input log
    journal/head, journal/lease                        the execution journal
    state-db/...                                       the working state's checkpoints
    keyed-spine/...                                    the log partitioned by block, and the
                                                       frontiers carried at each day boundary
    fold/head, fold/days/<day>/head                    the published state's manifests
    state/<table>/<semantics>/<discriminator>/<day>/   the published state's logs
```

## Lineages and leases

A **lineage** is one derivation of one fleet's input and the writer that may commit it
(`lake::lineage`). Each analyzer's data directory names its lineage, created once and read back on
every start, so a restart resumes its own journal. Within a lineage exactly one writer commits: it
holds a lease object beside the journal head, acquired and renewed by conditional writes, and every
commit stamps the lease's epoch. A writer whose epoch is older than the head's is fenced, whatever
its clock says. A standby sharing the lineage waits for the lease to expire and takes it over with
the next epoch.

A **head** (`lake::head`) is one document advanced only by a conditional write by the lease's
holder. After a refusal or a transport error the head is re-read: a change already in it stands,
otherwise it is applied to the document read and written once more. The journal and the input log
are heads.

## The input log

What arrived (`lake::ledger`). An ingest cycle takes one input position, writes one record object
per UTC day it touched, then commits the shard's head once. Each record is a batch: one segment,
named by its content hash, with its arrivals (heartbeat spans, declared drops, run lifecycle), its
stored mass per instance and day, its tag statements and annotations. A record object counts once
its position is committed under the epoch that owns it, so a fenced writer's orphans are never
read. A closed day compacts into one manifest; days past input retention retire.

The input log is the derivation's only input: every derived thing in the lake is a function of it,
which is why `reset-derived` can delete them all.

## The execution journal

One head per lineage (`pipeline::journal`): the namespace binding, the clocks, the keyed tick's
intent (its publication, and whether it was applied) and the input position the served input
tables are published through. A commit is one write of the whole head, so changes
that span records land together or not at all. Readers see a tick's edges only once its intent is
applied ([the pipeline](pipeline.md), section 4).

## The working state

The keyed operators' state: frontiers, placement's seeds, the held cells, rings and
the head that names the tick, each operator's edges, the meanings' day ranges and each module's
span (`lake::working`). It lives in one embedded
`RocksDB` per lineage and process, one column family per relation, and writes in units of work: a
tick's changes land together or roll back together.

Durability is the bucket's (`lake::working::durable`): an incremental checkpoint is a hard-link
snapshot taken when a unit ends, once enough bytes have been committed or enough time has passed,
uploaded by a thread of its own and named in the working state's head under the journal's lease. A
restore makes the newest checkpoint the live database and replays the input log from the position
it names. A checkpoint past the retained few is deleted once it has been unnamed long enough that
no restore that began before can lose a file.

## The published state

Every published table's day as a keyed log (`lake::state`): a restatement of the day writes a
**base**, a keyed change a **delta**, each at its publication, and a reader takes the base and the
deltas above it. The core reads its own output back from here, never from serving.

Nothing is found by listing: a write puts its objects, then commits their names in one conditional
write of the day's manifest (`lake::fold`) under the journal's lease. Maintenance retires the days
older than the rollups' horizon, the longest any serving table keeps: the manifest first, then
its objects. An object a failed publication
left is named nowhere and never read. Every object's name carries a revision the manifest reserved,
so no name is written twice, a retried publication included.

## The box

The data directory holds the working state's live database, a cache of immutable lake objects
(`lake::cache`: an object is cached only if it never changes under its key, so a cached copy is
never stale), the segment cache and the serving engine's files. All of it can be rebuilt from the
bucket.
