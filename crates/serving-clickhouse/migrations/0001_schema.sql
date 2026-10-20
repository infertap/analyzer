-- ===========================================================================================
-- THE SCHEMA: the serving store's running shape, in one file.
--
-- Every table is spelled through the cell's placeholder {engine_family} (empty on the
-- appliance, Replicated in a cell that replicates); every horizon reads its retention knob
-- (RetentionConfig). The input log and the journal are the lake's (`docs/lake.md`), not
-- tables. schema_migrations, the ledger this file is recorded in, is created by migrate itself.
--
-- Every table the analyzer writes is here, and nothing in the binary creates one, save the
-- tables a registered module declares.
-- ===========================================================================================


-- ---------------------------------------------------------------- SERVED INPUT FACTS — what arrived, as readers see it

-- Published by the core from the input log (`docs/lake.md`): record grain,
-- keyed by identity, under the input meaning (semantics_version = the input records'
-- format, discriminator empty). Readers group at read. Each ages from its ingest, as the
-- ledger did: a fixture's historic event time must not expire it at the first merge. The
-- stored-mass denominator, facts_rollup_daily, is the fourth, beside the rollups it
-- divides.
CREATE TABLE IF NOT EXISTS input_batches
(
    fleet_id LowCardinality(String),
    semantics_version LowCardinality(String),
    discriminator LowCardinality(String),
    batch_id String,
    carrier LowCardinality(String),
    source String,
    object_key String,
    ingested_at_ms Int64,
    record_count Int64,
    unknown_kinds Int64,
    first_at_ms Nullable(Float64),
    last_at_ms Nullable(Float64),
    input_position Int64,
    version UInt64 DEFAULT 0,
    gone UInt8 DEFAULT 0
)
ENGINE = {engine_family}ReplacingMergeTree(version, gone)
ORDER BY (fleet_id, semantics_version, discriminator, batch_id)
TTL toDateTime(intDiv(ingested_at_ms, 1000)) + INTERVAL {ledger_retention_days} DAY DELETE;

CREATE TABLE IF NOT EXISTS input_segments
(
    fleet_id LowCardinality(String),
    semantics_version LowCardinality(String),
    discriminator LowCardinality(String),
    incarnation String,
    segment_seq Int64,
    instance String,
    batch_id String,
    first_record_at_ms Float64,
    last_record_at_ms Float64,
    heartbeat_secs Int64,
    max_segment_secs Int64,
    hb_count Int64,
    hb_first_at_ms Nullable(Float64),
    hb_last_at_ms Nullable(Float64),
    stop_at_ms Nullable(Float64),
    msgs_seen Int64,
    dropped Int64,
    oversized Int64,
    dropped_at_hb_first Int64,
    oversized_at_hb_first Int64,
    publisher_restarts Int64,
    agent_version String,
    topic_model String,
    topic_pod String,
    declared_model String,
    declared_pod String,
    engine_version String,
    attributed UInt32,
    canary Nullable(String),
    ingested_at_ms Int64,
    version UInt64 DEFAULT 0,
    gone UInt8 DEFAULT 0
)
ENGINE = {engine_family}ReplacingMergeTree(version, gone)
ORDER BY (fleet_id, semantics_version, discriminator, incarnation, segment_seq, instance, batch_id)
TTL toDateTime(intDiv(ingested_at_ms, 1000)) + INTERVAL {ledger_retention_days} DAY DELETE;

CREATE TABLE IF NOT EXISTS input_drops
(
    fleet_id LowCardinality(String),
    semantics_version LowCardinality(String),
    discriminator LowCardinality(String),
    incarnation String,
    first_seq Int64,
    last_seq Int64,
    batch_id String,
    at_ms Float64,
    ingested_at_ms Int64,
    version UInt64 DEFAULT 0,
    gone UInt8 DEFAULT 0
)
ENGINE = {engine_family}ReplacingMergeTree(version, gone)
ORDER BY (fleet_id, semantics_version, discriminator, incarnation, first_seq, last_seq, batch_id)
TTL toDateTime(intDiv(ingested_at_ms, 1000)) + INTERVAL {ledger_retention_days} DAY DELETE;

-- Each run's key space: the canary its segments declared (spec 3.4), kept as long as the rollups
-- whose zeros it qualifies (spec 5.7). `at_ms` is the latest segment that declared it.
CREATE TABLE IF NOT EXISTS input_key_spaces
(
    fleet_id LowCardinality(String),
    semantics_version LowCardinality(String),
    discriminator LowCardinality(String),
    incarnation String,
    canary String,
    at_ms Float64,
    version UInt64 DEFAULT 0,
    gone UInt8 DEFAULT 0
)
ENGINE = {engine_family}ReplacingMergeTree(version, gone)
ORDER BY (fleet_id, semantics_version, discriminator, incarnation, canary)
TTL toDate(toDateTime(at_ms / 1000)) + INTERVAL {rollup_retention_years} YEAR DELETE;


-- The fleet's instances as the roster states them.
CREATE TABLE IF NOT EXISTS fleet_inventory
(
    fleet_id LowCardinality(String),
    semantics_version LowCardinality(String),
    discriminator LowCardinality(String),
    instance_id LowCardinality(String),
    incarnation LowCardinality(String),
    first_seen_ms Float64,
    last_seen_ms Float64,
    stop_observed UInt32,
    model LowCardinality(Nullable(String)),
    pod_identity Nullable(String),
    heartbeat_secs Nullable(UInt32),
    max_segment_secs Nullable(UInt32),
    version UInt64 DEFAULT 0,
    gone UInt8 DEFAULT 0
)
ENGINE = {engine_family}ReplacingMergeTree(version, gone)
ORDER BY (fleet_id, semantics_version, discriminator, instance_id, incarnation)
TTL toDateTime(last_seen_ms / 1000) + INTERVAL {ledger_retention_days} DAY DELETE;


-- ---------------------------------------------------------------- THE STORED-MASS DENOMINATOR
-- ingest writes per batch. No table holds an event: the keyed pipeline re-derives a block's
-- stores from the log partitioned by block, and a holder's newest from its ring.

CREATE TABLE IF NOT EXISTS facts_rollup_daily
(
    fleet_id LowCardinality(String),
    semantics_version LowCardinality(String),
    discriminator LowCardinality(String),
    instance_id LowCardinality(String),
    kind LowCardinality(String),
    day Date,
    batch_id String,
    records Int64,
    tokens Int64,
    incarnation String,
    own Int64,
    comparable Int64,
    version UInt64 DEFAULT 0,
    gone UInt8 DEFAULT 0
)
ENGINE = {engine_family}ReplacingMergeTree(version, gone)
PARTITION BY toYYYYMM(day)
ORDER BY (fleet_id, semantics_version, discriminator, instance_id, kind, day, batch_id)
TTL day + INTERVAL {rollup_retention_years} YEAR DELETE;

-- ---------------------------------------------------------------- THE MARKERS — where meaning changed

CREATE TABLE IF NOT EXISTS markers
(
    fleet_id LowCardinality(String),
    semantics_version LowCardinality(String),
    discriminator LowCardinality(String),
    at_ms Float64,
    kind LowCardinality(String),
    label String,
    provenance LowCardinality(String),
    from_value Nullable(String),
    to_value Nullable(String),
    source LowCardinality(String),
    scope Nullable(String),
    declared_at_ms Nullable(Float64),
    version UInt64 DEFAULT 0,
    gone UInt8 DEFAULT 0
)
ENGINE = {engine_family}ReplacingMergeTree(version, gone)
ORDER BY (fleet_id, semantics_version, discriminator, at_ms, kind, label)
TTL toDate(toDateTime(at_ms / 1000)) + INTERVAL {rollup_retention_years} YEAR DELETE;


-- ---------------------------------------------------------------- THE FIGURE ROLLUPS — the class and band cells, each cell written whole by version

CREATE TABLE IF NOT EXISTS class_rollup_hourly
(
    fleet_id LowCardinality(String),
    bucket DateTime,
    discriminator LowCardinality(String),
    class LowCardinality(String),
    workload_class LowCardinality(String) DEFAULT 'undeclared',
    model LowCardinality(Nullable(String)),
    instance_id LowCardinality(String),
    dp_rank UInt32,
    group_idx UInt32,
    semantics_version LowCardinality(String),
    events Int64,
    tokens Int64,
    -- The fold's write, whole: latest version per key is the cell; gone = 1 a tombstone.
    version UInt64 DEFAULT 0,
    gone UInt8 DEFAULT 0
)
ENGINE = {engine_family}ReplacingMergeTree(version, gone)
PARTITION BY toYYYYMMDD(bucket)
ORDER BY (fleet_id, bucket, semantics_version, discriminator, class, workload_class, model, instance_id, dp_rank, group_idx)
TTL bucket + INTERVAL {hour_retention_days} DAY DELETE
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS class_rollup_daily
(
    fleet_id LowCardinality(String),
    discriminator LowCardinality(String),
    class LowCardinality(String),
    workload_class LowCardinality(String) DEFAULT 'undeclared',
    model LowCardinality(Nullable(String)),
    instance_id LowCardinality(String),
    dp_rank UInt32,
    group_idx UInt32,
    semantics_version LowCardinality(String),
    month Date,
    bucket Date,
    events Int64,
    tokens Int64,
    -- The fold's write, whole: latest version per key is the cell; gone = 1 a tombstone.
    version UInt64 DEFAULT 0,
    gone UInt8 DEFAULT 0
)
ENGINE = {engine_family}ReplacingMergeTree(version, gone)
PARTITION BY month
ORDER BY (fleet_id, semantics_version, discriminator, class, workload_class, model, instance_id, dp_rank, group_idx, month, bucket)
TTL bucket + INTERVAL {rollup_retention_years} YEAR DELETE
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS band_rollup_hourly
(
    fleet_id LowCardinality(String),
    discriminator LowCardinality(String),
    class LowCardinality(String),
    band LowCardinality(String),
    workload_class LowCardinality(String) DEFAULT 'undeclared',
    model LowCardinality(Nullable(String)),
    instance_id LowCardinality(String),
    dp_rank UInt32,
    group_idx UInt32,
    semantics_version LowCardinality(String),
    bucket DateTime,
    events Int64,
    tokens Int64,
    -- The fold's write, whole: latest version per key is the cell; gone = 1 a tombstone.
    version UInt64 DEFAULT 0,
    gone UInt8 DEFAULT 0
)
ENGINE = {engine_family}ReplacingMergeTree(version, gone)
PARTITION BY toYYYYMM(bucket)
ORDER BY (fleet_id, bucket, semantics_version, discriminator, class, band, workload_class, model, instance_id, dp_rank, group_idx)
TTL bucket + INTERVAL {hour_retention_days} DAY DELETE
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS band_rollup_daily
(
    fleet_id LowCardinality(String),
    discriminator LowCardinality(String),
    class LowCardinality(String),
    band LowCardinality(String),
    workload_class LowCardinality(String) DEFAULT 'undeclared',
    model LowCardinality(Nullable(String)),
    instance_id LowCardinality(String),
    dp_rank UInt32,
    group_idx UInt32,
    semantics_version LowCardinality(String),
    month Date,
    bucket Date,
    events Int64,
    tokens Int64,
    -- The fold's write, whole: latest version per key is the cell; gone = 1 a tombstone.
    version UInt64 DEFAULT 0,
    gone UInt8 DEFAULT 0
)
ENGINE = {engine_family}ReplacingMergeTree(version, gone)
PARTITION BY month
ORDER BY (fleet_id, semantics_version, discriminator, class, band, workload_class, model, instance_id, dp_rank, group_idx, month, bucket)
TTL bucket + INTERVAL {rollup_retention_years} YEAR DELETE
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS recompute_minute
(
    fleet_id LowCardinality(String),
    semantics_version LowCardinality(String),
    discriminator LowCardinality(String),
    class LowCardinality(String),
    workload_class LowCardinality(String),
    model LowCardinality(Nullable(String)),
    bucket DateTime,
    events Int64,
    tokens Int64,
    emitted_at DateTime DEFAULT now(),
    -- The fold's write, whole: latest version per key is the cell; gone = 1 a tombstone.
    version UInt64 DEFAULT 0,
    gone UInt8 DEFAULT 0
)
ENGINE = {engine_family}ReplacingMergeTree(version, gone)
ORDER BY (fleet_id, semantics_version, discriminator, class, workload_class, model, bucket)
TTL bucket + INTERVAL {minute_retention_days} DAY DELETE
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS band_minute
(
    fleet_id LowCardinality(String),
    semantics_version LowCardinality(String),
    discriminator LowCardinality(String),
    class LowCardinality(String),
    band LowCardinality(String),
    workload_class LowCardinality(String) DEFAULT 'undeclared',
    model LowCardinality(Nullable(String)),
    bucket DateTime,
    events Int64,
    tokens Int64,
    emitted_at DateTime DEFAULT now(),
    -- The fold's write, whole: latest version per key is the cell; gone = 1 a tombstone.
    version UInt64 DEFAULT 0,
    gone UInt8 DEFAULT 0
)
ENGINE = {engine_family}ReplacingMergeTree(version, gone)
ORDER BY (fleet_id, semantics_version, discriminator, class, band, workload_class, model, bucket)
TTL bucket + INTERVAL {minute_retention_days} DAY DELETE
SETTINGS allow_nullable_key = 1;


-- ---------------------------------------------------------------- THE RANK-GRAIN MINUTE TIER

CREATE TABLE IF NOT EXISTS recompute_rank_minute
(
    fleet_id LowCardinality(String),
    semantics_version LowCardinality(String),
    discriminator LowCardinality(String),
    instance_id LowCardinality(String),
    dp_rank UInt32,
    bucket DateTime,
    class LowCardinality(String),
    workload_class LowCardinality(String) DEFAULT 'undeclared',
    model LowCardinality(Nullable(String)),
    events Int64,
    tokens Int64,
    -- The fold's write, whole: latest version per key is the cell; gone = 1 a tombstone.
    version UInt64 DEFAULT 0,
    gone UInt8 DEFAULT 0
)
ENGINE = {engine_family}ReplacingMergeTree(version, gone)
ORDER BY (fleet_id, semantics_version, discriminator, instance_id, dp_rank, bucket, class, workload_class, model)
TTL bucket + INTERVAL {minute_retention_days} DAY DELETE
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS band_rank_minute
(
    fleet_id LowCardinality(String),
    semantics_version LowCardinality(String),
    discriminator LowCardinality(String),
    instance_id LowCardinality(String),
    dp_rank UInt32,
    bucket DateTime,
    class LowCardinality(String),
    band LowCardinality(String),
    workload_class LowCardinality(String) DEFAULT 'undeclared',
    model LowCardinality(Nullable(String)),
    events Int64,
    tokens Int64,
    -- The fold's write, whole: latest version per key is the cell; gone = 1 a tombstone.
    version UInt64 DEFAULT 0,
    gone UInt8 DEFAULT 0
)
ENGINE = {engine_family}ReplacingMergeTree(version, gone)
ORDER BY (fleet_id, semantics_version, discriminator, instance_id, dp_rank, bucket, class, band, workload_class, model)
TTL bucket + INTERVAL {minute_retention_days} DAY DELETE
SETTINGS allow_nullable_key = 1;


-- ---------------------------------------------------------------- COVERAGE

-- The tag cells (`analyzer_analysis::tags`, spec 2.8): per (tag, identity, minute), the stores
-- attributed to a declared interval, their prefill and refetch tokens, the indeterminate tokens
-- kept apart, and the stores at an interval's edge, attributed to nothing.
CREATE TABLE IF NOT EXISTS tag_minute
(
    fleet_id LowCardinality(String),
    semantics_version LowCardinality(String),
    discriminator LowCardinality(String),
    tag String,
    identity String,
    bucket DateTime,
    stores Int64,
    prefill_tokens Int64,
    refetch_tokens Int64,
    indeterminate_tokens Int64,
    ambiguous_stores Int64,
    version UInt64 DEFAULT 0,
    gone UInt8 DEFAULT 0
)
ENGINE = {engine_family}ReplacingMergeTree(version, gone)
PARTITION BY toYYYYMM(bucket)
ORDER BY (fleet_id, semantics_version, discriminator, tag, identity, bucket)
TTL toDate(bucket) + INTERVAL {rollup_retention_years} YEAR DELETE;

-- What the correction paths restated (`analyzer_analysis::restated`): per hour of the late
-- arrivals' own instants and per cell family (classes, placement, coverage), the arrivals
-- corrected, those of days no longer open to change, and the late scope closers, both counted
-- and left uncorrected.
CREATE TABLE IF NOT EXISTS restated_hourly
(
    fleet_id LowCardinality(String),
    semantics_version LowCardinality(String),
    discriminator LowCardinality(String),
    bucket DateTime,
    family LowCardinality(String),
    corrected Int64,
    too_late Int64,
    closers Int64,
    version UInt64 DEFAULT 0,
    gone UInt8 DEFAULT 0
)
ENGINE = {engine_family}ReplacingMergeTree(version, gone)
PARTITION BY toYYYYMM(bucket)
ORDER BY (fleet_id, semantics_version, discriminator, bucket, family)
TTL toDate(bucket) + INTERVAL {rollup_retention_years} YEAR DELETE;

-- The declared tag intervals as they stand: each declaration's latest statement. A standing
-- fact per declaration, never retired by day.
CREATE TABLE IF NOT EXISTS tag_intervals
(
    fleet_id LowCardinality(String),
    semantics_version LowCardinality(String),
    discriminator LowCardinality(String),
    declaration_id String,
    identity String,
    tag String,
    from_ms Float64,
    until_ms Nullable(Float64),
    stated_at_ms Float64,
    version UInt64 DEFAULT 0,
    gone UInt8 DEFAULT 0
)
ENGINE = {engine_family}ReplacingMergeTree(version, gone)
ORDER BY (fleet_id, semantics_version, discriminator, declaration_id);

-- The blocks by stored mass (`analyzer_analysis::block_mass`): each day's top blocks from a
-- Space-Saving summary, each with its upper bound, its error and whether it is certainly in the
-- top; the day row states the day's mass, the summary's capacity and the cut.
CREATE TABLE IF NOT EXISTS block_mass_daily
(
    fleet_id LowCardinality(String),
    semantics_version LowCardinality(String),
    discriminator LowCardinality(String),
    day Date,
    block_id String,
    tokens_upper Int64,
    tokens_error Int64,
    certain UInt32,
    holders_at_least UInt32,
    version UInt64 DEFAULT 0,
    gone UInt8 DEFAULT 0
)
ENGINE = {engine_family}ReplacingMergeTree(version, gone)
PARTITION BY toYYYYMM(day)
ORDER BY (fleet_id, semantics_version, discriminator, day, block_id)
TTL day + INTERVAL {rollup_retention_years} YEAR DELETE;

CREATE TABLE IF NOT EXISTS block_mass_day
(
    fleet_id LowCardinality(String),
    semantics_version LowCardinality(String),
    discriminator LowCardinality(String),
    day Date,
    tokens Int64,
    capacity UInt32,
    cut UInt32,
    version UInt64 DEFAULT 0,
    gone UInt8 DEFAULT 0
)
ENGINE = {engine_family}ReplacingMergeTree(version, gone)
ORDER BY (fleet_id, semantics_version, discriminator, day)
TTL day + INTERVAL {rollup_retention_years} YEAR DELETE;

-- Each source's coverage per minute, summed tick by tick: covered and stated milliseconds, so
-- any window, the last hour included, reads as a sum of its minutes.
CREATE TABLE IF NOT EXISTS coverage_minute
(
    fleet_id LowCardinality(String),
    semantics_version LowCardinality(String),
    discriminator LowCardinality(String),
    bucket DateTime,
    instance_id LowCardinality(String),
    covered_ms Float64,
    window_ms Float64,
    version UInt64 DEFAULT 0,
    gone UInt8 DEFAULT 0
)
ENGINE = {engine_family}ReplacingMergeTree(version, gone)
ORDER BY (fleet_id, semantics_version, discriminator, bucket, instance_id)
TTL bucket + INTERVAL {minute_retention_days} DAY DELETE;

CREATE TABLE IF NOT EXISTS coverage_daily
(
    fleet_id LowCardinality(String),
    semantics_version LowCardinality(String),
    discriminator LowCardinality(String),
    day Date,
    instance_id LowCardinality(String),
    covered_ms Float64,
    window_ms Float64,
    version UInt64 DEFAULT 0,
    gone UInt8 DEFAULT 0
)
ENGINE = {engine_family}ReplacingMergeTree(version, gone)
PARTITION BY toYYYYMM(day)
ORDER BY (fleet_id, semantics_version, discriminator, day, instance_id)
TTL day + INTERVAL {rollup_retention_years} YEAR DELETE;

-- ---------------------------------------------------------------- DUPLICATION — the producer's serving tables, minute/hour/day and the per-day relations

CREATE TABLE IF NOT EXISTS replication_minute
(
    fleet_id LowCardinality(String),
    semantics_version LowCardinality(String),
    discriminator LowCardinality(String),
    workload_class LowCardinality(String),
    copies_band LowCardinality(String),
    locality LowCardinality(String),
    minute_ms Int64,
    token_ms Float64,
    copies_token_ms Float64,
    -- The fold's write, whole: the latest version per key is the cell; gone = 1 is a tombstone.
    version UInt64 DEFAULT 0,
    gone UInt8 DEFAULT 0
)
ENGINE = {engine_family}ReplacingMergeTree(version, gone)
PARTITION BY toYYYYMM(toDateTime(intDiv(minute_ms, 1000), 'UTC'))
ORDER BY (fleet_id, semantics_version, discriminator, workload_class, copies_band, locality, minute_ms)
TTL toDateTime(intDiv(minute_ms, 1000), 'UTC') + INTERVAL {minute_retention_days} DAY DELETE;

CREATE TABLE IF NOT EXISTS replication_hourly
(
    fleet_id LowCardinality(String),
    semantics_version LowCardinality(String),
    discriminator LowCardinality(String),
    workload_class LowCardinality(String),
    copies_band LowCardinality(String),
    locality LowCardinality(String),
    bucket_ms Int64,
    token_ms Float64,
    copies_token_ms Float64,
    -- The fold's write, whole: the latest version per key is the cell; gone = 1 is a tombstone.
    version UInt64 DEFAULT 0,
    gone UInt8 DEFAULT 0
)
ENGINE = {engine_family}ReplacingMergeTree(version, gone)
PARTITION BY toYYYYMM(toDateTime(intDiv(bucket_ms, 1000), 'UTC'))
ORDER BY (fleet_id, semantics_version, discriminator, workload_class, copies_band, locality, bucket_ms)
TTL toDateTime(intDiv(bucket_ms, 1000), 'UTC') + INTERVAL {hour_retention_days} DAY DELETE;

CREATE TABLE IF NOT EXISTS replication_daily
(
    fleet_id LowCardinality(String),
    semantics_version LowCardinality(String),
    discriminator LowCardinality(String),
    workload_class LowCardinality(String),
    copies_band LowCardinality(String),
    locality LowCardinality(String),
    bucket_ms Int64,
    token_ms Float64,
    copies_token_ms Float64,
    -- The fold's write, whole: the latest version per key is the cell; gone = 1 is a tombstone.
    version UInt64 DEFAULT 0,
    gone UInt8 DEFAULT 0
)
ENGINE = {engine_family}ReplacingMergeTree(version, gone)
PARTITION BY toYYYYMM(toDateTime(intDiv(bucket_ms, 1000), 'UTC'))
ORDER BY (fleet_id, semantics_version, discriminator, workload_class, copies_band, locality, bucket_ms)
TTL toDate(toDateTime(intDiv(bucket_ms, 1000), 'UTC')) + INTERVAL {rollup_retention_years} YEAR DELETE;

CREATE TABLE IF NOT EXISTS replication_instance_hourly
(
    fleet_id LowCardinality(String),
    semantics_version LowCardinality(String),
    discriminator LowCardinality(String),
    instance_id LowCardinality(String),
    copies_band LowCardinality(String),
    locality LowCardinality(String),
    bucket DateTime,
    token_ms Float64,
    copies_token_ms Float64,
    -- The fold's write, whole: the latest version per key is the cell; gone = 1 is a tombstone.
    version UInt64 DEFAULT 0,
    gone UInt8 DEFAULT 0
)
ENGINE = {engine_family}ReplacingMergeTree(version, gone)
PARTITION BY toYYYYMM(bucket)
ORDER BY (fleet_id, semantics_version, discriminator, instance_id, bucket, copies_band, locality)
TTL bucket + INTERVAL {instance_detail_retention_days} DAY DELETE;

CREATE TABLE IF NOT EXISTS replication_instance_minute
(
    fleet_id LowCardinality(String),
    semantics_version LowCardinality(String),
    discriminator LowCardinality(String),
    instance_id LowCardinality(String),
    copies_band LowCardinality(String),
    locality LowCardinality(String),
    bucket DateTime,
    token_ms Float64,
    copies_token_ms Float64,
    -- The fold's write, whole: the latest version per key is the cell; gone = 1 is a tombstone.
    version UInt64 DEFAULT 0,
    gone UInt8 DEFAULT 0
)
ENGINE = {engine_family}ReplacingMergeTree(version, gone)
PARTITION BY toYYYYMMDD(bucket)
ORDER BY (fleet_id, semantics_version, discriminator, instance_id, bucket, copies_band, locality)
TTL bucket + INTERVAL {minute_retention_days} DAY DELETE;

-- The whale register: each settled day's identities by duplicated token-time, cut at the
-- analyzer's WHALE_CUT, and the day's totals over every identity. Identity grain, so on the
-- ledger horizon.
CREATE TABLE IF NOT EXISTS replication_whale_day
(
    fleet_id LowCardinality(String),
    semantics_version LowCardinality(String),
    discriminator LowCardinality(String),
    day Date,
    identity String,
    n_tokens Int64,
    dup_token_ms Float64,
    held_token_ms Float64,
    version UInt64 DEFAULT 0,
    gone UInt8 DEFAULT 0
)
ENGINE = {engine_family}ReplacingMergeTree(version, gone)
PARTITION BY toYYYYMM(day)
ORDER BY (fleet_id, semantics_version, discriminator, day, identity)
TTL day + INTERVAL {ledger_retention_days} DAY DELETE;

CREATE TABLE IF NOT EXISTS replication_whale_day_total
(
    fleet_id LowCardinality(String),
    semantics_version LowCardinality(String),
    discriminator LowCardinality(String),
    day Date,
    held_identities Int64,
    duplicated_identities Int64,
    dup_token_ms Float64,
    held_token_ms Float64,
    cut UInt32,
    floor_dup_token_ms Float64,
    version UInt64 DEFAULT 0,
    gone UInt8 DEFAULT 0
)
ENGINE = {engine_family}ReplacingMergeTree(version, gone)
PARTITION BY toYYYYMM(day)
ORDER BY (fleet_id, semantics_version, discriminator, day)
TTL day + INTERVAL {ledger_retention_days} DAY DELETE;

CREATE TABLE IF NOT EXISTS replication_pairs_day
(
    fleet_id LowCardinality(String),
    semantics_version LowCardinality(String),
    discriminator LowCardinality(String),
    workload_class LowCardinality(String),
    day Int64,
    a_instance String,
    a_dp_rank UInt32,
    a_group_idx UInt32,
    b_instance String,
    b_dp_rank UInt32,
    b_group_idx UInt32,
    token_ms Float64,
    -- The fold's write, whole: the latest version per key is the cell; gone = 1 is a tombstone.
    version UInt64 DEFAULT 0,
    gone UInt8 DEFAULT 0
)
ENGINE = {engine_family}ReplacingMergeTree(version, gone)
PARTITION BY toYYYYMM(toDateTime(day * 86400, 'UTC'))
ORDER BY (fleet_id, semantics_version, discriminator, workload_class, day, a_instance, a_dp_rank, a_group_idx, b_instance, b_dp_rank, b_group_idx)
TTL toDate(toDateTime(day * 86400, 'UTC')) + INTERVAL {rollup_retention_years} YEAR DELETE;

CREATE TABLE IF NOT EXISTS replication_instance_spans_day
(
    fleet_id LowCardinality(String),
    semantics_version LowCardinality(String),
    discriminator LowCardinality(String),
    day Int64,
    instance_id LowCardinality(String),
    start_ms Float64,
    end_ms Float64,
    version UInt64 DEFAULT 0,
    gone UInt8 DEFAULT 0
)
ENGINE = {engine_family}ReplacingMergeTree(version, gone)
PARTITION BY toYYYYMM(toDateTime(day * 86400, 'UTC'))
ORDER BY (fleet_id, semantics_version, discriminator, day, instance_id, start_ms)
TTL toDate(toDateTime(day * 86400, 'UTC')) + INTERVAL {rollup_retention_years} YEAR DELETE;


-- ---------------------------------------------------------------- THE DERIVATION'S STANDING FACTS
-- The derivation's standing facts as readers see them (pipeline::publish::DERIVATION_STATUS):
-- the last tick and each edge it stated. One row per fact under the meaning head; the latest
-- version per fact is the fact.
CREATE TABLE IF NOT EXISTS derivation_status
(
    fleet_id LowCardinality(String),
    semantics_version LowCardinality(String),
    discriminator LowCardinality(String),
    fact LowCardinality(String),
    value Float64,
    version UInt64 DEFAULT 0,
    gone UInt8 DEFAULT 0
)
ENGINE = {engine_family}ReplacingMergeTree(version, gone)
ORDER BY (fleet_id, semantics_version, discriminator, fact);

-- ---------------------------------------------------------------- THE DERIVATION'S OWN STATE
-- The keyed operators' state is the lake's working state (`docs/pipeline.md`):
-- frontiers, seeds, held cells, the outbox and the head. Serving holds none of it.

-- The serving database's binding: which execution namespace it serves, so two namespaces
-- never share one (`Serving::bind`).
CREATE TABLE IF NOT EXISTS execution_namespace
(
    slot UInt8 DEFAULT 0,
    identity String
)
ENGINE = {engine_family}ReplacingMergeTree
ORDER BY slot;
