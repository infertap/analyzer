-- SPDX-License-Identifier: AGPL-3.0-only
--
-- The engine behaviours the product relies on, asserted against the analyzer's own tables
-- (scripts/parity-scan.sh loads crates/serving-clickhouse/migrations/0001_schema.sql first).
-- Assertions are throwIf: a failing behaviour is a failing scan, not a warning. Rows use
-- today()-relative days, so a TTL cannot race an assertion.

-- 1. Every cell is replaced by version: the later version is the cell, whatever the insert
--    order, and a tombstone (gone = 1) hides the key under FINAL.
INSERT INTO class_rollup_daily (fleet_id, discriminator, class, workload_class, instance_id,
    dp_rank, group_idx, semantics_version, month, bucket, events, tokens, version, gone)
VALUES ('p1', 'd', 'ETR', 'agentic', 'i1', 0, 0, 'js', toStartOfMonth(today()), today(), 9, 90, 7, 0);
INSERT INTO class_rollup_daily (fleet_id, discriminator, class, workload_class, instance_id,
    dp_rank, group_idx, semantics_version, month, bucket, events, tokens, version, gone)
VALUES ('p1', 'd', 'ETR', 'agentic', 'i1', 0, 0, 'js', toStartOfMonth(today()), today(), 3, 30, 2, 0);
SELECT throwIf(count() != 1 OR sum(events) != 9, 'a later version did not win under FINAL')
FROM class_rollup_daily FINAL WHERE fleet_id = 'p1' AND gone = 0;
INSERT INTO class_rollup_daily (fleet_id, discriminator, class, workload_class, instance_id,
    dp_rank, group_idx, semantics_version, month, bucket, events, tokens, version, gone)
VALUES ('p1', 'd', 'ETR', 'agentic', 'i1', 0, 0, 'js', toStartOfMonth(today()), today(), 0, 0, 8, 1);
SELECT throwIf(count() != 0, 'a tombstone did not hide its key under FINAL')
FROM class_rollup_daily FINAL WHERE fleet_id = 'p1' AND gone = 0;

-- 2. The same law survives a merge: OPTIMIZE ... FINAL leaves the latest version, so a read
--    without FINAL after a merge agrees with one with it.
INSERT INTO band_rollup_daily (fleet_id, discriminator, class, band, workload_class,
    instance_id, dp_rank, group_idx, semantics_version, month, bucket, events, tokens, version, gone)
VALUES ('p2', 'd', 'ETR', '<1m', 'agentic', 'i1', 0, 0, 'js', toStartOfMonth(today()), today(), 1, 10, 1, 0);
INSERT INTO band_rollup_daily (fleet_id, discriminator, class, band, workload_class,
    instance_id, dp_rank, group_idx, semantics_version, month, bucket, events, tokens, version, gone)
VALUES ('p2', 'd', 'ETR', '<1m', 'agentic', 'i1', 0, 0, 'js', toStartOfMonth(today()), today(), 4, 40, 2, 0);
OPTIMIZE TABLE band_rollup_daily FINAL;
SELECT throwIf(count() != 1 OR sum(tokens) != 40, 'a merge did not keep the latest version')
FROM band_rollup_daily WHERE fleet_id = 'p2';

-- 3. A nullable column in a sort key is accepted (allow_nullable_key) and keys on NULL: the
--    hourly rollup keys on a nullable model.
INSERT INTO class_rollup_hourly (fleet_id, bucket, discriminator, class, workload_class,
    model, instance_id, dp_rank, group_idx, semantics_version, events, tokens, version, gone)
VALUES ('p3', toStartOfHour(now()), 'd', 'ETR', 'agentic', NULL, 'i1', 0, 0, 'js', 1, 4, 1, 0),
       ('p3', toStartOfHour(now()), 'd', 'ETR', 'agentic', 'llama', 'i1', 0, 0, 'js', 1, 4, 1, 0);
SELECT throwIf(count() != 2, 'a NULL key and a value key did not stay two cells')
FROM class_rollup_hourly FINAL WHERE fleet_id = 'p3';

-- 4. Every table ages by a TTL the engine accepted, save the three that are standing state: the
--    derivation's standing facts, the namespace binding, and the declared tag intervals (a
--    declaration is never retired by day).
SELECT throwIf(groupArray(name) != ['derivation_status', 'execution_namespace', 'tag_intervals'],
    'the tables without a TTL are not exactly the standing ones')
FROM (SELECT name FROM system.tables
      WHERE database = currentDatabase() AND engine LIKE '%MergeTree%'
        AND create_table_query NOT LIKE '%TTL%' ORDER BY name);

SELECT 'parity-scan: the schema and its behaviours pass on ' || version();
