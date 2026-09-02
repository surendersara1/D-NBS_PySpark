/* ===========================================================================
   S3 TABLES × ATHENA — THE WHOLE FEATURE SET IN ONE FILE
   ---------------------------------------------------------------------------
   Run these blocks IN ORDER in the Athena query editor (engine v3).
   Every statement here is valid ATHENA SQL. Where a feature is Spark-only,
   it is marked  [NOT IN ATHENA]  with the Spark equivalent beside it.

   BEFORE YOU START — find & replace these TWO strings everywhere (Ctrl+H):

       s3tablescatalog/nbs-lab-bucket   ->  your catalog, exactly as it
                                            appears in the Catalogue dropdown
                                            e.g. s3tablescatalog/amp-shahzeb-s3tables
       "ecom"                           ->  your namespace, e.g. "join_demo"

   The catalog name for an S3 table bucket is literally
       s3tablescatalog/<table-bucket-name>
   It contains a slash, so it MUST be double-quoted every time.

   SHORTCUT: if you have already picked the Catalogue and Database in the
   left-hand panel, you can drop the first two name parts and just write
       orders            instead of  "s3tablescatalog/...". "ecom"."orders"
   Fully-qualified names are used below so the file works from any tab.

   SETUP (once, outside Athena):
     aws s3tables create-table-bucket --name nbs-lab-bucket --region us-east-1
     then in Lake Formation, grant your Athena principal access to the
     s3tablescatalog/nbs-lab-bucket catalog.

   ---------------------------------------------------------------------------
   SYNTAX FIXES APPLIED vs. the source notes — the five that actually bite:
     1. `USING iceberg`      -> Spark only. Athena infers Iceberg for S3 Tables.
     2. `backticks`          -> Spark/Hive. Athena quotes identifiers with ".
     3. CREATE DATABASE      -> this parser wants CREATE SCHEMA.
     4. CALL catalog.system.*    -> Spark only. Athena = OPTIMIZE / VACUUM.
     5. ALTER TABLE ADD PARTITION FIELD -> Spark only. See block 09.
   =========================================================================== */


/* ###########################################################################
   00 · NAMESPACE
   ---------------------------------------------------------------------------
   Use SCHEMA, not DATABASE. The SageMaker Unified Studio / Athena v3 parser
   rejects CREATE DATABASE with:
       mismatched input 'DATABASE'. Expecting: ... 'SCHEMA', 'TABLE', 'VIEW'
   CREATE SCHEMA is accepted by both engines, so it is the portable choice.

   ALREADY HAVE A NAMESPACE?  Skip this statement entirely and just set
   :DB below to it (e.g. join_demo, which already exists in your account).
   ########################################################################### */

CREATE SCHEMA IF NOT EXISTS "s3tablescatalog/nbs-lab-bucket"."ecom";


/* ###########################################################################
   01 · STANDARD TABLE — hidden partitioning + bucketing
   ---------------------------------------------------------------------------
   day(order_timestamp)  = a hidden transform. Users filter order_timestamp
                           directly and still get pruning. No derived column.
   bucket(128, user_id)  = the high-cardinality escape hatch. 128 buckets
                           forever, instead of millions of directories.
   ########################################################################### */

CREATE TABLE IF NOT EXISTS "s3tablescatalog/nbs-lab-bucket"."ecom"."orders" (
    order_id        STRING,
    user_id         STRING,
    order_timestamp TIMESTAMP,
    total_amount    DOUBLE,
    country         STRING,
    status          STRING
)
PARTITIONED BY (day(order_timestamp), bucket(128, user_id))
TBLPROPERTIES (
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes'    = '134217728'   -- 128 MB
);

/* NOTE: no LOCATION and no 'table_type'='ICEBERG' here.
   S3 Tables are managed and are Iceberg by definition. Adding either is
   what produces "Table type is not supported" errors.                     */


/* ###########################################################################
   02 · INSERT DATA
   ########################################################################### */

INSERT INTO "s3tablescatalog/nbs-lab-bucket"."ecom"."orders"
VALUES
  ('ORD-1001','U-01',TIMESTAMP '2026-03-01 09:15:00', 120.50,'US','completed'),
  ('ORD-1002','U-02',TIMESTAMP '2026-03-01 11:40:00', 640.00,'DE','completed'),
  ('ORD-1003','U-01',TIMESTAMP '2026-03-02 08:05:00',  45.25,'US','pending'),
  ('ORD-1004','U-03',TIMESTAMP '2026-03-02 14:22:00', 980.75,'SG','completed'),
  ('ORD-1005','U-02',TIMESTAMP '2026-03-03 10:00:00', 210.00,'DE','refunded'),
  ('ORD-1006','U-04',TIMESTAMP '2026-03-03 16:45:00',1500.00,'US','completed'),
  ('ORD-1007','U-05',TIMESTAMP '2026-03-04 07:30:00',  75.00,'FR','completed'),
  ('ORD-1008','U-03',TIMESTAMP '2026-03-04 19:10:00', 320.40,'SG','completed');

SELECT count(*) AS row_count
FROM "s3tablescatalog/nbs-lab-bucket"."ecom"."orders";


/* ###########################################################################
   03 · PROVE THE HIDDEN PARTITIONING WORKS
   ---------------------------------------------------------------------------
   Filter the RAW timestamp column. No day= predicate anywhere. Compare the
   "Data scanned" figure in the Athena results pane between these two.
   ########################################################################### */

-- pruned: only one day's files are opened
SELECT count(*) AS one_day
FROM "s3tablescatalog/nbs-lab-bucket"."ecom"."orders"
WHERE order_timestamp >= TIMESTAMP '2026-03-02 00:00:00'
  AND order_timestamp <  TIMESTAMP '2026-03-03 00:00:00';

-- full scan, for contrast
SELECT count(*) AS all_days
FROM "s3tablescatalog/nbs-lab-bucket"."ecom"."orders";


/* ###########################################################################
   04 · ACID DML — the thing a plain Parquet lake simply cannot do
   ########################################################################### */

-- UPDATE
UPDATE "s3tablescatalog/nbs-lab-bucket"."ecom"."orders"
SET status = 'refunded'
WHERE order_id = 'ORD-1003';

-- DELETE
DELETE FROM "s3tablescatalog/nbs-lab-bucket"."ecom"."orders"
WHERE total_amount < 50.00;

-- MERGE (upsert). Athena supports the full MERGE grammar on Iceberg.
MERGE INTO "s3tablescatalog/nbs-lab-bucket"."ecom"."orders" AS t
USING (
    SELECT * FROM (VALUES
        ('ORD-1002','U-02',TIMESTAMP '2026-03-01 11:40:00', 700.00,'DE','completed'),
        ('ORD-9001','U-09',TIMESTAMP '2026-03-05 12:00:00', 250.00,'JP','completed')
    ) AS s(order_id,user_id,order_timestamp,total_amount,country,status)
) AS s
ON t.order_id = s.order_id
WHEN MATCHED THEN
    UPDATE SET total_amount = s.total_amount, status = s.status
WHEN NOT MATCHED THEN
    INSERT (order_id,user_id,order_timestamp,total_amount,country,status)
    VALUES (s.order_id,s.user_id,s.order_timestamp,s.total_amount,s.country,s.status);

SELECT order_id, total_amount, status
FROM "s3tablescatalog/nbs-lab-bucket"."ecom"."orders"
ORDER BY order_id;


/* ###########################################################################
   05 · SYSTEM METADATA TABLES
   ---------------------------------------------------------------------------
   The $-suffixed name MUST sit inside the quotes with the table name:
       "ecom"."orders$snapshots"     correct
       "ecom"."orders"$snapshots     syntax error
   ########################################################################### */

-- every commit you just made, one row each
SELECT committed_at, snapshot_id, parent_id, operation, summary
FROM "s3tablescatalog/nbs-lab-bucket"."ecom"."orders$snapshots"
ORDER BY committed_at;

-- current data files: sizes, row counts, and the min/max bounds that prune
SELECT file_path, record_count, file_size_in_bytes, partition
FROM "s3tablescatalog/nbs-lab-bucket"."ecom"."orders$files";

-- the compaction diagnostic: many files with a small average = a target
SELECT partition,
       count(*)                     AS files,
       sum(record_count)            AS rows,
       cast(avg(file_size_in_bytes) AS BIGINT) AS avg_bytes
FROM "s3tablescatalog/nbs-lab-bucket"."ecom"."orders$files"
GROUP BY partition
ORDER BY files DESC;

-- when each snapshot became current, and its lineage
SELECT * FROM "s3tablescatalog/nbs-lab-bucket"."ecom"."orders$history"
ORDER BY made_current_at;

-- the manifests that prune the files above
SELECT * FROM "s3tablescatalog/nbs-lab-bucket"."ecom"."orders$manifests";

-- per-partition summary, including delete-file debt
SELECT * FROM "s3tablescatalog/nbs-lab-bucket"."ecom"."orders$partitions";

-- branches and tags
SELECT * FROM "s3tablescatalog/nbs-lab-bucket"."ecom"."orders$refs";


/* ###########################################################################
   06 · TIME TRAVEL & ROLLBACK
   ---------------------------------------------------------------------------
   Grab a snapshot_id from block 05 and paste it below.
   ########################################################################### */

-- by timestamp (relative form always works, no copy-paste needed)
SELECT count(*) AS rows_10_min_ago
FROM "s3tablescatalog/nbs-lab-bucket"."ecom"."orders"
FOR TIMESTAMP AS OF (current_timestamp - INTERVAL '10' MINUTE);

-- by snapshot id  <-- REPLACE 1234567890123456789
SELECT count(*) AS rows_at_that_snapshot
FROM "s3tablescatalog/nbs-lab-bucket"."ecom"."orders"
FOR VERSION AS OF 1234567890123456789;

-- see the row you UPDATEd as it was before, without restoring anything
SELECT order_id, status
FROM "s3tablescatalog/nbs-lab-bucket"."ecom"."orders"
FOR TIMESTAMP AS OF (current_timestamp - INTERVAL '10' MINUTE)
WHERE order_id = 'ORD-1003';

/* [NOT IN ATHENA] rollback. Athena reads history but cannot move the pointer.
   Spark:  CALL cat.system.rollback_to_snapshot('ecom.orders', 1234...);
   Athena workaround — recreate current state from an old snapshot:
       INSERT INTO ... SELECT * FROM t FOR VERSION AS OF <id>;               */


/* ###########################################################################
   07 · SCHEMA EVOLUTION — metadata only, no rewrite
   ########################################################################### */

ALTER TABLE "s3tablescatalog/nbs-lab-bucket"."ecom"."orders"
    ADD COLUMNS (channel STRING, discount_pct DOUBLE);

-- existing rows read NULL for the new column. That is correct, not an error.
SELECT order_id, channel
FROM "s3tablescatalog/nbs-lab-bucket"."ecom"."orders"
LIMIT 5;

-- type widening is safe; narrowing is refused
ALTER TABLE "s3tablescatalog/nbs-lab-bucket"."ecom"."orders"
    CHANGE COLUMN total_amount total_amount DOUBLE;

ALTER TABLE "s3tablescatalog/nbs-lab-bucket"."ecom"."orders"
    DROP COLUMN discount_pct;

/* Column IDs are why the above is free: a rename keeps the id, so every
   existing Parquet file still resolves. Hive resolved by name/position and
   silently returned the wrong column.
   [NOT IN ATHENA] RENAME COLUMN — use Spark:
       ALTER TABLE cat.ecom.orders RENAME COLUMN country TO country_code;   */


/* ###########################################################################
   08 · TABLE MAINTENANCE — Athena's two verbs
   ---------------------------------------------------------------------------
   S3 Tables run continuous background compaction and snapshot expiry for you,
   so these are usually unnecessary. They are here so you can see them work.
   ########################################################################### */

-- compaction: merge small Parquet files toward the target size
OPTIMIZE "s3tablescatalog/nbs-lab-bucket"."ecom"."orders" REWRITE DATA USING BIN_PACK;

-- re-run block 05's file-count query to see the difference
SELECT count(*) AS files_after_optimize
FROM "s3tablescatalog/nbs-lab-bucket"."ecom"."orders$files";

-- expire old snapshots and delete the files only they referenced.
-- This is what frees storage AND what ends time travel — same operation.
VACUUM "s3tablescatalog/nbs-lab-bucket"."ecom"."orders";

/* Control retention before VACUUMing: */
ALTER TABLE "s3tablescatalog/nbs-lab-bucket"."ecom"."orders"
    SET TBLPROPERTIES (
        'vacuum_max_snapshot_age_seconds' = '432000',   -- 5 days
        'vacuum_min_snapshots_to_keep'    = '10'
    );

/* [NOT IN ATHENA] the Spark procedures the notes listed:
       CALL cat.system.rewrite_data_files(...)   -> Athena: OPTIMIZE
       CALL cat.system.expire_snapshots(...)     -> Athena: VACUUM
       CALL cat.system.remove_orphan_files(...)  -> S3 Tables does this itself
       CALL cat.system.rewrite_manifests(...)    -> no Athena equivalent      */


/* ###########################################################################
   09 · PARTITION EVOLUTION            [NOT IN ATHENA — Spark/EMR only]
   ---------------------------------------------------------------------------
   Athena can CREATE a partitioned table but cannot ALTER the partition spec.
   Run this from EMR Spark or Glue, then query the result from Athena:

       ALTER TABLE cat.ecom.orders ADD PARTITION FIELD hours(order_timestamp);
       ALTER TABLE cat.ecom.orders DROP PARTITION FIELD bucket(128, user_id);

   Old data stays under the old spec and is never rewritten; new writes use
   the new one. The engine plans each spec separately and unions them.
   Verify from Athena afterwards — two spec_ids will appear:
   ########################################################################### */

SELECT spec_id, count(*) AS partitions_in_spec
FROM "s3tablescatalog/nbs-lab-bucket"."ecom"."orders$partitions"
GROUP BY spec_id;


/* ###########################################################################
   10 · CTAS — transform and land in one atomic statement
   ---------------------------------------------------------------------------
   Athena CTAS uses WITH (...), not PARTITIONED BY.
   ########################################################################### */

CREATE TABLE "s3tablescatalog/nbs-lab-bucket"."ecom"."high_value_orders"
WITH (
    partitioning = ARRAY['day(order_timestamp)', 'bucket(64, user_id)'],
    write_compression = 'ZSTD'
) AS
SELECT order_id, user_id, order_timestamp, total_amount, country
FROM "s3tablescatalog/nbs-lab-bucket"."ecom"."orders"
WHERE total_amount > 500.00;

SELECT * FROM "s3tablescatalog/nbs-lab-bucket"."ecom"."high_value_orders";


/* ###########################################################################
   11 · VIEWS
   ########################################################################### */

CREATE OR REPLACE VIEW "s3tablescatalog/nbs-lab-bucket"."ecom"."v_daily_revenue" AS
SELECT date_trunc('day', order_timestamp) AS order_date,
       count(DISTINCT user_id)            AS active_users,
       count(*)                           AS orders,
       sum(total_amount)                  AS daily_revenue
FROM "s3tablescatalog/nbs-lab-bucket"."ecom"."orders"
GROUP BY 1;

SELECT * FROM "s3tablescatalog/nbs-lab-bucket"."ecom"."v_daily_revenue"
ORDER BY order_date;

/* MATERIALIZED VIEWS — your engine DOES support these.
   Proof: the CREATE SCHEMA error in block 00 listed the keywords the parser
   accepts after CREATE, and 'MATERIALIZED' was one of them. Classic Athena
   has no MVs; SageMaker Unified Studio / Athena v3 does.
   An MV is a REAL Iceberg table with precomputed rows, not a saved query.   */

CREATE MATERIALIZED VIEW "s3tablescatalog/nbs-lab-bucket"."ecom"."mv_hourly_metrics" AS
SELECT date_trunc('hour', order_timestamp) AS order_hour,
       country,
       count(order_id)   AS total_orders,
       sum(total_amount) AS revenue
FROM "s3tablescatalog/nbs-lab-bucket"."ecom"."orders"
GROUP BY 1, 2;

SELECT * FROM "s3tablescatalog/nbs-lab-bucket"."ecom"."mv_hourly_metrics"
ORDER BY order_hour;

REFRESH MATERIALIZED VIEW "s3tablescatalog/nbs-lab-bucket"."ecom"."mv_hourly_metrics";

/* If CREATE MATERIALIZED VIEW is rejected in your workgroup, fall back to a
   plain table plus a scoped refresh — same result, you own the schedule:

CREATE TABLE "s3tablescatalog/nbs-lab-bucket"."ecom"."mv_hourly_fallback"
WITH (partitioning = ARRAY['day(order_hour)']) AS
SELECT date_trunc('hour', order_timestamp) AS order_hour, country,
       count(order_id) AS total_orders, sum(total_amount) AS revenue
FROM "s3tablescatalog/nbs-lab-bucket"."ecom"."orders"
GROUP BY 1, 2;

-- refresh = delete the affected window, re-insert it. Idempotent.
DELETE FROM "s3tablescatalog/nbs-lab-bucket"."ecom"."mv_hourly_fallback"
WHERE order_hour >= TIMESTAMP '2026-03-04 00:00:00';

INSERT INTO "s3tablescatalog/nbs-lab-bucket"."ecom"."mv_hourly_fallback"
SELECT date_trunc('hour', order_timestamp) AS order_hour, country,
       count(order_id) AS total_orders, sum(total_amount) AS revenue
FROM "s3tablescatalog/nbs-lab-bucket"."ecom"."orders"
WHERE order_timestamp >= TIMESTAMP '2026-03-04 00:00:00'
GROUP BY 1, 2;                                                             */


/* ###########################################################################
   12 · JOINS — including the cross-catalog join, which is the real point
   ########################################################################### */

-- ordinary join between two S3 Tables
SELECT o.country,
       count(*)          AS orders,
       sum(h.total_amount) AS high_value_revenue
FROM      "s3tablescatalog/nbs-lab-bucket"."ecom"."orders"             o
LEFT JOIN "s3tablescatalog/nbs-lab-bucket"."ecom"."high_value_orders"  h
       ON o.order_id = h.order_id
GROUP BY o.country
ORDER BY orders DESC;

-- LEFT ANTI: orders that never made the high-value table
SELECT o.order_id, o.total_amount
FROM "s3tablescatalog/nbs-lab-bucket"."ecom"."orders" o
WHERE NOT EXISTS (
    SELECT 1 FROM "s3tablescatalog/nbs-lab-bucket"."ecom"."high_value_orders" h
    WHERE h.order_id = o.order_id);

/* CROSS-CATALOG JOIN — an S3 Table joined to a plain Glue/S3 Parquet table.
   No copying, no federation setup. Uncomment and point at a real table:

SELECT o.order_id, o.total_amount, d.region_name
FROM "s3tablescatalog/nbs-lab-bucket"."ecom"."orders"  o
JOIN "awsdatacatalog"."legacy_db"."country_dim"        d
  ON o.country = d.country_code;                                          */


/* ###########################################################################
   13 · SEMI-STRUCTURED / VARIANT      [Iceberg V3 — check region support]
   ---------------------------------------------------------------------------
   VARIANT auto-shreds JSON into typed Parquet columns. Athena's read support
   is arriving progressively; if CREATE fails, create it from Spark and query
   it here. The portable alternative that works TODAY in Athena is a STRING
   column plus json_extract_scalar, shown second.
   ########################################################################### */

-- V3 variant (Spark-created if Athena rejects it)
CREATE TABLE IF NOT EXISTS "s3tablescatalog/nbs-lab-bucket"."ecom"."app_events" (
    event_id      STRING,
    user_id       STRING,
    event_time    TIMESTAMP,
    event_payload STRING          -- swap to VARIANT once enabled in your region
)
PARTITIONED BY (hour(event_time), bucket(64, user_id))
TBLPROPERTIES ('format-version' = '3');

INSERT INTO "s3tablescatalog/nbs-lab-bucket"."ecom"."app_events"
VALUES ('E-1','U-01',TIMESTAMP '2026-03-01 09:16:00',
        '{"page":"checkout","ms":420,"ab":"B"}'),
       ('E-2','U-02',TIMESTAMP '2026-03-01 11:41:00',
        '{"page":"cart","ms":180,"ab":"A"}');

-- works today, no V3 needed
SELECT event_id,
       json_extract_scalar(event_payload, '$.page')          AS page,
       cast(json_extract_scalar(event_payload,'$.ms') AS INT) AS latency_ms
FROM "s3tablescatalog/nbs-lab-bucket"."ecom"."app_events";


/* ###########################################################################
   14 · WHAT S3 TABLES DOES **NOT** DO — the point the notes made well
   ---------------------------------------------------------------------------
   Hash / range / round-robin / coalesce / broadcast are COMPUTE-ENGINE
   shuffle mechanics. They happen inside Spark or Athena's own engine, in
   worker memory, before any Parquet is written.

   S3 Tables only owns two things:
     1. METADATA DEFINITIONS  — partition transforms and bucket specs
                                (blocks 01 and 09 above)
     2. PHYSICAL MAINTENANCE  — background compaction, snapshot expiry,
                                orphan cleanup (block 08 happens for free)

   You cannot "set broadcast join" on an S3 Table. You set it on the engine.
   In Athena the optimizer decides; in Spark you write broadcast(df).
   ########################################################################### */


/* ###########################################################################
   99 · TEARDOWN
   ########################################################################### */

-- DROP TABLE "s3tablescatalog/nbs-lab-bucket"."ecom"."app_events";
-- DROP MATERIALIZED VIEW "s3tablescatalog/nbs-lab-bucket"."ecom"."mv_hourly_metrics";
-- DROP TABLE "s3tablescatalog/nbs-lab-bucket"."ecom"."high_value_orders";
-- DROP VIEW  "s3tablescatalog/nbs-lab-bucket"."ecom"."v_daily_revenue";
-- DROP TABLE "s3tablescatalog/nbs-lab-bucket"."ecom"."orders";
-- DROP SCHEMA "s3tablescatalog/nbs-lab-bucket"."ecom";
