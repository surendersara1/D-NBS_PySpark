/* ===========================================================================
   S3 TABLES × ATHENA — verified against the AWS docs, not guessed
   Source: docs.aws.amazon.com/athena/latest/ug/gdc-register-s3-table-bucket-cat.html
   ---------------------------------------------------------------------------

   THE ONE RULE THAT FIXES EVERYTHING
   ----------------------------------
   Set the dropdowns on the left, then write UNQUALIFIED names.

       Data source  =  AwsDataCatalog
       Catalogue    =  s3tablescatalog/amp-shahzeb-s3tables...
       Database     =  join_demo

   Then it is just  `orders_lab`.  No catalog prefix, no quoting, nothing to
   find-and-replace. This is exactly what the AWS docs tell you to do, and it
   is why the earlier three-part quoted names blew up.

   If you ever DO need the fully-qualified form, only the catalog is quoted
   (it contains a slash), and the quotes are DOUBLE:

       "s3tablescatalog/amp-shahzeb-s3tables".join_demo.orders_lab

   ---------------------------------------------------------------------------
   FOUR THINGS THE AWS DOCS SAY THAT PEOPLE GET WRONG
     1. TBLPROPERTIES ('table_type' = 'iceberg')  IS REQUIRED on CREATE TABLE.
     2. Do NOT specify LOCATION. S3 Tables are managed.
     3. CREATE VIEW is NOT SUPPORTED on S3 Tables. Neither is ALTER TABLE
        RENAME or ALTER DATABASE. (Blocks 10 and 12 below.)
     4. OPTIMIZE / VACUUM: S3 Tables does compaction and snapshot expiry for
        you in the background. You normally never run these.

   Run ONE block at a time: highlight it, hit Run.
   =========================================================================== */


-- ###########################################################################
-- 00 · NAMESPACE  (skip this - you already have join_demo)
-- ###########################################################################
-- Unqualified, with the Catalogue dropdown set. This is the doc's exact form.

CREATE DATABASE IF NOT EXISTS ecom_lab;

-- Then switch the Database dropdown to ecom_lab, or stay on join_demo.
-- Everything below is named *_lab so your existing tables are untouched.


-- ###########################################################################
-- 01 · CREATE — hidden partitioning + bucketing
-- ###########################################################################
-- day(order_timestamp) : hidden transform. Filter the raw timestamp and you
--                        still get pruning. No derived column to remember.
-- bucket(128, user_id) : the high-cardinality escape hatch. 128 buckets
--                        forever, instead of millions of directories.

CREATE TABLE orders_lab (
    order_id        string,
    user_id         string,
    order_timestamp timestamp,
    total_amount    double,
    country         string,
    status          string
)
PARTITIONED BY (day(order_timestamp), bucket(128, user_id))
TBLPROPERTIES ('table_type' = 'iceberg');


-- ###########################################################################
-- 02 · INSERT
-- ###########################################################################

INSERT INTO orders_lab VALUES
  ('ORD-1001','U-01',TIMESTAMP '2026-03-01 09:15:00', 120.50,'US','completed'),
  ('ORD-1002','U-02',TIMESTAMP '2026-03-01 11:40:00', 640.00,'DE','completed'),
  ('ORD-1003','U-01',TIMESTAMP '2026-03-02 08:05:00',  45.25,'US','pending'),
  ('ORD-1004','U-03',TIMESTAMP '2026-03-02 14:22:00', 980.75,'SG','completed'),
  ('ORD-1005','U-02',TIMESTAMP '2026-03-03 10:00:00', 210.00,'DE','refunded'),
  ('ORD-1006','U-04',TIMESTAMP '2026-03-03 16:45:00',1500.00,'US','completed'),
  ('ORD-1007','U-05',TIMESTAMP '2026-03-04 07:30:00',  75.00,'FR','completed'),
  ('ORD-1008','U-03',TIMESTAMP '2026-03-04 19:10:00', 320.40,'SG','completed');

SELECT count(*) AS row_count FROM orders_lab;


-- ###########################################################################
-- 03 · HIDDEN PARTITIONING — filter the RAW column, still get pruning
-- ###########################################################################
-- No day= predicate anywhere. Watch "Data scanned" in the results pane.

SELECT count(*) AS one_day
FROM orders_lab
WHERE order_timestamp >= TIMESTAMP '2026-03-02 00:00:00'
  AND order_timestamp <  TIMESTAMP '2026-03-03 00:00:00';

SELECT count(*) AS all_days FROM orders_lab;


-- ###########################################################################
-- 04 · ACID DML — what a plain Parquet lake cannot do at all
-- ###########################################################################

UPDATE orders_lab SET status = 'refunded' WHERE order_id = 'ORD-1003';

DELETE FROM orders_lab WHERE total_amount < 50.00;

MERGE INTO orders_lab AS t
USING (
    SELECT * FROM (VALUES
        ('ORD-1002','U-02',TIMESTAMP '2026-03-01 11:40:00',700.00,'DE','completed'),
        ('ORD-9001','U-09',TIMESTAMP '2026-03-05 12:00:00',250.00,'JP','completed')
    ) AS s (order_id,user_id,order_timestamp,total_amount,country,status)
) AS s
ON t.order_id = s.order_id
WHEN MATCHED THEN
    UPDATE SET total_amount = s.total_amount, status = s.status
WHEN NOT MATCHED THEN
    INSERT (order_id,user_id,order_timestamp,total_amount,country,status)
    VALUES (s.order_id,s.user_id,s.order_timestamp,s.total_amount,s.country,s.status);

SELECT order_id, total_amount, status FROM orders_lab ORDER BY order_id;


-- ###########################################################################
-- 05 · METADATA TABLES — Athena uses  "table$suffix"  with DOUBLE quotes
-- ###########################################################################
-- The $ part goes INSIDE the quotes:  "orders_lab$snapshots"

SELECT committed_at, snapshot_id, parent_id, operation, summary
FROM "orders_lab$snapshots"
ORDER BY committed_at;

SELECT file_path, record_count, file_size_in_bytes, partition
FROM "orders_lab$files";

-- THE compaction diagnostic: many files, small average size
SELECT partition,
       count(*)                                AS files,
       sum(record_count)                       AS num_rows,
       cast(avg(file_size_in_bytes) AS bigint) AS avg_bytes
FROM "orders_lab$files"
GROUP BY partition
ORDER BY files DESC;

SELECT * FROM "orders_lab$history" ORDER BY made_current_at;
SELECT * FROM "orders_lab$manifests";
SELECT * FROM "orders_lab$partitions";
SELECT * FROM "orders_lab$refs";


-- ###########################################################################
-- 06 · TIME TRAVEL
-- ###########################################################################

-- relative timestamp: always works, nothing to paste
SELECT count(*) AS rows_a_moment_ago
FROM orders_lab FOR TIMESTAMP AS OF (current_timestamp - INTERVAL '5' MINUTE);

-- the row you UPDATEd, as it was before — nothing restored
SELECT order_id, status
FROM orders_lab FOR TIMESTAMP AS OF (current_timestamp - INTERVAL '5' MINUTE)
WHERE order_id = 'ORD-1003';

-- by snapshot id  <-- paste one from block 05
SELECT count(*) AS rows_at_snapshot
FROM orders_lab FOR VERSION AS OF 1234567890123456789;


-- ###########################################################################
-- 07 · SCHEMA EVOLUTION — metadata only, no data rewritten
-- ###########################################################################

ALTER TABLE orders_lab ADD COLUMNS (channel string, discount_pct double);

-- existing rows read NULL for the new column. Correct, not an error.
SELECT order_id, channel FROM orders_lab LIMIT 5;

ALTER TABLE orders_lab DROP COLUMN discount_pct;

DESCRIBE orders_lab;

-- NOT SUPPORTED on S3 Tables per AWS docs:  ALTER TABLE ... RENAME
-- Column renames must be done from Spark:
--     ALTER TABLE cat.db.orders_lab RENAME COLUMN country TO country_code;


-- ###########################################################################
-- 08 · CTAS  — read the three rules first, they are not obvious
-- ###########################################################################
-- Per the AWS docs, CTAS for S3 Tables differs from normal Athena CTAS:
--   1. OMIT location        - S3 Tables manage their own storage.
--   2. OMIT table_type      - it already DEFAULTS to ICEBERG.
--   3. OMIT format          - it already defaults to PARQUET.
--   Do NOT pass is_external. It is not an S3 Tables property and it is what
--   makes Athena try to write into your query-results bucket instead.
--
-- So the whole WITH clause is usually just the partitioning:

CREATE TABLE high_value_orders_lab
WITH (
    partitioning = ARRAY['day(order_timestamp)', 'bucket(64, user_id)']
) AS
SELECT order_id, user_id, order_timestamp, total_amount, country
FROM orders_lab
WHERE total_amount > 500.00;

SELECT * FROM high_value_orders_lab;

-- ---------------------------------------------------------------------------
-- IF YOU GET  TABLE_ALREADY_EXISTS  AFTER A FAILED CTAS
-- ---------------------------------------------------------------------------
-- The AWS docs are explicit about this one:
--   "If your CTAS query fails, you might have to delete your table using the
--    S3 Tables API before attempting to re-run your query. You cannot use the
--    Athena DROP TABLE statements to remove the table that was partially
--    created by the query."
--
-- So DROP TABLE will NOT clear it. Use the CLI (adjust names/region):
--
--   aws s3tables delete-table \
--     --table-bucket-arn arn:aws:s3tables:us-east-1:<ACCOUNT_ID>:bucket/<TABLE_BUCKET> \
--     --namespace <NAMESPACE> \
--     --name <TABLE_NAME> \
--     --region us-east-1
--
-- Then list what actually exists, to confirm it is gone:
--
--   aws s3tables list-tables \
--     --table-bucket-arn arn:aws:s3tables:us-east-1:<ACCOUNT_ID>:bucket/<TABLE_BUCKET> \
--     --namespace <NAMESPACE> --region us-east-1
--
-- Every value you need is inside the Athena error text itself:
--   "catalog:<ACCOUNT_ID>:s3tablescatalog/<TABLE_BUCKET>$schema:<NAMESPACE>"


-- ###########################################################################
-- 09 · JOINS
-- ###########################################################################

SELECT o.country,
       count(*)            AS orders,
       sum(h.total_amount) AS high_value_revenue
FROM      orders_lab            o
LEFT JOIN high_value_orders_lab h ON o.order_id = h.order_id
GROUP BY o.country
ORDER BY orders DESC;

-- LEFT ANTI: orders that never reached the high-value table
SELECT o.order_id, o.total_amount
FROM orders_lab o
WHERE NOT EXISTS (
    SELECT 1 FROM high_value_orders_lab h WHERE h.order_id = o.order_id);

-- CROSS-CATALOG JOIN: an S3 Table joined to a plain Glue table.
-- Here the OTHER side needs its full path because it lives in a different
-- catalog than the dropdown. This is the one place you qualify.
--
-- SELECT o.order_id, o.total_amount, d.region_name
-- FROM orders_lab o
-- JOIN "awsdatacatalog"."legacy_db"."country_dim" d
--   ON o.country = d.country_code;


-- ###########################################################################
-- 10 · VIEWS — NOT SUPPORTED ON S3 TABLES
-- ###########################################################################
-- AWS docs, Considerations and limitations:
--   "ALTER TABLE RENAME, CREATE VIEW, and ALTER DATABASE are not supported."
--
-- Two ways around it:
--   a) create the view in the regular AwsDataCatalog over the S3 Table, or
--   b) materialise it as a table, which is what block 11 does.


-- ###########################################################################
-- 11 · MATERIALISED RESULT — the supported stand-in for a view
-- ###########################################################################

CREATE TABLE mv_hourly_metrics_lab
WITH (
    partitioning = ARRAY['day(order_hour)']
) AS
SELECT date_trunc('hour', order_timestamp) AS order_hour,
       country,
       count(order_id)   AS total_orders,
       sum(total_amount) AS revenue
FROM orders_lab
GROUP BY 1, 2;

SELECT * FROM mv_hourly_metrics_lab ORDER BY order_hour;

-- "refresh" = delete the affected window, re-insert it. Idempotent.
DELETE FROM mv_hourly_metrics_lab
WHERE order_hour >= TIMESTAMP '2026-03-04 00:00:00';

INSERT INTO mv_hourly_metrics_lab
SELECT date_trunc('hour', order_timestamp) AS order_hour,
       country,
       count(order_id)   AS total_orders,
       sum(total_amount) AS revenue
FROM orders_lab
WHERE order_timestamp >= TIMESTAMP '2026-03-04 00:00:00'
GROUP BY 1, 2;


-- ###########################################################################
-- 12 · MAINTENANCE — S3 Tables already does it
-- ###########################################################################
-- The docs list OPTIMIZE and VACUUM as exceptions for S3 Tables: compaction,
-- snapshot expiry and orphan-file removal run CONTINUOUSLY in the background.
-- That is the headline feature of S3 Tables versus a self-managed Iceberg
-- table, where you schedule all of it yourself.
--
-- Watch it happen instead: run block 05's file-count query now, then again
-- tomorrow. File count falls without you doing anything.
--
-- Self-managed Iceberg equivalents you WOULD have to schedule:
--     Athena :  OPTIMIZE t REWRITE DATA USING BIN_PACK;   VACUUM t;
--     Spark  :  CALL cat.system.rewrite_data_files(...)
--               CALL cat.system.expire_snapshots(...)
--               CALL cat.system.remove_orphan_files(...)


-- ###########################################################################
-- 13 · WHAT S3 TABLES DOES **NOT** DO
-- ###########################################################################
-- Hash / range / round-robin / coalesce / broadcast are COMPUTE-ENGINE
-- shuffle mechanics. They happen inside the engine, in worker memory,
-- before any Parquet is written.
--
-- S3 Tables owns exactly two things:
--   1. METADATA DEFINITIONS  - partition transforms and bucket specs (block 01)
--   2. PHYSICAL MAINTENANCE  - background compaction and expiry (block 12)
--
-- You cannot "set broadcast join" on an S3 Table. You set it on the engine.


-- ###########################################################################
-- 99 · TEARDOWN
-- ###########################################################################

-- DROP TABLE mv_hourly_metrics_lab;
-- DROP TABLE high_value_orders_lab;
-- DROP TABLE orders_lab;
