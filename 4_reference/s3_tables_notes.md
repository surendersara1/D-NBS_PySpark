Amazon S3 Tables deliver fully managed, engine-agnostic Apache Iceberg storage built directly into Amazon S3, optimized for high-throughput analytics and continuous maintenance via the AWS Glue Data Catalog.

**Types of Tables**

- **Standard Iceberg Tables:** Fully managed Apache Iceberg tables supporting both Iceberg V2 and V3 specifications.
- **Semi-Structured (Variant) Tables:** Iceberg V3 tables utilizing the `variant` data type to store dynamic JSON while automatically shredding fields into optimized Parquet columns.
- **S3 System Metadata Tables:** Automatically generated tables providing system metrics, snapshot history, manifest lists, and data file details exposed as queryable Iceberg objects.

**Supported Joins**
Because S3 Tables interact via the open Apache Iceberg REST Catalog API, join behavior is determined by the processing engine (e.g., Athena, EMR Spark, Glue ETL, Redshift, Trino, Flink):

- **Standard SQL Joins:** Inner, Left/Right/Full Outer, Cross, Semi, and Anti joins across S3 Tables.
- **Cross-Format & Cross-Storage Joins:** Native joins between S3 Tables and external storage (e.g., general-purpose S3 Parquet/Delta/Hudi tables, relational DBs via Athena, or S3 Vector buckets).
- **Cross-Namespace & Cross-Account Joins:** Joining S3 Tables across different Glue namespaces, table buckets, or AWS accounts.

**Types of Views**

| View Type                                   | Storage Location                                                                          | Primary Use Case                                                                            | Supported Engines                                         |
| ------------------------------------------- | ----------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- | --------------------------------------------------------- |
| **AWS Glue Data Catalog Virtual Views**     | Virtual SQL definitions stored in Glue Data Catalog (no physical data).                   | Logical abstraction, cross-engine dialect mapping, and secure sharing without data copying. | Athena, Spark, Redshift Spectrum.                         |
| **Apache Iceberg Materialized Views (MVs)** | Precomputed query results stored physically as Iceberg tables in S3 Tables or S3 buckets. | Accelerating expensive aggregations and heavy joins while reducing compute costs.           | Created via Spark (EMR/Glue); Queried via Athena & Spark. |

**Core Table Features**

- **ACID Compliance:** Atomic transactions, optimistic concurrency control, and write rollback capabilities.
- **Automated Maintenance:** Continuous background file compaction, snapshot expiration, and unreferenced file removal without manual cron jobs.
- **Schema Evolution:** In-place column additions, deletions, renames, reordering, and type widening without table rewrites.
- **Partition Evolution:** Alter partitioning strategies over time (e.g., day to hour) without rewriting historical data.
- **Time Travel & Rollback:** Query historical table snapshots via `AS OF TIMESTAMP` or snapshot version IDs.
- **Iceberg V3 Capabilities:** Support for Deletion Vectors (Merge-on-Read fast deletes), Row Lineage tracking for CDC, and JSON `variant` data types.
- **High Performance & Storage Optimization:** Delivers up to 10x higher transactions per second (TPS) and 3x faster queries, with automatic S3 Intelligent-Tiering reducing storage costs up to 80%.

**View & Materialized View Features**

- **Incremental & Scheduled Refreshes:** Materialized views support manual or automated schedule refreshes (`REFRESH EVERY`), updating only changed partitions/rows.
- **Multi-Dialect SQL Storage:** Virtual views store SQL statements tailored to multiple query engines (Spark SQL, Athena SQL, Redshift SQL) under one object.
- **Security & Abstraction:** Grant `SELECT` permissions on a View or Materialized View without giving users access to raw underlying base tables.
- **Engine Decoupling:** Precompute complex MVs using Spark batch clusters and query them with low-latency engines like Athena.

Are you planning to build these S3 Tables using Amazon Athena, EMR Spark, or AWS Glue ETL as your primary compute engine?89-369-YHVH  [12:01 PM]
**No.** The Amazon S3 Tables API does not handle runtime data distribution mechanisms like Hash, Range, Coalesce, or Broadcast. S3 Tables is strictly a **managed storage engine and Apache Iceberg REST catalog**.

Execution shuffles occur inside the **compute engine** (e.g., Apache Spark, AWS Glue, Amazon Athena). S3 Tables only manages **metadata definitions** (e.g., Iceberg partition and bucket specs) and **physical storage maintenance** (e.g., background file compaction).

**Storage vs. Compute Responsibility**

| Feature                          | Primary Handled By        | Mechanism in S3 Tables Architecture                                                        |
| -------------------------------- | ------------------------- | ------------------------------------------------------------------------------------------ |
| **Hash / Range / Round-Robin**   | Compute Engine (Spark)    | Memory shuffle on worker nodes before generating Parquet files sent to S3.                 |
| **Coalesce / Broadcast**         | Compute Engine (Spark)    | Execution DAG optimization (bypassing or altering runtime network shuffles).               |
| **On Write / Partitioning**      | Iceberg Spec (S3 Catalog) | Hidden partition transforms (`day(ts)`, `truncate(N, col)`) tracked in catalog metadata.   |
| **Bucketing (`bucket(N, col)`)** | Iceberg Spec + Spark      | Hashing high-cardinality keys into $N$ deterministic buckets written to S3 manifest files. |

**Educational Summary: The 7 Distribution Rules in S3 Tables**

**Hash Partitioning**

- **The Reality:** Executor-level shuffle computing $\text{hash}(key) \pmod N$ to group matching keys onto dedicated nodes.
- **S3 Tables Role:** Simply receives the resulting Parquet files. Severe data skew on a key produces uneven file sizes in S3, causing straggler tasks during engine reads.

**Range Partitioning**

- **The Reality:** The only shuffle method that preserves total sorting order across output partitions. Spark runs an initial **sampling job** across dataset partitions to calculate equal-quantile boundaries (this appears as a preliminary stage in the Spark UI).
- **S3 Tables Role:** Stored file ranges align with global order, allowing Iceberg manifest files to perform min/max data pruning during downstream queries.

**Round-Robin Partitioning**

- **The Reality:** Uniformly distributes rows across partitions ($1, 2, 3 \dots N$) to guarantee equal partition sizes regardless of key content.
- **S3 Tables Role:** Destroys data locality. It prevents Iceberg metadata from filtering rows on read, but guarantees no single Parquet file in the S3 Table bucket is oversized.

**Coalesce vs. Repartition**

- **The Reality:** `coalesce(N)` reduces partition count **without a shuffle** by combining local worker partitions.
- **The Trap:** `coalesce` pushes upstream transformations into fewer parallel execution threads. `repartition(N)` forces a full shuffle, creating a clean stage boundary and re-balancing parallel workloads across workers.
- **S3 Tables Role:** `coalesce` controls how many physical Parquet files Spark writes into S3 Tables without paying a network shuffle tax.

**Broadcast Joins**

- **The Reality:** Replicates an entire small table directly to every executor node's memory, **deleting the join shuffle entirely**.
- **S3 Tables Role:** Spark fetches file references from the S3 Tables catalog, reads the small table into memory, and joins it locally with large table partitions.

**On Write Partitioning**

- **The Reality:** Organizes table layout by logical groups. Low-cardinality columns (e.g., `year`, `region`) speed up queries via metadata pruning. High-cardinality columns (e.g., `user_id`, `timestamp`) trigger the **small file problem**, creating millions of tiny files that overwhelm metadata catalogs.
- **S3 Tables Role:** S3 Tables runs continuous background compaction to merge small Parquet files automatically, though extreme high-cardinality partitioning should still be avoided.

**Bucketing (`bucket(N, col)`)**

- **The Escape Hatch:** Resolves high-cardinality partitioning issues. Instead of creating individual directories for every value, Iceberg hashes the column into a fixed count of $N$ buckets (e.g., `bucket(128, user_id)`).
- **S3 Tables Role:** Defined directly in the S3 Table Iceberg schema. If two tables share the exact same bucket column and bucket count ($N$), query engines can execute **shuffle-less bucket joins** by reading matching bucket files directly.

Are you looking to configure these Iceberg bucket specifications using PySpark DDL or via Amazon Athena DDL statements?89-369-YHVH  [1:36 PM]
Here are the exact SQL DDL CREATE commands for Amazon S3 Tables (using Apache Iceberg) across Amazon Athena and Apache Spark, incorporating hidden partitioning, bucketing, semi-structured variant data, views, and system metadata tables.

In all examples, s3tablescatalog is the Glue Data Catalog integration for S3 Table Buckets.

1. Standard S3 Table with Hidden Partitioning & Bucketing (bucket(N, col))

Defines high-cardinality hashing (bucket(128, user_id)) and date-range pruning (day(order_timestamp)) directly in the Iceberg catalog schema.

• Athena / Spark SQL DDL:

SQL

CREATE TABLE `s3tablescatalog`.`ecom_namespace`.`orders` (
order_id STRING,
user_id STRING,
order_timestamp TIMESTAMP,
total_amount DOUBLE,
country STRING
)
USING iceberg
PARTITIONED BY (
day(order_timestamp),
bucket(128, user_id)
)
TBLPROPERTIES (
'table_type' = 'ICEBERG',
'write.parquet.compression-codec' = 'zstd'
);2. Semi-Structured (Variant Data) Table (Iceberg V3)

Creates a table that auto-shreds dynamic JSON fields into optimized Parquet columns using the native VARIANT data type.

• Spark SQL DDL:

SQL

CREATE TABLE `s3tablescatalog`.`analytics_namespace`.`raw_app_events` (
event_id STRING,
user_id STRING,
event_time TIMESTAMP,
event_payload VARIANT
)
USING iceberg
PARTITIONED BY (
hours(event_time),
bucket(64, user_id)
)
TBLPROPERTIES (
'format-version' = '3'
);3. CTAS (Create Table As Select) in S3 Tables

Transforms and writes incoming raw S3/Parquet data directly into an S3 Table in a single atomic query.

• Athena DDL:

SQL

CREATE TABLE `s3tablescatalog`.`ecom_namespace`.`high_value_orders`
WITH (
format = 'PARQUET',
partitioning = ARRAY['day(order_timestamp)', 'bucket(64, user_id)']
) AS
SELECT
order_id,
user_id,
order_timestamp,
total_amount
FROM `awsdatacatalog`.`legacy_db`.`raw_orders`
WHERE total_amount > 500.00;4. Virtual Views & Materialized Views

• Glue Virtual View (Logical SQL abstraction shared across Athena & Spark):

SQL

CREATE VIEW `s3tablescatalog`.`ecom_namespace`.`v_daily_revenue` AS
SELECT
date_trunc('day', order_timestamp) AS order_date,
count(DISTINCT user_id) AS active_users,
sum(total_amount) AS daily_revenue
FROM `s3tablescatalog`.`ecom_namespace`.`orders`
GROUP BY 1;•
Iceberg Materialized View (Precomputed Iceberg table backed by physical storage):

SQL

-- Executed via Spark SQL Engine
CREATE MATERIALIZED VIEW `s3tablescatalog`.`ecom_namespace`.`mv_hourly_metrics`
PARTITIONED BY (hours(order_hour))
AS
SELECT
date_trunc('hour', order_timestamp) AS order_hour,
country,
count(order_id) AS total_orders,
sum(total_amount) AS revenue
FROM `s3tablescatalog`.`ecom_namespace`.`orders`
GROUP BY 1, 2;5. Querying S3 System Metadata Tables

S3 Tables automatically generate queryable metadata objects for time travel, snapshot audits, and file layout tracking:
SQL

-- View all committed snapshots (for Time Travel audits)
SELECT \* FROM `s3tablescatalog`.`ecom_namespace`.`orders$snapshots`;

-- View data files, file sizes, and partition min/max bounds
SELECT \* FROM `s3tablescatalog`.`ecom_namespace`.`orders$files`;

-- View metadata history and snapshot evolution
SELECT \* FROM `s3tablescatalog`.`ecom_namespace`.`orders$history`;

-- View individual manifest files tracking Parquet paths
SELECT \* FROM `s3tablescatalog`.`ecom_namespace`.`orders$manifests`;
