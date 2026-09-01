# Amazon S3 Tables (Apache Iceberg) Metadata Architecture & Lifecycle

> **The Golden Rule:** *Iceberg files are 100% immutable — no file is ever modified in-place or updated; every commit simply creates a new metadata JSON and manifest chain.*

---

## 1. Apache Iceberg Metadata Files on S3 Tables

* **Table Metadata File (`v<N>.metadata.json`):** Serves as the root definition of the table, tracking schemas, partition specs, sort orders, and the complete history of snapshot IDs.
* **Version Hint File (`version-hint.text`):** Stores an integer indicating the latest metadata version number (used by file-system catalogs when an external metastore is not tracking the pointer).
* **Manifest List File (`snap-<snapshot_id>-<uuid>.avro`):** Defines a single snapshot by listing all manifest files associated with that commit along with summary partition bounds.
* **Manifest File (`<uuid>.avro`):** Tracks the individual data files (and delete files), storing physical file paths, partition values, and column-level summary statistics for scan pruning.
* **Position Delete File (`<uuid>-deletes.parquet / .avro`):** Stores the file path and row positions of deleted records for row-level modifications in merge-on-read tables.
* **Equality Delete File (`<uuid>-equality-deletes.parquet / .avro`):** Stores specific column values that identify rows to delete across data files without referencing row indices.
* **Puffin File (`<uuid>.puffin`):** Stores auxiliary statistics, index data, and sketches (such as HyperLogLog and Theta Sketches for distinct count estimation).

---

## 2. Multi-Day Schema Evolution Lifecycle

### Scenario:
* **Day 1:** Create `table1` and insert initial data.
* **Day 2:** `ALTER TABLE table1 ADD COLUMN col1` (Metadata-only).
* **Day 3:** `ALTER TABLE table1 ADD COLUMN col2` (Metadata-only).
* **Day 4:** `ALTER TABLE table1 ADD COLUMN col3` (Metadata-only).

### File State Breakdown Across 4 Days:

| Day | Action | Data Files (`.parquet`) | Manifest Files (`.avro`) | Manifest List (`snap-*.avro`) | Table Metadata (`vN.metadata.json`) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Day 1** | Base Table + Initial Data | Creates initial `.parquet` | Creates `m1.avro` | Creates `snap-1.avro` | Creates `v1.metadata.json` |
| **Day 2** | `ALTER TABLE ADD COLUMN col1` | **None** (No data written) | **None** (Untouched) | **None** (Untouched) | Creates **`v2.metadata.json`** |
| **Day 3** | `ALTER TABLE ADD COLUMN col2` | **None** (No data written) | **None** (Untouched) | **None** (Untouched) | Creates **`v3.metadata.json`** |
| **Day 4** | `ALTER TABLE ADD COLUMN col3` | **None** (No data written) | **None** (Untouched) | **None** (Untouched) | Creates **`v4.metadata.json`** |

> *(Note: If data is inserted on any day, a new `.parquet`, a new `manifest.avro`, and a new `snap-*.avro` are created alongside the new metadata JSON.)*

---

## 3. Step-by-Step: Inspecting Historical Snapshots & Schemas on AWS (Athena / Spark)

### Step 1: List Historical Snapshots and Metadata Versions
Query Iceberg system metadata tables directly using Amazon Athena (or Spark SQL):

```sql
-- View snapshot history and parent-child commit lineage
SELECT snapshot_id, committed_at, operation, summary 
FROM "s3tablescatalog/my_bucket"."my_db"."table1$snapshots";

-- View chronological history of metadata JSON files
SELECT made_current_at, snapshot_id, metadata_file 
FROM "s3tablescatalog/my_bucket"."my_db"."table1$history";
```

### Step 2: Inspect Historical Schemas and Field IDs
Track when columns were introduced and how field IDs were assigned over time:

```sql
-- View schema timeline and evolution history
SELECT schema_id, current_schema_id, fields 
FROM "s3tablescatalog/my_bucket"."my_db"."table1$schemas";
```

### Step 3: Time-Travel Query to Older States
Read the table exactly as it existed on previous days using Snapshot ID or Timestamp:

```sql
-- Query as of a specific snapshot version (e.g., Day 1 state)
SELECT * FROM "s3tablescatalog/my_bucket"."my_db"."table1"
FOR VERSION AS OF 1092837465928374;

-- Query as of a specific historical timestamp
SELECT * FROM "s3tablescatalog/my_bucket"."my_db"."table1"
FOR TIMESTAMP AS OF TIMESTAMP '2026-08-29 00:00:00 UTC';
```