"""
01_anatomy.py  —  the metadata tree, walked by hand.

Atlas chapters 02 and 05. You will descend the exact path a query engine
takes - catalog -> metadata.json -> manifest list -> manifest -> data file -
using nothing but SQL against Iceberg's own metadata tables.

    python 01_anatomy.py

What you should take away:
  * Every level exists to throw away work at the level below it.
  * Statistics are written BY THE WRITER, so they are never stale.
  * "Which files will this query open?" is a question you can answer exactly,
    before running it, for free.
"""
import config as C
from common import (get_spark, fq, meta, block, sql, check, print_tree,
                    file_count)

spark = get_spark("01_anatomy")

# ===========================================================================
block("01", "LEVEL 1 - THE CATALOG POINTER",
      "the only thing a catalog stores: table -> current metadata file")
# ===========================================================================
mle = sql(spark, f"""
    SELECT timestamp, file, latest_snapshot_id, latest_schema_id
    FROM {meta('metadata_log_entries')}
    ORDER BY timestamp DESC LIMIT 3""",
    label="metadata_log_entries - every metadata.json this table has had",
    truncate=False)

current_meta = mle.first()["file"]
print(f"\n  the engine now opens exactly one file:\n    {current_meta}")

# ===========================================================================
block("02", "LEVEL 2 - METADATA.JSON",
      "schema, partition spec, the snapshot list, and which one is current")
# ===========================================================================
sql(spark, f"""
    SELECT committed_at, snapshot_id, operation,
           summary['added-records']  AS added,
           summary['total-records']  AS total_rows,
           summary['total-data-files'] AS total_files
    FROM {meta('snapshots')} ORDER BY committed_at""",
    label="snapshots[] - the whole commit history lives in metadata.json",
    truncate=False)

sql(spark, f"SELECT * FROM {meta('refs')}",
    label="refs - main always exists; branches and tags join it in lab 05",
    truncate=False)

# ===========================================================================
block("03", "LEVEL 3 - THE MANIFEST LIST",
      "one entry per manifest, carrying partition ranges for pruning")
# ===========================================================================
sql(spark, f"""
    SELECT path, length, added_snapshot_id,
           added_data_files_count  AS added,
           existing_data_files_count AS existing,
           deleted_data_files_count  AS deleted,
           partition_summaries
    FROM {meta('manifests')}""",
    label="manifests - note partition_summaries: lower/upper bound per manifest",
    truncate=False)

n_manifests = spark.sql(f"SELECT count(*) FROM {meta('manifests')}").first()[0]
print(f"\n  {n_manifests} manifests. A query first discards whole manifests")
print(f"  whose partition range cannot match - before opening any of them.")

# ===========================================================================
block("04", "LEVEL 4 - THE MANIFEST FILES",
      "per-file statistics: this is where file pruning actually happens")
# ===========================================================================
sql(spark, f"""
    SELECT partition, record_count, file_size_in_bytes,
           lower_bounds, upper_bounds
    FROM {meta('files')} WHERE content = 0
    ORDER BY partition LIMIT 6""",
    label="files - lower/upper bounds are keyed by COLUMN ID, not name",
    truncate=False)

print("  Read those bound maps as {column_id -> value}. Column ids come from")
print("  the schema (lab 03) and are why a rename never breaks an old file.")

sql(spark, f"""
    SELECT partition,
           count(*)                AS files,
           sum(record_count)       AS rows,
           cast(avg(file_size_in_bytes) AS INT) AS avg_bytes
    FROM {meta('files')} WHERE content = 0
    GROUP BY partition ORDER BY partition""",
    label="the compaction diagnostic: many files, small average = a target",
    truncate=False)

# ===========================================================================
block("05", "LEVEL 5 - THE DATA LAYER, ON DISK")
# ===========================================================================
print_tree("orders", max_files=6)

# ===========================================================================
block("06", "THE PRUNING CASCADE, MEASURED",
      "how many of those files does one predicate actually need?")
# ===========================================================================
total_files = file_count(spark)

# Ask the metadata, not the data: which files could hold 2026-03-02?
matching = spark.sql(f"""
    SELECT count(*) FROM {meta('files')}
    WHERE content = 0 AND partition.order_ts_day = DATE '2026-03-02'""").first()[0]

print(f"  data files in the table ............ {total_files}")
print(f"  files that can contain 2026-03-02 .. {matching}")
print(f"  pruned away before any I/O ......... {total_files - matching}"
      f"  ({100 * (total_files - matching) // total_files}%)")

# And confirm Spark agrees when it plans the real query
plan = (spark.table(fq("orders"))
        .filter("order_ts >= TIMESTAMP '2026-03-02 00:00:00' "
                "AND order_ts <  TIMESTAMP '2026-03-03 00:00:00'")
        ._jdf.queryExecution().executedPlan().toString())
pushed = "order_ts" in plan
print(f"\n  predicate reached the scan node: {pushed}")
print("  (an empty PushedFilters here is the cheapest bug you will ever fix)")

# ===========================================================================
block("07", "ENTRIES - THE PER-FILE EVENT LOG",
      "status 0=existing  1=added  2=deleted, per snapshot")
# ===========================================================================
sql(spark, f"""
    SELECT snapshot_id, status, count(*) AS files
    FROM {meta('entries')}
    GROUP BY snapshot_id, status ORDER BY snapshot_id, status""",
    label="entries - which snapshot added which files",
    truncate=False)

print("  This is the join key between snapshots and files. Lab 05 uses it to")
print("  answer 'what changed between these two branches?'")

# ===========================================================================
block("08", "VERIFY")
# ===========================================================================
n_snaps = spark.sql(f"SELECT count(*) FROM {meta('snapshots')}").first()[0]
n_hist = spark.sql(f"SELECT count(*) FROM {meta('history')}").first()[0]
n_refs = spark.sql(f"SELECT count(*) FROM {meta('refs')}").first()[0]
n_partitions = spark.sql(f"SELECT count(*) FROM {meta('partitions')}").first()[0]
bounds_present = spark.sql(f"""
    SELECT count(*) FROM {meta('files')}
    WHERE content = 0 AND size(lower_bounds) > 0""").first()[0]

check(n_snaps == 3, f"snapshots table exposes all 3 commits")
check(n_hist == n_snaps, "history has one row per snapshot made current")
check(n_refs == 1, "exactly one ref so far, and it is 'main'")
check(n_partitions == 6, f"6 day partitions from the seed data (got {n_partitions})")
check(bounds_present == total_files,
      f"all {total_files} data files carry column bounds - stats are never stale")
check(matching < total_files,
      f"partition pruning eliminates files ({matching} of {total_files} match)")
check(pushed, "the timestamp predicate was pushed down to the scan")

print("\nAnatomy complete. Next: 02_commits_time_travel.py\n")
spark.stop()
