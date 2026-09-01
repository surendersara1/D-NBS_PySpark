"""
03_evolution.py  —  changing the shape of a live table without rewriting it.

Atlas chapters 07 and 08.

    python 03_evolution.py

What you should take away:
  * Columns are tracked by permanent integer ID. Renaming one is metadata.
  * An id is retired on DROP and never reissued - so a stale file can never
    resurrect as the wrong column.
  * Partition evolution does not touch old data. The engine plans each spec
    separately and unions the results.
"""
import config as C
from common import (get_spark, fq, meta, block, sql, check, file_count)

spark = get_spark("03_evolution")
T = fq("orders")

# ===========================================================================
block("01", "THE SCHEMA IS A MAP OF IDs",
      "note last-column-id: the high-water mark that guarantees uniqueness")
# ===========================================================================
sql(spark, f"DESCRIBE TABLE {T}", label="the table as it stands", truncate=False)

# Column ids are visible in the file statistics: the bound maps are keyed by id.
sql(spark, f"""
    SELECT file_path, lower_bounds
    FROM {meta('files')} WHERE content = 0 LIMIT 1""",
    label="lower_bounds is keyed by COLUMN ID - {1 -> .., 2 -> ..}",
    truncate=False)

rows_before = spark.table(T).count()
files_before = file_count(spark)
snaps_before = spark.sql(f"SELECT count(*) FROM {meta('snapshots')}").first()[0]
print(f"\n  before any evolution: {rows_before} rows, {files_before} files, "
      f"{snaps_before} snapshots")

# ===========================================================================
block("02", "RENAME A COLUMN",
      "the id does not move, so every existing Parquet file still resolves")
# ===========================================================================
sample_before = spark.sql(
    f"SELECT department FROM {T} WHERE order_id = 1000").first()[0]

spark.sql(f"ALTER TABLE {T} RENAME COLUMN department TO dept")
print(f"  department -> dept")

sample_after = spark.sql(
    f"SELECT dept FROM {T} WHERE order_id = 1000").first()[0]
print(f"  order 1000 read as 'department' before: {sample_before!r}")
print(f"  order 1000 read as 'dept'       after : {sample_after!r}")
print("  Same bytes on disk. Nothing was rewritten. In Hive this would have")
print("  either failed or, worse, silently returned the wrong column.")

files_after_rename = file_count(spark)
snaps_after_rename = spark.sql(f"SELECT count(*) FROM {meta('snapshots')}").first()[0]
print(f"\n  data files: {files_before} -> {files_after_rename}"
      f"    snapshots: {snaps_before} -> {snaps_after_rename}")
print("  A schema change is not a data operation - it adds no snapshot at all.")

# ===========================================================================
block("03", "ADD A COLUMN",
      "old files have no such id, so they read NULL - not an error")
# ===========================================================================
spark.sql(f"ALTER TABLE {T} ADD COLUMN channel STRING")
spark.sql(f"ALTER TABLE {T} ADD COLUMN margin_pct DOUBLE AFTER order_amount")
print("  added channel (at the end) and margin_pct (positioned AFTER order_amount)")

nulls = spark.sql(f"SELECT count(*) FROM {T} WHERE channel IS NULL").first()[0]
print(f"  existing rows now reporting channel IS NULL: {nulls} of {rows_before}")

# New writes populate it; old rows keep reading NULL. Both are correct.
spark.sql(f"""INSERT INTO {T}
    (order_id, customer_id, region, dept, status, order_amount,
     margin_pct, order_ts, channel)
    VALUES (8001, 9, 'EMEA', 'Home', 'completed', 55.00, 0.31,
            TIMESTAMP '2026-03-04 09:00:00', 'web')""")
mixed = spark.sql(
    f"SELECT count(*) FROM {T} WHERE channel IS NOT NULL").first()[0]
print(f"  rows with a channel value after one new insert: {mixed}")

sql(spark, f"DESCRIBE TABLE {T}",
    label="margin_pct sits where we asked, not at the end", truncate=False)

# ===========================================================================
block("04", "PROMOTE A TYPE",
      "widening is safe and free; narrowing is refused")
# ===========================================================================
spark.sql(f"ALTER TABLE {T} ALTER COLUMN customer_id TYPE BIGINT")
print("  customer_id: BIGINT (already wide here, but this is the safe direction)")

print("\n  The next statement is SUPPOSED to fail. Spark logs the refusal at")
print("  ERROR level before raising, so you will see a wall of red below -")
print("  that is the safety net working, not a broken lab.\n")
try:
    spark.sql(f"ALTER TABLE {T} ALTER COLUMN order_amount TYPE INT")
    narrowing_refused = False
except Exception as e:
    narrowing_refused = True
    print(f"\n  narrowing decimal(10,2) -> int refused:")
    print(f"    {str(e).splitlines()[0][:110]}")

# ===========================================================================
block("05", "DROP A COLUMN",
      "the id is retired permanently and never reissued")
# ===========================================================================
spark.sql(f"ALTER TABLE {T} DROP COLUMN margin_pct")
print("  dropped margin_pct")
cols = [f.name for f in spark.table(T).schema.fields]
print(f"  columns now: {cols}")

# Add a NEW column - it must NOT reuse the dropped id
spark.sql(f"ALTER TABLE {T} ADD COLUMN promo_code STRING")
print("  added promo_code - it receives a FRESH id, not margin_pct's old one")
print("  (that is what last-column-id in metadata.json guarantees)")

# ===========================================================================
block("06", "PARTITION EVOLUTION",
      "the table is partitioned by days(order_ts). Go coarser, live.")
# ===========================================================================
sql(spark, f"""
    SELECT spec_id, count(*) AS partitions_in_spec
    FROM {meta('partitions')} GROUP BY spec_id ORDER BY spec_id""",
    label="every partition currently belongs to spec 0", truncate=False)

files_before_spec = file_count(spark)          # measure immediately before
spark.sql(f"ALTER TABLE {T} ADD PARTITION FIELD months(order_ts)")
print("  ALTER TABLE ... ADD PARTITION FIELD months(order_ts)")
print("  Existing data is NOT rewritten. Only new writes use the new spec.")

files_after_spec = file_count(spark)
print(f"  data files before spec change: {files_before_spec}")
print(f"  data files after  spec change: {files_after_spec}   (unchanged)")

# Write something new - it lands under the new spec
spark.sql(f"""INSERT INTO {T}
    (order_id, customer_id, region, dept, status, order_amount, order_ts, channel)
    VALUES (8002, 11, 'APAC', 'Beauty', 'completed', 77.00,
            TIMESTAMP '2026-04-15 11:00:00', 'app')""")
print("  inserted one order in April, after the spec change")

sql(spark, f"""
    SELECT spec_id, count(*) AS partitions_in_spec
    FROM {meta('partitions')} GROUP BY spec_id ORDER BY spec_id""",
    label="TWO specs now coexist in one table", truncate=False)

specs = {r["spec_id"]: r["partitions_in_spec"] for r in spark.sql(f"""
    SELECT spec_id, count(*) AS partitions_in_spec
    FROM {meta('partitions')} GROUP BY spec_id""").collect()}
print(f"  spec ids present: {sorted(specs)}")
print("  A query spanning both builds a plan per spec and unions them -")
print("  which is exactly why the change cost nothing.")

# ===========================================================================
block("07", "AND IT ALL STILL READS",
      "old rows, new rows, two specs, one renamed column, one dropped")
# ===========================================================================
sql(spark, f"""
    SELECT region, count(*) AS orders, cast(sum(order_amount) AS DECIMAL(12,2)) AS revenue
    FROM {T} GROUP BY region ORDER BY revenue DESC""",
    label="a perfectly ordinary aggregation over an evolved table",
    truncate=False)

final_rows = spark.table(T).count()

# ===========================================================================
block("08", "VERIFY")
# ===========================================================================
schema_names = [f.name for f in spark.table(T).schema.fields]

check(sample_after == sample_before,
      f"the renamed column returns identical data ({sample_before!r})")
check(snaps_after_rename == snaps_before,
      "a RENAME added no snapshot - it is metadata, not a data operation")
check(files_after_rename == files_before,
      f"a RENAME rewrote no data files (still {files_before})")
check("dept" in schema_names and "department" not in schema_names,
      "the schema shows dept, not department")
check(nulls == rows_before,
      f"all {rows_before} pre-existing rows read NULL for the added column")
check(mixed == 1, "the one new row carries a channel value; old rows do not")
check(narrowing_refused,
      "narrowing decimal(10,2) -> int was refused, not silently applied")
check("margin_pct" not in schema_names, "margin_pct is gone from the schema")
check("promo_code" in schema_names, "promo_code was added after the drop")
check(files_after_spec == files_before_spec,
      f"ADD PARTITION FIELD rewrote zero data files (still {files_before_spec})")
check(len(specs) == 2,
      f"two partition specs now coexist in one table: {sorted(specs)}")
check(final_rows == rows_before + 2,
      f"every row still readable across both specs ({final_rows})")

print("\nEvolution complete. Next: 04_cow_mor_deletes.py\n")
spark.stop()
