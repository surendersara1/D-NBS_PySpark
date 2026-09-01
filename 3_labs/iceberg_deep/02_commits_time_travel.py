"""
02_commits_time_travel.py  —  every write is a snapshot; every snapshot is a
                              table you can still read.

Atlas chapters 03, 04 and 06.

    python 02_commits_time_travel.py

What you should take away:
  * INSERT / UPDATE / DELETE / MERGE each append exactly one snapshot.
  * Time travel is not a backup. It is the ordinary read path entered at a
    different pointer, over data files that were never copied.
  * A rollback leaves a permanent, detectable fingerprint in .history.
"""
import config as C
from common import (get_spark, fq, meta, block, sql, check,
                    snapshot_ids, current_snapshot, file_count)

spark = get_spark("02_commits")
T = fq("orders")

start_rows = spark.table(T).count()
start_snaps = len(snapshot_ids(spark))
print(f"  starting point: {start_rows} rows, {start_snaps} snapshots")

# ===========================================================================
block("01", "FOUR WRITE OPERATIONS, FOUR SNAPSHOTS",
      "watch the 'operation' column - Iceberg records WHAT each commit did")
# ===========================================================================
spark.sql(f"""INSERT INTO {T} VALUES
    (7001, 5, 'EMEA', 'Home', 'completed', 42.00, TIMESTAMP '2026-03-03 10:00:00')""")
print("  1/4  INSERT   committed")

spark.sql(f"UPDATE {T} SET order_amount = 43.50 WHERE order_id = 7001")
print("  2/4  UPDATE   committed")

spark.sql(f"DELETE FROM {T} WHERE order_id = 7001")
print("  3/4  DELETE   committed")

spark.sql(f"""
    MERGE INTO {T} t
    USING {fq('orders_staging')} s
       ON t.order_id = s.order_id
    WHEN MATCHED THEN UPDATE SET
        t.order_amount = s.order_amount, t.status = s.status
    WHEN NOT MATCHED THEN INSERT *""")
print("  4/4  MERGE    committed  (12 updates + 8 inserts)")

sql(spark, f"""
    SELECT committed_at, operation,
           summary['added-data-files']   AS added_f,
           summary['deleted-data-files'] AS deleted_f,
           summary['added-records']      AS added_r,
           summary['total-records']      AS total_r
    FROM {meta('snapshots')} ORDER BY committed_at""",
    label="the commit log - note operation: append / overwrite / delete",
    n=20, truncate=False)

after_merge = spark.table(T).count()
print(f"  rows: {start_rows} -> {after_merge}  (+8 inserted by the MERGE)")

# ===========================================================================
block("02", "THE MERGE ACTUALLY MERGED",
      "12 existing orders were updated in place, not duplicated")
# ===========================================================================
sql(spark, f"""
    SELECT status, count(*) AS n FROM {T}
    WHERE order_amount = 999.99 GROUP BY status""",
    label="the 12 updated rows carry the staging table's values",
    truncate=False)

dupes = spark.sql(f"""
    SELECT count(*) FROM (
      SELECT order_id FROM {T} GROUP BY order_id HAVING count(*) > 1)""").first()[0]
print(f"  duplicate order_ids after MERGE: {dupes}")

# ===========================================================================
block("03", "TIME TRAVEL BY SNAPSHOT ID",
      "the same tree, entered at an older pointer")
# ===========================================================================
snaps = snapshot_ids(spark)
first_snap, last_snap = snaps[0], snaps[-1]

now_rows = spark.table(T).count()
then_rows = spark.sql(f"SELECT count(*) FROM {T} VERSION AS OF {first_snap}").first()[0]
print(f"  current snapshot  {last_snap}  ->  {now_rows} rows")
print(f"  first snapshot    {first_snap}  ->  {then_rows} rows")

# The 999.99 updates cannot exist in the first snapshot - it predates the MERGE
then_updated = spark.sql(f"""
    SELECT count(*) FROM {T} VERSION AS OF {first_snap}
    WHERE order_amount = 999.99""").first()[0]
print(f"  rows at 999.99 in the first snapshot: {then_updated}  (the MERGE hadn't happened)")

# ===========================================================================
block("04", "TIME TRAVEL BY TIMESTAMP",
      "ask for a moment; Iceberg picks the newest snapshot older than it")
# ===========================================================================
# TWO traps in four lines, both worth knowing:
#
# 1. Iceberg resolves to the newest snapshot STRICTLY OLDER than the value you
#    give. Passing a snapshot's own commit time finds nothing older and raises.
#    So we ask for a moment one second AFTER the first commit.
# 2. Never let a Spark timestamp become a Python datetime on the way. PySpark
#    converts it into the DRIVER's local zone, and Iceberg then parses your
#    string back as session time - silently shifting it by your UTC offset.
#    Format it to a string inside SQL with date_format() and the zone is fixed.
ts = spark.sql(f"""
    SELECT date_format(committed_at + INTERVAL 1 SECOND, 'yyyy-MM-dd HH:mm:ss.SSS')
    FROM {meta('snapshots')} ORDER BY committed_at LIMIT 1""").first()[0]
by_ts = spark.sql(
    f"SELECT count(*) FROM {T} TIMESTAMP AS OF '{ts}'").first()[0]
print(f"  TIMESTAMP AS OF '{ts}' -> {by_ts} rows  (resolves to snapshot 1)")

try:
    spark.sql(f"SELECT count(*) FROM {T} TIMESTAMP AS OF '1999-01-01 00:00:00'").first()
    too_early_raised = False
except Exception as e:
    too_early_raised = "snapshot older than" in str(e) or "Cannot find" in str(e)
    print(f"  asking for 1999 raises, it does not return empty:")
    print(f"    {str(e).splitlines()[0][:110]}")

# ===========================================================================
block("05", "DATA FILES ARE SHARED, NOT COPIED",
      "this is why time travel is nearly free - and why storage grows")
# ===========================================================================
live_files = file_count(spark)
all_files = spark.sql(
    f"SELECT count(DISTINCT file_path) FROM {meta('all_data_files')}").first()[0]
print(f"  files the CURRENT snapshot references .... {live_files}")
print(f"  distinct files across ALL live snapshots . {all_files}")
print(f"  files kept alive only by history ......... {all_files - live_files}")
print("\n  Those extra files are the price of time travel. expire_snapshots")
print("  (lab 05) is what eventually releases them.")

# ===========================================================================
block("06", "ROLLBACK - AND THE FINGERPRINT IT LEAVES",
      "undo the MERGE by making an older snapshot current again")
# ===========================================================================
target = snaps[-2]                      # the snapshot just before the MERGE
before_rollback = spark.table(T).count()

spark.sql(f"CALL {C.CATALOG}.system.rollback_to_snapshot('{C.DB}.orders', {target})")
after_rollback = spark.table(T).count()
print(f"  rows {before_rollback} -> {after_rollback} after rolling back to {target}")

sql(spark, f"""
    SELECT made_current_at, snapshot_id, parent_id, is_current_ancestor
    FROM {meta('history')} ORDER BY made_current_at""",
    label="history - TWO snapshots now share a parent, and one is not an ancestor",
    n=20, truncate=False)

orphaned = spark.sql(f"""
    SELECT count(*) FROM {meta('history')} WHERE is_current_ancestor = false
""").first()[0]
print(f"  snapshots marked is_current_ancestor = false: {orphaned}")
print("  That pair - shared parent_id, one false - IS the rollback signature.")
print("  It is how you prove after the fact that somebody rolled this table back.")

# ===========================================================================
block("07", "ROLL FORWARD AGAIN",
      "the MERGE snapshot was never deleted, so we can just point at it")
# ===========================================================================
spark.sql(f"CALL {C.CATALOG}.system.set_current_snapshot('{C.DB}.orders', {last_snap})")
restored = spark.table(T).count()
print(f"  rows back to {restored} by set_current_snapshot({last_snap})")
print("  rollback_to_snapshot only walks this table's history;")
print("  set_current_snapshot accepts ANY snapshot, including one on a branch.")

# ===========================================================================
block("08", "VERIFY")
# ===========================================================================
ops = [r[0] for r in spark.sql(
    f"SELECT operation FROM {meta('snapshots')} ORDER BY committed_at").collect()]

check(len(snaps) == start_snaps + 4,
      f"four writes produced four new snapshots ({start_snaps} -> {len(snaps)})")
check("append" in ops and ("overwrite" in ops or "delete" in ops),
      f"snapshot operations recorded distinctly: {sorted(set(ops))}")
check(after_merge == start_rows + 8,
      "MERGE inserted 8 and updated 12 - it did not append 20")
check(dupes == 0, "no duplicate order_id after the MERGE")
check(then_rows == C.SEED_ORDERS // 3,
      f"the first snapshot still shows only its own 80 rows (got {then_rows})")
check(then_updated == 0,
      "the first snapshot cannot see updates that happened later")
check(too_early_raised,
      "travelling before the table existed raises, it does not silently return empty")
check(all_files >= live_files,
      f"history keeps {all_files - live_files} extra files alive")
check(orphaned >= 1, "the rollback left a detectable non-ancestor snapshot")
check(restored == after_merge, "set_current_snapshot rolled us forward again")

print("\nCommits and time travel complete. Next: 03_evolution.py\n")
spark.stop()
