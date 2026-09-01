"""
05_maintenance_wap.py  —  operating the table: compaction, layout, expiry,
                          orphans, and never shipping bad data again.

Atlas chapters 10, 11 and 13.

    python 05_maintenance_wap.py

What you should take away:
  * Compaction, expiry and orphan removal do three DIFFERENT jobs. None of
    them substitutes for another.
  * Expiring snapshots is what actually frees storage - and it is what ends
    time travel. Those are the same operation.
  * write-audit-publish lets you write into the real table, audit it there,
    and publish only if it passes. No staging table, no copy.
"""
import os
from datetime import datetime, timedelta

import config as C
from common import (get_spark, fq, meta, block, sql, show, check,
                    file_count, snapshot_ids, warehouse_dir)

spark = get_spark("05_maintenance")

# A dedicated table so this lab is independent of what labs 02-04 left behind.
M = fq("t_maint")
spark.sql(f"DROP TABLE IF EXISTS {M} PURGE")
spark.sql(f"""
    CREATE TABLE {M} (
        order_id BIGINT, region STRING, dept STRING,
        order_amount DECIMAL(10,2), order_ts TIMESTAMP)
    USING iceberg PARTITIONED BY (days(order_ts))
    TBLPROPERTIES ('format-version' = '2')""")

# ===========================================================================
block("01", "MANUFACTURE THE SMALL FILES PROBLEM",
      "12 tiny appends - exactly what a trickle-feed pipeline produces")
# ===========================================================================
for i in range(12):
    spark.sql(f"""
        INSERT INTO {M}
        SELECT order_id, region, dept, order_amount, order_ts
        FROM {fq('orders')}
        WHERE order_id >= {1000 + i * 20} AND order_id < {1000 + (i + 1) * 20}""")

files_small = file_count(spark, "t_maint")
snaps_small = len(snapshot_ids(spark, "t_maint"))
rows_m = spark.table(M).count()
print(f"  {rows_m} rows across {files_small} data files, {snaps_small} snapshots")

sql(spark, f"""
    SELECT partition, count(*) AS files,
           cast(avg(file_size_in_bytes) AS INT) AS avg_bytes,
           sum(record_count) AS rows
    FROM {meta('files', 't_maint')} WHERE content = 0
    GROUP BY partition ORDER BY files DESC""",
    label="THE diagnostic: many files, tiny average size",
    truncate=False)

# ===========================================================================
block("02", "COMPACT - BINPACK",
      "combine files. No reordering, fastest possible job.")
# ===========================================================================
res = spark.sql(f"""
    CALL {C.CATALOG}.system.rewrite_data_files(
        table    => '{C.DB}.t_maint',
        strategy => 'binpack',
        options  => map('min-input-files','2'))""").first()

files_binpack = file_count(spark, "t_maint")
print(f"  rewrote {res['rewritten_data_files_count']} files"
      f" into {res['added_data_files_count']}")
print(f"  data files {files_small} -> {files_binpack}")
print(f"  rows unchanged: {spark.table(M).count()}")

# ===========================================================================
block("03", "COMPACT - SORT",
      "cluster rows across files so pruning has something to bite on")
# ===========================================================================
spark.sql(f"ALTER TABLE {M} WRITE ORDERED BY region ASC, order_amount DESC")
res = spark.sql(f"""
    CALL {C.CATALOG}.system.rewrite_data_files(
        table      => '{C.DB}.t_maint',
        strategy   => 'sort',
        sort_order => 'region ASC NULLS LAST, order_amount DESC',
        options    => map('rewrite-all','true'))""").first()
print(f"  sort rewrote {res['rewritten_data_files_count']} files")

# Clustering is visible in the per-file bounds: how many files can hold 'APAC'?
total_f = file_count(spark, "t_maint")
print(f"\n  data files after sort: {total_f}")
print("  With region clustered, a WHERE region = 'APAC' query can skip every")
print("  file whose region bounds exclude APAC. Before sorting, every file")
print("  held a mix of all three regions and none could be skipped.")

sql(spark, f"""
    SELECT record_count, file_size_in_bytes, lower_bounds[2] AS region_low,
           upper_bounds[2] AS region_high
    FROM {meta('files', 't_maint')} WHERE content = 0 LIMIT 8""",
    label="per-file region bounds after sorting (column id 2 = region)",
    truncate=False)

# ===========================================================================
block("04", "REWRITE MANIFESTS",
      "the data files are fine; the metadata listing them is fragmented")
# ===========================================================================
man_before = spark.sql(
    f"SELECT count(*) FROM {meta('manifests', 't_maint')}").first()[0]
spark.sql(f"CALL {C.CATALOG}.system.rewrite_manifests('{C.DB}.t_maint')")
man_after = spark.sql(
    f"SELECT count(*) FROM {meta('manifests', 't_maint')}").first()[0]
print(f"  manifests {man_before} -> {man_after}")
print("  This touches no data at all. It only makes scan PLANNING cheaper.")

# ===========================================================================
block("05", "EXPIRE SNAPSHOTS",
      "the only operation that actually frees storage - and it ends time travel")
# ===========================================================================
snaps_before = snapshot_ids(spark, "t_maint")
all_files_before = spark.sql(
    f"SELECT count(DISTINCT file_path) FROM {meta('all_data_files', 't_maint')}"
).first()[0]
live_files = file_count(spark, "t_maint")
oldest = snaps_before[0]

print(f"  snapshots ............................ {len(snaps_before)}")
print(f"  files the current snapshot needs ..... {live_files}")
print(f"  distinct files kept alive by history . {all_files_before}")
print(f"  pure history overhead ................ {all_files_before - live_files}")

# Prove time travel to the oldest snapshot works BEFORE we expire it
old_rows = spark.sql(f"SELECT count(*) FROM {M} VERSION AS OF {oldest}").first()[0]
print(f"\n  time travel to the oldest snapshot works: {old_rows} rows")

spark.sql(f"""
    CALL {C.CATALOG}.system.expire_snapshots(
        table       => '{C.DB}.t_maint',
        older_than  => TIMESTAMP '2099-01-01 00:00:00.000',
        retain_last => 1)""")

snaps_after = snapshot_ids(spark, "t_maint")
all_files_after = spark.sql(
    f"SELECT count(DISTINCT file_path) FROM {meta('all_data_files', 't_maint')}"
).first()[0]
print(f"\n  snapshots {len(snaps_before)} -> {len(snaps_after)}")
print(f"  distinct files {all_files_before} -> {all_files_after}"
      f"   ({all_files_before - all_files_after} deleted from storage)")

try:
    spark.sql(f"SELECT count(*) FROM {M} VERSION AS OF {oldest}").first()
    expired_unreachable = False
except Exception as e:
    expired_unreachable = True
    print(f"\n  time travel to the EXPIRED snapshot now fails:")
    print(f"    {str(e).splitlines()[0][:100]}")
print("  Storage saved and history lost are the same transaction. Choose your")
print("  retention deliberately - it is a data-recovery policy, not cleanup.")

# ===========================================================================
block("06", "ORPHAN FILES",
      "written by failed jobs; NO metadata references them, so expiry misses them")
# ===========================================================================
if C.MODE == "local_iceberg":
    data_dir = os.path.join(warehouse_dir("t_maint"), "data")
    plant = None
    for dirpath, _, files in os.walk(data_dir):
        plant = os.path.join(dirpath, "00000-999-orphan-from-a-failed-job.parquet")
        with open(plant, "wb") as fh:
            fh.write(b"PAR1this file is garbage from a crashed executorPAR1")
        break

    # Iceberg REFUSES to remove orphans younger than 24 hours - deleting a file
    # a concurrent writer is still committing would corrupt the table. Age the
    # planted file by a week so it is a legitimate candidate.
    week_ago = datetime.now() - timedelta(days=7)
    os.utime(plant, (week_ago.timestamp(), week_ago.timestamp()))
    cutoff = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d %H:%M:%S")
    print(f"  planted an orphan: .../{os.path.basename(plant)}")
    print(f"  aged it 7 days; using older_than = {cutoff}")

    tracked = spark.sql(
        f"SELECT count(*) FROM {meta('files', 't_maint')}").first()[0]
    print(f"  metadata still tracks {tracked} files - the orphan is invisible to it")

    dry = spark.sql(f"""
        CALL {C.CATALOG}.system.remove_orphan_files(
            table      => '{C.DB}.t_maint',
            older_than => TIMESTAMP '{cutoff}',
            dry_run    => true)""").collect()
    print(f"  dry_run found {len(dry)} orphan(s) - ALWAYS do this first")

    spark.sql(f"""
        CALL {C.CATALOG}.system.remove_orphan_files(
            table      => '{C.DB}.t_maint',
            older_than => TIMESTAMP '{cutoff}',
            dry_run    => false)""")
    orphan_gone = not os.path.exists(plant)
    print(f"  orphan removed from storage: {orphan_gone}")
else:
    orphan_gone, dry = True, [1]
    print("  (orphan planting is local_iceberg only)")

# ===========================================================================
block("07", "WRITE-AUDIT-PUBLISH",
      "write into the real table, audit it there, publish only if it passes")
# ===========================================================================
W = fq("t_wap")
spark.sql(f"DROP TABLE IF EXISTS {W} PURGE")
spark.sql(f"""
    CREATE TABLE {W} (order_id BIGINT, region STRING, order_amount DECIMAL(10,2))
    USING iceberg TBLPROPERTIES ('format-version' = '2')""")
spark.sql(f"INSERT INTO {W} VALUES (1,'NA',10.00),(2,'EMEA',20.00),(3,'APAC',30.00)")
main_before = spark.table(W).count()
print(f"  production table starts with {main_before} clean rows")

# --- branch off -----------------------------------------------------------
spark.sql(f"ALTER TABLE {W} CREATE BRANCH etl_branch")
spark.sql(f"ALTER TABLE {W} SET TBLPROPERTIES ('write.wap.enabled'='true')")
spark.conf.set("spark.wap.branch", "etl_branch")
print("  created etl_branch and bound THIS SESSION to it via spark.wap.branch")

# --- the unchanged ETL runs; it lands on the branch, not main -------------
spark.sql(f"""INSERT INTO {W} VALUES
    (4,'NA',40.00), (5,NULL,50.00), (5,NULL,50.00), (6,'EMEA',-99.00)""")
print("  ETL wrote 4 rows - including a NULL region, a duplicate, a negative")

on_branch = spark.sql(f"SELECT count(*) FROM {W} VERSION AS OF 'etl_branch'").first()[0]
on_main = spark.sql(f"SELECT count(*) FROM {W} VERSION AS OF 'main'").first()[0]
print(f"  rows on etl_branch: {on_branch}")
print(f"  rows on main      : {on_main}   <- production has not moved")

sql(spark, f"SELECT name, type, snapshot_id FROM {meta('refs', 't_wap')}",
    label="refs - main and the branch point at different snapshots", truncate=False)

# --- audit ---------------------------------------------------------------
nulls = spark.sql(
    f"SELECT count(*) FROM {W} VERSION AS OF 'etl_branch' WHERE region IS NULL"
).first()[0]
negs = spark.sql(
    f"SELECT count(*) FROM {W} VERSION AS OF 'etl_branch' WHERE order_amount < 0"
).first()[0]
dupes = spark.sql(f"""
    SELECT count(*) FROM (
      SELECT order_id FROM {W} VERSION AS OF 'etl_branch'
      GROUP BY order_id HAVING count(*) > 1)""").first()[0]
print(f"\n  AUDIT   null regions: {nulls}   negative amounts: {negs}"
      f"   duplicate ids: {dupes}")
audit_failed = (nulls or negs or dupes)
print(f"  verdict: {'FAIL - do not publish' if audit_failed else 'pass'}")

# --- the audit failed, so we DON'T publish. Throw the branch away. -------
spark.conf.unset("spark.wap.branch")
spark.sql(f"ALTER TABLE {W} DROP BRANCH etl_branch")
main_after_drop = spark.table(W).count()
print(f"\n  dropped etl_branch. production rows: {main_after_drop}"
      f"   (never saw one bad row)")

# --- now the corrected run, which passes and gets published --------------
spark.sql(f"ALTER TABLE {W} CREATE BRANCH etl_branch")
spark.conf.set("spark.wap.branch", "etl_branch")
spark.sql(f"INSERT INTO {W} VALUES (4,'NA',40.00), (5,'APAC',50.00)")
clean_nulls = spark.sql(
    f"SELECT count(*) FROM {W} VERSION AS OF 'etl_branch' WHERE region IS NULL"
).first()[0]
print(f"  corrected run on a fresh branch. null regions now: {clean_nulls}")

branch_snap = spark.sql(
    f"SELECT snapshot_id FROM {meta('refs', 't_wap')} WHERE name='etl_branch'"
).first()[0]
spark.conf.unset("spark.wap.branch")

spark.sql(f"""CALL {C.CATALOG}.system.cherrypick_snapshot(
    '{C.DB}.t_wap', {branch_snap})""")
published = spark.table(W).count()
print(f"  cherrypick_snapshot published it. production rows: "
      f"{main_before} -> {published}")
print("  That was a metadata-only operation. No data file moved.")

# --- tag the release ------------------------------------------------------
spark.sql(f"ALTER TABLE {W} CREATE TAG `release_2026_09` RETAIN 30 DAYS")
tags = spark.sql(
    f"SELECT count(*) FROM {meta('refs', 't_wap')} WHERE type='TAG'").first()[0]
print(f"  tagged the published state; tags on the table: {tags}")

# ===========================================================================
block("08", "VERIFY")
# ===========================================================================
check(files_binpack < files_small,
      f"binpack compaction reduced data files {files_small} -> {files_binpack}")
check(spark.table(M).count() == rows_m,
      f"compaction preserved every row ({rows_m})")
check(man_after <= man_before,
      f"rewrite_manifests consolidated metadata ({man_before} -> {man_after})")
check(len(snaps_after) == 1,
      f"expire_snapshots left exactly the retained snapshot "
      f"({len(snaps_before)} -> {len(snaps_after)})")
check(all_files_after < all_files_before,
      f"expiry physically deleted {all_files_before - all_files_after} data files")
check(expired_unreachable,
      "time travel to an expired snapshot now fails - storage and history go together")
check(len(dry) >= 1, "the orphan was invisible to metadata but found by dry_run")
check(orphan_gone, "remove_orphan_files deleted the planted orphan")
check(on_main == main_before,
      f"WAP: production stayed at {main_before} rows while the branch was written")
check(on_branch == main_before + 4,
      f"WAP: the branch received all 4 ETL rows ({on_branch})")
check(bool(audit_failed), "the audit correctly caught the planted defects")
check(main_after_drop == main_before,
      "dropping the failed branch left production untouched")
check(published == main_before + 2,
      f"cherrypick published only the CORRECTED run ({published} rows)")
check(tags == 1, "the published state is tagged for reproducibility")

print("\nMaintenance and WAP complete. That is the whole suite.\n")
spark.stop()
