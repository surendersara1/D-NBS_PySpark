"""
04_cow_mor_deletes.py  —  copy-on-write vs merge-on-read, and the delete files
                          that make MoR possible.

Atlas chapter 09.

    python 04_cow_mor_deletes.py

What you should take away:
  * Data files are immutable, so "update one row" ALWAYS writes something new.
    The only choice is what: a whole new file, or a small delete file.
  * You can see delete files in .files - content 1 = position, 2 = equality.
  * MoR is not free. Read cost grows until compaction reconciles the deletes.
"""
import config as C
from common import (get_spark, fq, meta, block, sql, show, check,
                    file_count, delete_file_count)

spark = get_spark("04_cow_mor")

# We build two IDENTICAL tables and set them to opposite modes, so the only
# variable is the write mode. Same rows, same partitioning, same operations.
COW, MOR = fq("t_cow"), fq("t_mor")


def build(table, mode):
    spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
    spark.sql(f"""
        CREATE TABLE {table} (
            order_id BIGINT, region STRING, status STRING,
            order_amount DECIMAL(10,2), order_ts TIMESTAMP)
        USING iceberg
        PARTITIONED BY (days(order_ts))
        TBLPROPERTIES (
            'format-version'    = '2',
            'write.delete.mode' = '{mode}',
            'write.update.mode' = '{mode}',
            'write.merge.mode'  = '{mode}')""")
    spark.sql(f"""
        INSERT INTO {table}
        SELECT order_id, region, status, order_amount, order_ts
        FROM {fq('orders')} WHERE order_id < 1120""")


# ===========================================================================
block("01", "TWO TABLES, ONE DIFFERENCE",
      "identical data and layout; only the three write modes differ")
# ===========================================================================
build(COW, "copy-on-write")
build(MOR, "merge-on-read")

cow_rows = spark.table(COW).count()
mor_rows = spark.table(MOR).count()
print(f"  {COW}: {cow_rows} rows, {file_count(spark, 't_cow')} data files")
print(f"  {MOR}: {mor_rows} rows, {file_count(spark, 't_mor')} data files")

for t, lbl in ((COW, "copy-on-write table"), (MOR, "merge-on-read table")):
    props = (spark.sql(f"SHOW TBLPROPERTIES {t}")
             .filter("key LIKE 'write.%.mode'").orderBy("key"))
    show(props, label=f"{lbl} - the three modes", truncate=False)

cow_files_0 = file_count(spark, "t_cow")
mor_files_0 = file_count(spark, "t_mor")

# ===========================================================================
block("02", "DELETE ONE ROW FROM EACH",
      "same statement, completely different physical result")
# ===========================================================================
spark.sql(f"DELETE FROM {COW} WHERE order_id = 1005")
spark.sql(f"DELETE FROM {MOR} WHERE order_id = 1005")

cow_files_1 = file_count(spark, "t_cow")
mor_files_1 = file_count(spark, "t_mor")
cow_pos, cow_eq = delete_file_count(spark, "t_cow")
mor_pos, mor_eq = delete_file_count(spark, "t_mor")

print(f"  COPY-ON-WRITE")
print(f"    data files   {cow_files_0} -> {cow_files_1}"
      f"   (the file holding the row was REWRITTEN without it)")
print(f"    delete files {cow_pos} position, {cow_eq} equality  <- none, ever")
print(f"  MERGE-ON-READ")
print(f"    data files   {mor_files_0} -> {mor_files_1}"
      f"   (untouched - nothing was rewritten)")
print(f"    delete files {mor_pos} position, {mor_eq} equality  <- the new side-file")

# ===========================================================================
block("03", "LOOK AT THE DELETE FILE ITSELF",
      "content: 0 = data, 1 = position delete, 2 = equality delete")
# ===========================================================================
sql(spark, f"""
    SELECT content, file_path, record_count, file_size_in_bytes
    FROM {meta('files', 't_mor')} ORDER BY content DESC LIMIT 5""",
    label="the MoR table now tracks a file that contains no table data at all",
    truncate=False)

print("  A position delete file holds (file_path, pos) pairs - 'skip row N of")
print("  that exact file'. Writing it required READING the data file first to")
print("  learn the position. That is MoR's write-side cost.")
print()
print("  Equality deletes (content = 2) record a predicate instead and cost")
print("  nothing to write - but every read must then compare every candidate")
print("  row. Spark writes POSITION deletes; equality deletes come from")
print("  streaming upserts (Flink). You will normally see content = 1 here.")

# ===========================================================================
block("04", "BOTH TABLES STILL RETURN THE SAME ANSWER",
      "the delete file is reconciled at read time, invisibly")
# ===========================================================================
cow_after = spark.table(COW).count()
mor_after = spark.table(MOR).count()
cow_gone = spark.sql(f"SELECT count(*) FROM {COW} WHERE order_id = 1005").first()[0]
mor_gone = spark.sql(f"SELECT count(*) FROM {MOR} WHERE order_id = 1005").first()[0]
print(f"  rows: CoW {cow_after}   MoR {mor_after}   (identical)")
print(f"  order 1005 visible: CoW {cow_gone}   MoR {mor_gone}   (both gone)")
print("  Correctness is identical. Only the physics differ.")

# ===========================================================================
block("05", "NOW DO IT NINE MORE TIMES",
      "this is where the two strategies really separate")
# ===========================================================================
for oid in range(1006, 1015):
    spark.sql(f"DELETE FROM {COW} WHERE order_id = {oid}")
    spark.sql(f"DELETE FROM {MOR} WHERE order_id = {oid}")

cow_files_n = file_count(spark, "t_cow")
mor_files_n = file_count(spark, "t_mor")
cow_pos_n, _ = delete_file_count(spark, "t_cow")
mor_pos_n, _ = delete_file_count(spark, "t_mor")

print(f"  after 10 single-row deletes:")
print(f"    CoW  data files {cow_files_0} -> {cow_files_n},"
      f"  delete files {cow_pos_n}")
print(f"    MoR  data files {mor_files_0} -> {mor_files_n},"
      f"  delete files {mor_pos_n}")
print()
print("  CoW rewrote a full data file on EVERY delete - 10 rewrites of files")
print("  that mostly contained rows nobody asked to remove.")
print("  MoR left the data alone and accumulated delete files instead. Reads")
print("  now have to reconcile all of them, on every single query.")

# ===========================================================================
block("06", "COMPACTION IS WHAT PAYS OFF THE MoR DEBT",
      "rewrite_data_files is the ONLY thing that reconciles delete files away")
# ===========================================================================
print(f"  before compaction: {mor_files_n} data files, {mor_pos_n} delete files")

# --- attempt 1: the compaction somebody actually scheduled. The trap. -----
spark.sql(f"""
    CALL {C.CATALOG}.system.rewrite_data_files(table => '{C.DB}.t_mor')""")

naive_files, (naive_pos, _) = (file_count(spark, "t_mor"),
                               delete_file_count(spark, "t_mor"))
print(f"  after a PLAIN rewrite_data_files: "
      f"{naive_files} data files, {naive_pos} delete files")
print()
print("  Nothing happened - and this is the most surprising thing about MoR")
print("  maintenance. binpack only rewrites files it considers badly sized.")
print("  These files are already fine, so having delete files pointed at them")
print("  is, on its own, not a reason to rewrite. The debt just sits there.")
print()

# --- attempt 2: force it, and keep going until it settles ----------------
print("  with rewrite-all => true:")
for attempt in range(1, 4):
    res = spark.sql(f"""
        CALL {C.CATALOG}.system.rewrite_data_files(
            table   => '{C.DB}.t_mor',
            options => map('rewrite-all','true'))""").first()
    pos_now, _ = delete_file_count(spark, "t_mor")
    print(f"    pass {attempt}: rewrote {res['rewritten_data_files_count']} files,"
          f" added {res['added_data_files_count']}"
          f"  ->  {file_count(spark, 't_mor')} data files,"
          f" {pos_now} delete files")
    if pos_now == 0:
        break

mor_files_c = file_count(spark, "t_mor")
mor_pos_c, mor_eq_c = delete_file_count(spark, "t_mor")
mor_rows_c = spark.table(MOR).count()
print(f"    rows still correct: {mor_rows_c}")
print()
print("  Note it took more than one pass. Compaction CONVERGES on a clean")
print("  table rather than reaching it in a single shot - a delete file")
print("  committed at a sequence number above the files being rewritten")
print("  survives that pass and is cleared by the next one.")
print()
print("  NOW the deleted rows are physically absent from the rewritten data")
print("  files, so the delete files have nothing left to point at and drop out")
print("  of the new snapshot. Read cost returns to CoW levels.")
print()
print("  In production you would not use rewrite-all on a big table. You would")
print("  scope it - where => 'day = ...' - and tune delete-file-threshold so")
print("  files carrying real delete debt become eligible on their own. The")
print("  lesson stands either way: MoR without a compaction schedule that")
print("  actually rewrites is a trap, not a strategy.")

# ===========================================================================
block("07", "METADATA-ONLY DELETE",
      "when the predicate lines up with a partition, NO file is touched at all")
# ===========================================================================
before_meta_del = file_count(spark, "t_cow")
part_rows = spark.sql(f"""
    SELECT count(*) FROM {COW}
    WHERE order_ts >= TIMESTAMP '2026-03-01 00:00:00'
      AND order_ts <  TIMESTAMP '2026-03-02 00:00:00'""").first()[0]

spark.sql(f"""
    DELETE FROM {COW}
    WHERE order_ts >= TIMESTAMP '2026-03-01 00:00:00'
      AND order_ts <  TIMESTAMP '2026-03-02 00:00:00'""")

after_meta_del = file_count(spark, "t_cow")
print(f"  dropped a whole day partition ({part_rows} rows)")
print(f"  data files {before_meta_del} -> {after_meta_del}")
print("  Iceberg just stopped listing those files. No Parquet was read or")
print("  rewritten - the whole delete happened in metadata.")

# ===========================================================================
block("08", "VERIFY")
# ===========================================================================
check(cow_pos == 0 and cow_eq == 0,
      "a copy-on-write table produced NO delete files")
check(mor_pos >= 1,
      f"a merge-on-read table produced {mor_pos} position delete file(s)")
check(mor_files_1 == mor_files_0,
      f"MoR rewrote zero data files on delete (still {mor_files_0})")
check(cow_after == mor_after == cow_rows - 1,
      "both tables return exactly the same rows after the delete")
check(cow_gone == 0 and mor_gone == 0,
      "the deleted row is invisible in both, delete file reconciled on read")
check(mor_pos_n > mor_pos,
      f"delete files accumulate as MoR deletes repeat ({mor_pos} -> {mor_pos_n})")
check(naive_pos == mor_pos_n,
      f"a PLAIN compaction reconciled nothing - delete files still {naive_pos}")
check(mor_pos_c == 0,
      f"rewrite-all reconciled every delete file away ({mor_pos_n} -> 0)")
check(mor_rows_c == cow_rows - 10,
      f"compaction preserved correctness ({mor_rows_c} rows)")
check(after_meta_del < before_meta_del,
      "a partition-aligned DELETE removed whole files without rewriting any")

print("\nRow-level operations complete. Next: 05_maintenance_wap.py\n")
spark.stop()
