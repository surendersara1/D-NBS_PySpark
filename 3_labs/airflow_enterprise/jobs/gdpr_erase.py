"""
gdpr_erase.py — called by DAG 05, mapped task erasure/delete_rows (EMR on EKS)

Erases a set of subscribers from ONE Iceberg table. One mapped task, one table,
one invocation of this script.

    spark-submit gdpr_erase.py \
        --table glue_catalog.telco_prod_silver.cdr_events \
        --key-column subscriber_id \
        --write-mode merge-on-read \
        --subscriber-ids 4471234567,4477654321

WHY THE WRITE MODE ARGUMENT EXISTS

    copy-on-write   DELETE rewrites every data file that contained a matching
                    row. Expensive to write, free to read, and the rows are
                    physically gone from the new files immediately.
    merge-on-read   DELETE writes a small position-delete file instead. Cheap
                    to write, and the row is STILL PHYSICALLY PRESENT in the
                    data file. It is filtered out at read time.

For GDPR the difference is the whole point. A merge-on-read delete has not
erased anything yet — it has only hidden it. That is why DAG 05 always follows
this job with a forced rewrite and then snapshot expiry. This script reports
which mode the table used so the evidence is in the task log.

It also writes an audit row per table BEFORE deleting, so there is a record of
what was targeted even if the erasure later fails.
"""
from __future__ import annotations

from pyspark.sql import functions as F

import job_common as J


def main() -> None:
    p = J.base_args(__doc__)
    p.add_argument("--table", required=True)
    p.add_argument("--key-column", default="subscriber_id")
    p.add_argument("--write-mode", default="copy-on-write",
                   choices=("copy-on-write", "merge-on-read"))
    p.add_argument("--subscriber-ids", required=True, help="comma separated")
    p.add_argument("--audit-table", default=None)
    args = p.parse_args()

    ids = [s.strip() for s in args.subscriber_ids.split(",") if s.strip()]
    if not ids:
        J.step("no subscriber ids passed — nothing to erase")
        return

    spark = J.build_spark(f"gdpr-erase-{args.table.split('.')[-1]}")
    table, key = args.table, args.key_column

    if not J.table_exists(spark, table):
        J.fail(f"{table} does not exist — the erasure scope is stale, fix the catalog tags")

    # --------------------------------------------------------------- 1. what mode is it really
    # Trust the table, not the argument. Table properties are the truth; the
    # catalog tag the DAG read may be out of date.
    props = {r["key"]: r["value"] for r in
             spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}
    actual_mode = props.get("write.delete.mode", "copy-on-write")
    if actual_mode != args.write_mode:
        J.step(f"NOTE: catalog said {args.write_mode}, table property says "
               f"{actual_mode}. Using the table.")
    J.banner(f"{table}: erasing {len(ids)} subscribers | delete mode = {actual_mode}")

    # --------------------------------------------------------------- 2. count first
    id_list = ", ".join(f"'{i}'" for i in ids)
    before = spark.sql(
        f"SELECT count(*) c FROM {table} WHERE {key} IN ({id_list})"
    ).collect()[0]["c"]
    J.step(f"rows matching the erasure request: {before:,}")

    if before == 0:
        J.step("nothing to delete in this table")
        spark.stop()
        return

    snap_before = J.snapshot_id(spark, table)
    files_before = J.data_file_count(spark, table)
    dels_before = J.delete_file_count(spark, table)

    # --------------------------------------------------------------- 3. audit BEFORE
    audit = args.audit_table or f"{J.CATALOG}.{table.split('.')[1]}.gdpr_erasure_audit"
    if J.table_exists(spark, audit):
        (spark.createDataFrame(
            [(table, key, len(ids), int(before), actual_mode, str(snap_before))],
            "target_table string, key_column string, subscriber_count int, "
            "rows_targeted int, delete_mode string, snapshot_before string")
         .withColumn("erased_at", F.current_timestamp())
         .writeTo(audit).append())
        J.step(f"audit row written to {audit}")

    # --------------------------------------------------------------- 4. delete
    spark.sql(f"DELETE FROM {table} WHERE {key} IN ({id_list})")

    after = spark.sql(
        f"SELECT count(*) c FROM {table} WHERE {key} IN ({id_list})"
    ).collect()[0]["c"]
    if after != 0:
        J.fail(f"{after:,} rows still visible after DELETE — erasure did not apply")

    files_after = J.data_file_count(spark, table)
    dels_after = J.delete_file_count(spark, table)
    J.describe_commit(spark, table, "after DELETE")
    J.step(f"data files {files_before} -> {files_after}, "
           f"delete files {dels_before} -> {dels_after}")

    # --------------------------------------------------------------- 5. tell the truth
    if actual_mode == "merge-on-read" and dels_after > dels_before:
        J.step(
            "MERGE-ON-READ: the rows are hidden by position-delete files but are "
            "STILL PHYSICALLY PRESENT in the data files. This table is NOT yet "
            "erased. The rewrite and expire_snapshots steps that follow in DAG 05 "
            "are what actually destroy the bytes."
        )
    else:
        J.step(
            "COPY-ON-WRITE: rows are gone from the new data files. The OLD files "
            "still exist and are still reachable by time travel until "
            "expire_snapshots and remove_orphan_files run."
        )

    J.banner(f"{table}: {before:,} rows deleted, snapshot "
             f"{snap_before} -> {J.snapshot_id(spark, table)}")
    spark.stop()


if __name__ == "__main__":
    main()
