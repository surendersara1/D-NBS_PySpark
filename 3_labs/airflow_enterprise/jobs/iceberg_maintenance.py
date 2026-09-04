"""
iceberg_maintenance.py — called by DAG 05, tasks expire_snapshots,
remove_orphan_files and rewrite_manifests.

One script, three actions, selected with --action. Runs across every table in
the named databases.

    spark-submit iceberg_maintenance.py --action expire_snapshots \
        --older-than-hours 168 --retain-last 5 \
        --databases telco_prod_bronze,telco_prod_silver,telco_prod_gold

THE PART THAT MATTERS LEGALLY

DAG 05 runs these AFTER the GDPR erasure and its data-file rewrite, and the
order is not a preference:

    DELETE            new snapshot without the rows; the OLD snapshot still
                      references data files that physically contain them
    rewrite_data_files   rows are physically gone from the NEW files
    expire_snapshots     nothing references the OLD files any more
    remove_orphan_files  the OLD files are finally deleted from S3

Run expire before the rewrite and you have expired the wrong snapshots. Skip
orphan removal and the bytes are still in the bucket. Either way you have told
a regulator you erased something you did not.

THE 24-HOUR RULE

remove_orphan_files refuses an interval under 24 hours by default, and the
guard exists for a good reason: a file written by a job that has not committed
yet looks exactly like an orphan. Deleting it corrupts a live write. This
script will not let you lower it below 24h without --i-understand-the-risk,
which is deliberately annoying.
"""
from __future__ import annotations

import job_common as J


ACTIONS = ("expire_snapshots", "remove_orphan_files", "rewrite_manifests")


def tables_in(spark, database: str) -> list[str]:
    rows = spark.sql(f"SHOW TABLES IN {J.CATALOG}.{database}").collect()
    return [f"{J.CATALOG}.{database}.{r['tableName']}" for r in rows]


def expire(spark, table: str, older_than_hours: int, retain_last: int) -> None:
    ts = spark.sql(
        f"SELECT CAST(current_timestamp() - INTERVAL {older_than_hours} HOURS AS STRING) t"
    ).collect()[0]["t"]
    res = spark.sql(f"""
        CALL {J.CATALOG}.system.expire_snapshots(
            table => '{table}',
            older_than => TIMESTAMP '{ts}',
            retain_last => {retain_last}
        )
    """).collect()[0]
    J.step(f"{table}: expired -> data_files={res[0]} "
           f"position_delete_files={res[1]} equality_delete_files={res[2]} "
           f"manifests={res[3]} manifest_lists={res[4]}")


def orphans(spark, table: str, older_than_hours: int, dry_run: bool) -> None:
    ts = spark.sql(
        f"SELECT CAST(current_timestamp() - INTERVAL {older_than_hours} HOURS AS STRING) t"
    ).collect()[0]["t"]
    res = spark.sql(f"""
        CALL {J.CATALOG}.system.remove_orphan_files(
            table => '{table}',
            older_than => TIMESTAMP '{ts}',
            dry_run => {str(dry_run).lower()}
        )
    """).collect()
    verb = "would remove" if dry_run else "removed"
    J.step(f"{table}: {verb} {len(res)} orphan files")
    for r in res[:5]:
        J.step(f"    {r[0]}")


def manifests(spark, table: str) -> None:
    """Rewrite manifests so metadata reads prune well again.

    After thousands of small commits the manifest list is long and each
    manifest covers a scattered set of partitions, so planning gets slow even
    though the DATA is fine. This is the cheap fix people forget exists.
    """
    before = spark.sql(f"SELECT count(*) c FROM {table}.manifests").collect()[0]["c"]
    res = spark.sql(
        f"CALL {J.CATALOG}.system.rewrite_manifests(table => '{table}')"
    ).collect()[0]
    after = spark.sql(f"SELECT count(*) c FROM {table}.manifests").collect()[0]["c"]
    J.step(f"{table}: manifests {before} -> {after} "
           f"(rewritten={res[0]} added={res[1]})")


def main() -> None:
    p = J.base_args(__doc__)
    p.add_argument("--action", required=True, choices=ACTIONS)
    p.add_argument("--databases", required=True, help="comma separated")
    p.add_argument("--older-than-hours", type=int, default=168)
    p.add_argument("--retain-last", type=int, default=5)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--i-understand-the-risk", action="store_true",
                   help="required to set --older-than-hours below 24 for orphan removal")
    args = p.parse_args()

    if (args.action == "remove_orphan_files"
            and args.older_than_hours < 24
            and not args.i_understand_the_risk):
        J.fail(
            f"remove_orphan_files with older_than_hours={args.older_than_hours} can delete "
            "files belonging to a write that has not committed yet. Iceberg's own default "
            "minimum is 24h. Pass --i-understand-the-risk only if all writers are stopped."
        )

    spark = J.build_spark(f"iceberg-maintenance-{args.action}")
    dbs = [d.strip() for d in args.databases.split(",") if d.strip()]

    all_tables: list[str] = []
    for db in dbs:
        found = tables_in(spark, db)
        J.step(f"{db}: {len(found)} tables")
        all_tables.extend(found)

    J.banner(f"{args.action} across {len(all_tables)} tables in {len(dbs)} databases")

    failures: list[tuple[str, str]] = []
    for t in all_tables:
        try:
            if args.action == "expire_snapshots":
                expire(spark, t, args.older_than_hours, args.retain_last)
            elif args.action == "remove_orphan_files":
                orphans(spark, t, args.older_than_hours, args.dry_run)
            else:
                manifests(spark, t)
        except Exception as exc:
            # One unmaintainable table must not stop maintenance of the other
            # 60 — but every failure is collected and the job still exits
            # non-zero at the end so Airflow shows it.
            J.step(f"ERROR on {t}: {exc}")
            failures.append((t, str(exc)[:200]))

    if failures:
        for t, e in failures:
            print(f"  FAILED {t}: {e}")
        J.fail(f"{len(failures)} of {len(all_tables)} tables failed {args.action}")

    J.banner(f"{args.action} complete on {len(all_tables)} tables")
    spark.stop()


if __name__ == "__main__":
    main()
