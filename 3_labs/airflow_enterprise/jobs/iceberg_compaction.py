"""
iceberg_compaction.py — called by DAG 02 (compact_partitions) and DAG 05
(rewrite_data_files).

    spark-submit iceberg_compaction.py \
        --table glue_catalog.telco_prod_gold.ran_cell_kpi \
        --strategy sort --sort-by region,cell_id \
        --rewrite-position-deletes true

THE BEHAVIOUR THIS SCRIPT EXISTS TO GET RIGHT

`rewrite_data_files` on its own does NOT reconcile merge-on-read delete files.
The iceberg_deep lab proves it: run a plain compaction on a table with
position deletes and the delete files are still there afterwards, so every
reader still pays to apply them. Read cost keeps climbing while the compaction
job reports success every night.

Two things fix it, and this script does both:

    delete-file-threshold   makes the planner consider a file worth rewriting
                            because of the deletes attached to it, not only
                            because of its size
    rewrite_position_delete_files   a separate procedure, called after

There is also a trap in the other direction: `rewrite-all` forces every file to
be rewritten regardless of size. That is correct for a GDPR erasure, where the
point is to physically drop rows, and wasteful for nightly hygiene, where most
files are already fine. DAG 05 passes it, DAG 02 does not.

THE THREE STRATEGIES
    binpack   just coalesce small files. Cheapest. Fixes file COUNT.
    sort      cluster rows by the given columns. Costs a shuffle, and pays for
              itself on every query that filters those columns.
    zorder    multi-dimensional clustering, when queries filter on several
              columns in unpredictable combinations.
"""
from __future__ import annotations

import job_common as J


def main() -> None:
    p = J.base_args(__doc__)
    p.add_argument("--table", help="fully qualified table")
    p.add_argument("--tables-from-xcom", default=None,
                   help="comma-separated list; the DAG passes several tables at once")
    p.add_argument("--strategy", default="binpack", choices=("binpack", "sort", "zorder"))
    p.add_argument("--sort-by", default=None, help="columns for sort/zorder")
    p.add_argument("--target-file-size-mb", type=int, default=512)
    p.add_argument("--min-input-files", type=int, default=5)
    p.add_argument("--delete-file-threshold", type=int, default=2,
                   help="rewrite a data file once this many delete files point at it")
    p.add_argument("--rewrite-all", default="false")
    p.add_argument("--rewrite-position-deletes", default="false")
    args = p.parse_args()

    targets = (
        [t.strip() for t in args.tables_from_xcom.split(",") if t.strip()]
        if args.tables_from_xcom else [args.table]
    )
    if not targets or targets == [None]:
        J.fail("pass --table or --tables-from-xcom")

    spark = J.build_spark(f"iceberg-compaction-{args.strategy}")
    rewrite_all = str(args.rewrite_all).lower() == "true"
    do_deletes = str(args.rewrite_position_deletes).lower() == "true"
    target_bytes = args.target_file_size_mb * 1024 * 1024

    for table in targets:
        if not J.table_exists(spark, table):
            J.step(f"skip {table}: does not exist")
            continue

        before_data = J.data_file_count(spark, table)
        before_del = J.delete_file_count(spark, table)
        J.banner(f"{table}: {before_data} data files, {before_del} delete files "
                 f"| strategy={args.strategy} rewrite_all={rewrite_all}")

        opts = [
            f"'target-file-size-bytes','{target_bytes}'",
            f"'min-input-files','{args.min_input_files}'",
            # THE line that makes compaction see delete files at all.
            f"'delete-file-threshold','{args.delete_file_threshold}'",
            "'partial-progress.enabled','true'",     # commit as it goes; survives a spot loss
            "'partial-progress.max-commits','10'",
            "'max-concurrent-file-group-rewrites','10'",
        ]
        if rewrite_all:
            opts.append("'rewrite-all','true'")

        sort_clause = ""
        if args.strategy in ("sort", "zorder"):
            if not args.sort_by:
                J.fail(f"--strategy {args.strategy} needs --sort-by")
            cols = ",".join(c.strip() for c in args.sort_by.split(","))
            if args.strategy == "zorder":
                sort_clause = f", strategy => 'sort', sort_order => 'zorder({cols})'"
            else:
                sort_clause = f", strategy => 'sort', sort_order => '{cols}'"

        res = spark.sql(f"""
            CALL {J.CATALOG}.system.rewrite_data_files(
                table => '{table}',
                options => map({','.join(opts)})
                {sort_clause}
            )
        """).collect()[0]
        J.step(f"rewrite_data_files: rewritten_files={res[0]} added_files={res[1]} "
               f"rewritten_bytes={res[2]}")

        # The second half. Without this the delete files survive compaction.
        if do_deletes and before_del > 0:
            dres = spark.sql(f"""
                CALL {J.CATALOG}.system.rewrite_position_delete_files(
                    table => '{table}',
                    options => map('rewrite-all','{str(rewrite_all).lower()}')
                )
            """).collect()[0]
            J.step(f"rewrite_position_delete_files: rewritten={dres[0]} added={dres[1]}")

        after_data = J.data_file_count(spark, table)
        after_del = J.delete_file_count(spark, table)
        J.step(f"{table}: data files {before_data} -> {after_data}, "
               f"delete files {before_del} -> {after_del}")

        if do_deletes and after_del >= before_del and before_del > 0:
            # Not fatal — MoR convergence can take more than one pass — but it
            # must be visible, because this is exactly the case that hides.
            J.step("WARNING: delete files did not decrease. If this repeats across "
                   "runs, raise --delete-file-threshold or pass --rewrite-all true.")

        J.describe_commit(spark, table, "after compaction")

    J.banner(f"compaction complete on {len(targets)} table(s)")
    spark.stop()


if __name__ == "__main__":
    main()
