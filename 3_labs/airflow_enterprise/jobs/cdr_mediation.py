"""
cdr_mediation.py — called by DAG 01, task bronze/silver `mediate-<market>`

Reads one market's raw call detail records for one hour, deduplicates them,
rates them, and MERGEs the result into the silver Iceberg table.

    spark-submit --deploy-mode cluster cdr_mediation.py \
        --market HU --run-date 2026-09-03 --run-hour 14 \
        --late-window-hours 6 \
        --source s3://telco-prod-raw/cdr/HU/ \
        --target-table glue_catalog.telco_prod_silver.cdr_events

THE THREE PROBLEMS THIS JOB EXISTS TO SOLVE

1. DUPLICATES. A switch that fails mid-write replays its whole file. The same
   cdr_id then arrives two or three times, sometimes with a corrected status.
   Deduplication keeps the LAST version by source timestamp, not the first —
   a row_number() over a window, which is the pattern from deck 3.

2. LATE ARRIVAL. Records for 14:00 keep landing until 20:00. Processing only
   the current hour silently under-bills. This job reprocesses a trailing
   window and relies on MERGE to make that safe.

3. SKEW. A handful of hub numbers (customer service lines, voicemail) appear in
   millions of rows. Any join or aggregation keyed on the B-number is skewed.
   AQE handles most of it; the explicit salt below handles the rest.
"""
from __future__ import annotations

from pyspark.sql import Window
from pyspark.sql import functions as F

import job_common as J


def main() -> None:
    p = J.base_args(__doc__)
    p.add_argument("--market", required=True)
    p.add_argument("--late-window-hours", type=int, default=6)
    p.add_argument("--source", required=True)
    p.add_argument("--target-table", required=True)
    p.add_argument("--rating-table", default=None,
                   help="tariff dimension; defaults to <silver>.tariff_plan")
    args = p.parse_args()

    spark = J.build_spark(f"cdr-mediation-{args.market}-{args.run_date}-{args.run_hour}")
    target = args.target_table
    rating = args.rating_table or target.rsplit(".", 1)[0] + ".tariff_plan"

    # ---------------------------------------------------------------- read
    # The trailing window, not just this hour. Partition pruning on the raw
    # prefix keeps this cheap even though the window is 30 hours wide.
    win_start, win_end = J.late_window(args.run_date, args.late_window_hours)
    J.banner(f"market={args.market} hour={args.run_hour} "
             f"reprocessing window {win_start} -> {win_end}")

    raw = (
        spark.read.format("parquet")
        .load(args.source)
        .where(F.col("dt").between(win_start[:10], win_end[:10]))
        .where(F.col("event_ts").between(F.lit(win_start), F.lit(win_end)))
    )
    J.assert_not_empty(raw, "raw CDRs in window")

    # ---------------------------------------------------------------- 1. dedup
    # Keep the newest version of each cdr_id. source_ts is when the SWITCH
    # emitted it, so a corrected replay wins over the original.
    newest = Window.partitionBy("cdr_id").orderBy(
        F.col("source_ts").desc(), F.col("ingest_ts").desc()
    )
    deduped = (
        raw.withColumn("_rn", F.row_number().over(newest))
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )
    raw_n = raw.count()
    dedup_n = deduped.count()
    J.step(f"dedup: {raw_n:,} -> {dedup_n:,} "
           f"({100.0 * (raw_n - dedup_n) / max(raw_n, 1):.3f}% duplicates removed)")

    # ---------------------------------------------------------------- 2. normalise
    normalised = (
        deduped
        .withColumn("market", F.lit(args.market))
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("event_hour", F.hour("event_ts"))
        # E.164: strip separators, expand the national prefix to a country code.
        .withColumn("a_number", F.regexp_replace(F.col("a_number"), r"[^0-9+]", ""))
        .withColumn("b_number", F.regexp_replace(F.col("b_number"), r"[^0-9+]", ""))
        .withColumn("duration_sec", F.col("duration_sec").cast("int"))
        # A negative or absurd duration is a switch bug, not a call.
        .withColumn(
            "quality_flag",
            F.when(F.col("duration_sec") < 0, F.lit("NEGATIVE_DURATION"))
             .when(F.col("duration_sec") > 86400, F.lit("DURATION_OVER_24H"))
             .when(F.col("b_number").isNull(), F.lit("MISSING_B_NUMBER"))
             .otherwise(F.lit("OK")),
        )
        .withColumn("duration_sec",
                    F.when(F.col("duration_sec") < 0, F.lit(0))
                     .otherwise(F.col("duration_sec")))
    )

    # ---------------------------------------------------------------- 3. rate
    # The tariff table is small (thousands of rows) so it broadcasts. Without
    # the hint AQE usually gets this right anyway, but being explicit on a
    # tier-1 hourly job removes the variance.
    tariffs = spark.table(rating).where(F.col("market") == args.market)

    rated = (
        normalised.join(F.broadcast(tariffs),
                        on=["market", "service_type", "tariff_plan_id"], how="left")
        .withColumn(
            "charged_units",
            F.when(F.col("service_type") == "VOICE",
                   F.ceil(F.col("duration_sec") / F.coalesce(F.col("billing_increment_sec"),
                                                             F.lit(60))))
             .when(F.col("service_type") == "DATA",
                   F.ceil(F.col("bytes_total") / F.lit(1024 * 1024)))
             .otherwise(F.lit(1)),                      # SMS and MMS are per-event
        )
        .withColumn("charge_amount",
                    F.round(F.col("charged_units") * F.coalesce(F.col("unit_rate"),
                                                               F.lit(0.0)), 5))
        .withColumn("rated_flag",
                    F.when(F.col("unit_rate").isNull(), F.lit("UNRATED_NO_TARIFF"))
                     .otherwise(F.lit("RATED")))
    )

    unrated = rated.where(F.col("rated_flag") == "UNRATED_NO_TARIFF").count()
    if unrated:
        # Not fatal — unrated CDRs are held and re-rated when the tariff lands.
        # But it must be visible in the task log, not swallowed.
        J.step(f"WARNING: {unrated:,} CDRs had no matching tariff and are held UNRATED")

    # ---------------------------------------------------------------- 4. skew guard
    # Hub numbers dominate the B-number distribution. Salting spreads them
    # before the write's hash exchange. Atlas 3 covers why this is needed and
    # atlas 1 shows the histogram it fixes.
    final = (
        rated.withColumn("_salt", (F.rand() * 16).cast("int"))
        .repartition("event_date", "event_hour", "_salt")
        .drop("_salt")
        .select(
            "cdr_id", "market", "event_ts", "event_date", "event_hour",
            "a_number", "b_number", "service_type", "tariff_plan_id",
            "duration_sec", "bytes_total", "charged_units", "charge_amount",
            "rated_flag", "quality_flag", "source_ts",
            F.current_timestamp().alias("processed_ts"),
        )
    )

    J.assert_unique(spark, final, ["cdr_id"], "mediated CDRs")

    # ---------------------------------------------------------------- 5. MERGE
    # MERGE, not append. This is what makes reprocessing the trailing window
    # safe: a record already present is UPDATED if the switch corrected it and
    # left alone otherwise. Append would multiply every late row by the number
    # of hours it stays in the window.
    final.createOrReplaceTempView("mediated_batch")
    J.describe_commit(spark, target, "before")

    spark.sql(f"""
        MERGE INTO {target} t
        USING mediated_batch s
          ON  t.cdr_id = s.cdr_id
          AND t.event_date = s.event_date        -- lets Iceberg prune partitions
        WHEN MATCHED AND s.source_ts > t.source_ts THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    J.describe_commit(spark, target, "after")
    J.banner(f"mediation complete: {args.market} {args.run_date} {args.run_hour}:00 "
             f"| {dedup_n:,} records | snapshot {J.snapshot_id(spark, target)}")
    spark.stop()


if __name__ == "__main__":
    main()
