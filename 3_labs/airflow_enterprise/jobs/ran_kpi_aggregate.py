"""
ran_kpi_aggregate.py — called by DAG 02, mapped task aggregate_cell_counters

Aggregates raw radio counters into cell-level KPIs for one 15-minute window,
for one region.

    spark-submit ran_kpi_aggregate.py --region north \
        --window-start 2026-09-03T14:00:00+00:00 \
        --window-end   2026-09-03T14:15:00+00:00

THE SHAPE OF THE PROBLEM
    ~40,000 cells x 4 vendors, each emitting a different counter name for the
    same concept. Ericsson calls it pmRrcConnEstabSucc, Nokia calls it
    RRC_CONN_ESTAB_SUCC. The vendor mapping table normalises them, and that
    mapping is a small broadcast dimension.

    KPIs are RATIOS, and ratios do not average. Summing numerator and
    denominator separately and dividing at the end is the only correct way;
    averaging per-cell percentages weights a cell with 3 calls the same as one
    with 30,000. This is the single most common analytical bug in RAN
    reporting and the reason this job aggregates counters, not percentages.

IDEMPOTENCY
    The write is overwritePartitions on (window_start, region), so re-running a
    window replaces exactly that window and nothing else. A retry after a
    partial failure is safe, which matters when the DAG retries within a
    15-minute budget.
"""
from __future__ import annotations

from pyspark.sql import functions as F

import job_common as J


def main() -> None:
    p = J.base_args(__doc__)
    p.add_argument("--region", required=True)
    p.add_argument("--window-start", required=True)
    p.add_argument("--window-end", required=True)
    p.add_argument("--source-table", default=None)
    p.add_argument("--target-table", default=None)
    args = p.parse_args()

    spark = J.build_spark(f"ran-kpi-{args.region}-{args.window_start}")
    src = args.source_table or J.fq("telco_prod_bronze", "ran_counters_raw")
    dim = J.fq("telco_prod_silver", "cell_topology")
    vendor_map = J.fq("telco_prod_silver", "ran_counter_mapping")
    tgt = args.target_table or J.fq("telco_prod_gold", "ran_cell_kpi")

    ws, we = args.window_start, args.window_end
    J.banner(f"region={args.region} window {ws} -> {we}")

    # ---------------------------------------------------------------- read
    raw = (
        spark.table(src)
        .where(F.col("region") == args.region)
        .where((F.col("counter_ts") >= F.lit(ws)) & (F.col("counter_ts") < F.lit(we)))
    )

    # ---------------------------------------------------------------- normalise vendors
    mapping = spark.table(vendor_map).select("vendor", "vendor_counter", "canonical_counter")
    normalised = (
        raw.join(F.broadcast(mapping),
                 on=[raw.vendor == mapping.vendor,
                     raw.counter_name == mapping.vendor_counter],
                 how="inner")
        .select(raw["*"], mapping["canonical_counter"])
    )

    # ---------------------------------------------------------------- pivot counters
    # One row per cell, one column per canonical counter.
    wanted = [
        "rrc_attempts", "rrc_success",
        "call_attempts", "call_drops",
        "ho_attempts", "ho_success",
        "prb_used", "prb_available",
        "dl_bytes", "ul_bytes",
    ]
    pivoted = (
        normalised.groupBy("cell_id", "region")
        .pivot("canonical_counter", wanted)
        .agg(F.sum("counter_value"))
        .na.fill(0)
    )

    # ---------------------------------------------------------------- KPIs
    # Ratios computed from summed counters, with a zero guard on every
    # denominator. A NULL here is honest: it means "no traffic, no opinion".
    def ratio(num: str, den: str):
        return F.when(F.col(den) > 0,
                      F.round(100.0 * F.col(num) / F.col(den), 4)).otherwise(F.lit(None))

    kpi = (
        pivoted
        .withColumn("rrc_success_rate", ratio("rrc_success", "rrc_attempts"))
        .withColumn("drop_call_rate", ratio("call_drops", "call_attempts"))
        .withColumn("handover_success_rate", ratio("ho_success", "ho_attempts"))
        .withColumn("prb_utilisation", ratio("prb_used", "prb_available"))
        .withColumn("total_bytes", F.col("dl_bytes") + F.col("ul_bytes"))
        .withColumn("throughput_mbps",
                    F.round((F.col("dl_bytes") + F.col("ul_bytes")) * 8
                            / 1_000_000 / 900.0, 3))          # 900s = the 15-min window
    )

    # ---------------------------------------------------------------- enrich
    topology = spark.table(dim).select(
        "cell_id", "site_id", "technology", "band", "latitude", "longitude", "vendor")
    final = (
        kpi.join(F.broadcast(topology), on="cell_id", how="left")
        .withColumn("window_start", F.to_timestamp(F.lit(ws)))
        .withColumn("window_end", F.to_timestamp(F.lit(we)))
        .withColumn("computed_at", F.current_timestamp())
        .select(
            "window_start", "window_end", "region", "cell_id", "site_id",
            "technology", "band", "vendor",
            "rrc_attempts", "rrc_success", "call_attempts", "call_drops",
            "ho_attempts", "ho_success", "prb_used", "prb_available",
            "rrc_success_rate", "drop_call_rate", "handover_success_rate",
            "prb_utilisation", "total_bytes", "throughput_mbps",
            "latitude", "longitude", "computed_at",
        )
    )

    n = J.assert_not_empty(final, f"KPIs for {args.region}")
    J.assert_unique(spark, final, ["window_start", "cell_id"], "cell KPIs")

    # ---------------------------------------------------------------- write
    # Dynamic partition overwrite: replaces only (window_start, region) that
    # this run produced. Rerunning the window is therefore idempotent.
    (final.sortWithinPartitions("region", "cell_id")
          .writeTo(tgt)
          .option("write.distribution-mode", "hash")
          .overwritePartitions())

    J.describe_commit(spark, tgt, "after write")
    J.banner(f"{args.region}: {n:,} cells for window {ws}")
    spark.stop()


if __name__ == "__main__":
    main()
