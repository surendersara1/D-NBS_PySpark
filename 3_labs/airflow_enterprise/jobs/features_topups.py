"""
features_topups.py — called by DAG 03, TaskGroup subscriber_360/features_topups

Prepaid top-up behaviour. For a prepaid base, the gap between top-ups is the
single most predictive churn feature there is: prepaid subscribers do not
cancel, they simply stop topping up.

TECHNIQUE ON SHOW
  * inter-event gaps with lag() over a window, then aggregating the gaps. This
    is the generic "time between events" pattern — reuse it for logins,
    purchases, or any behavioural stream.
  * survival-style framing: compare the CURRENT gap against the subscriber's
    own historical gap. Being 3x past your own normal top-up interval means
    much more than an absolute number of days.
"""
from __future__ import annotations

from pyspark.sql import Window
from pyspark.sql import functions as F

import job_common as J


def main() -> None:
    p = J.base_args(__doc__)
    p.add_argument("--output", required=True)
    p.add_argument("--lookback-days", type=int, default=180)
    args = p.parse_args()

    spark = J.build_spark(f"features-topups-{args.run_date}")
    topups = J.fq("telco_prod_silver", "topup_events")
    run_date = args.run_date

    J.banner(f"top-up features as of {run_date}")

    t = (
        spark.table(topups)
        .where(F.col("topup_date") <= F.lit(run_date))
        .where(F.col("topup_date") > F.date_sub(F.lit(run_date), args.lookback_days))
        .where(F.col("status") == "SUCCESS")
    )

    # Days since the previous top-up, per subscriber.
    w = Window.partitionBy("subscriber_id").orderBy("topup_date")
    gaps = (
        t.withColumn("prev_topup", F.lag("topup_date").over(w))
         .withColumn("gap_days", F.datediff(F.col("topup_date"), F.col("prev_topup")))
    )

    agg = gaps.groupBy("subscriber_id").agg(
        F.count("*").alias("topups_180d"),
        F.round(F.sum("topup_amount"), 2).alias("topup_amount_180d"),
        F.round(F.avg("topup_amount"), 2).alias("avg_topup_amount"),
        F.round(F.avg("gap_days"), 2).alias("avg_gap_days"),
        F.expr("percentile_approx(gap_days, 0.5)").alias("median_gap_days"),
        F.round(F.stddev("gap_days"), 2).alias("stddev_gap_days"),
        F.max("topup_date").alias("last_topup_date"),
        F.countDistinct("channel").alias("distinct_channels"),
        F.sum(F.when(F.col("channel") == "APP", 1).otherwise(0)).alias("app_topups"),
    )

    features = (
        agg
        .withColumn("days_since_last_topup",
                    F.datediff(F.lit(run_date), F.col("last_topup_date")))
        # The feature that actually matters: how overdue is this subscriber
        # relative to their OWN rhythm.
        .withColumn("overdue_ratio",
                    F.when(F.col("median_gap_days") > 0,
                           F.round(F.col("days_since_last_topup")
                                   / F.col("median_gap_days"), 3))
                     .otherwise(F.lit(None)))
        .withColumn("is_overdue_2x", F.col("overdue_ratio") >= 2.0)
        .withColumn("app_topup_share",
                    F.when(F.col("topups_180d") > 0,
                           F.round(F.col("app_topups") / F.col("topups_180d"), 4))
                     .otherwise(F.lit(None)))
        .withColumn("topup_regularity",
                    F.when(F.col("avg_gap_days") > 0,
                           F.round(F.col("stddev_gap_days") / F.col("avg_gap_days"), 4))
                     .otherwise(F.lit(None)))
        .withColumn("feature_date", F.lit(run_date).cast("date"))
    )

    J.assert_not_empty(features, "top-up features")
    J.assert_unique(spark, features, ["subscriber_id"], "top-up features")

    features.repartition(100).write.mode("overwrite").parquet(args.output)
    J.banner(f"top-up features written to {args.output}")
    spark.stop()


if __name__ == "__main__":
    main()
