"""
features_usage.py — called by DAG 03, TaskGroup subscriber_360/features_usage

The heaviest of the six feature jobs: 90 days of CDRs (order 10^11 rows for the
group) collapsed to one row per subscriber.

    spark-submit features_usage.py --run-date 2026-09-03 \
        --output s3://telco-prod-lakehouse/ml/churn/features/usage/dt=2026-09-03/

TECHNIQUES ON SHOW
  * multi-window trend features. Churn is predicted far better by the CHANGE in
    behaviour than by its level: a subscriber whose 7-day minutes are 40% below
    their 90-day average is the signal. Three windows in one pass using
    conditional aggregation, not three scans.
  * conditional aggregation with F.when inside F.sum, which is how you compute
    many windows in a single shuffle.
  * a deliberate zero-vs-null distinction. A subscriber with no calls in the
    last 7 days has 0 minutes, which is a strong churn signal; a subscriber who
    joined 3 days ago has NULL, which is no signal at all. Collapsing the two
    is the classic feature-engineering bug.
"""
from __future__ import annotations

from pyspark.sql import functions as F

import job_common as J


def main() -> None:
    p = J.base_args(__doc__)
    p.add_argument("--output", required=True)
    p.add_argument("--lookback-days", type=int, default=90)
    args = p.parse_args()

    spark = J.build_spark(f"features-usage-{args.run_date}")
    cdr = J.fq("telco_prod_silver", "cdr_events")
    subs = J.fq("telco_prod_silver", "subscriber")
    run_date = args.run_date

    J.banner(f"usage features as of {run_date}, {args.lookback_days}-day lookback")

    events = (
        spark.table(cdr)
        .where(F.col("event_date") > F.date_sub(F.lit(run_date), args.lookback_days))
        .where(F.col("event_date") <= F.lit(run_date))
        .where(F.col("quality_flag") == "OK")
    )

    # Days-ago bucket, computed once and reused by every conditional aggregate.
    ev = events.withColumn("days_ago", F.datediff(F.lit(run_date), F.col("event_date")))

    def in_window(days: int, expr):
        return F.sum(F.when(F.col("days_ago") < days, expr).otherwise(F.lit(0)))

    agg = ev.groupBy("subscriber_id").agg(
        # volume, three windows, one shuffle
        in_window(7, F.col("duration_sec")).alias("voice_sec_7d"),
        in_window(30, F.col("duration_sec")).alias("voice_sec_30d"),
        in_window(90, F.col("duration_sec")).alias("voice_sec_90d"),
        in_window(7, F.when(F.col("service_type") == "SMS", 1).otherwise(0)).alias("sms_7d"),
        in_window(30, F.when(F.col("service_type") == "SMS", 1).otherwise(0)).alias("sms_30d"),
        in_window(7, F.col("bytes_total")).alias("bytes_7d"),
        in_window(30, F.col("bytes_total")).alias("bytes_30d"),
        in_window(90, F.col("bytes_total")).alias("bytes_90d"),
        # spend
        in_window(30, F.col("charge_amount")).alias("charge_30d"),
        in_window(90, F.col("charge_amount")).alias("charge_90d"),
        # breadth of the social graph — a shrinking circle precedes churn
        F.countDistinct(F.when(F.col("days_ago") < 30, F.col("b_number"))).alias("distinct_b_30d"),
        F.countDistinct(F.when(F.col("days_ago") < 90, F.col("b_number"))).alias("distinct_b_90d"),
        # off-net share: calls to other operators
        in_window(30, F.when(F.col("is_onnet") == False, 1).otherwise(0)).alias("offnet_calls_30d"),  # noqa: E712
        in_window(30, F.lit(1)).alias("events_30d"),
        # recency
        F.min("days_ago").alias("days_since_last_event"),
        F.max("event_date").alias("last_event_date"),
    )

    # Tenure decides whether a zero is meaningful.
    tenure = spark.table(subs).select(
        "subscriber_id",
        F.datediff(F.lit(run_date), F.col("activation_date")).alias("tenure_days"),
    )

    def trend(short_col: str, long_col: str, short_days: int, long_days: int):
        """Ratio of the recent daily rate to the long-run daily rate.

        1.0 = unchanged, 0.4 = down 60%. NULL when the long window is empty,
        because a ratio against nothing is meaningless.
        """
        short_rate = F.col(short_col) / F.lit(short_days)
        long_rate = F.col(long_col) / F.lit(long_days)
        return F.when(long_rate > 0, F.round(short_rate / long_rate, 4)).otherwise(F.lit(None))

    features = (
        agg.join(F.broadcast(tenure), "subscriber_id", "left")
        .withColumn("voice_trend_7_90", trend("voice_sec_7d", "voice_sec_90d", 7, 90))
        .withColumn("data_trend_7_90", trend("bytes_7d", "bytes_90d", 7, 90))
        .withColumn("spend_trend_30_90", trend("charge_30d", "charge_90d", 30, 90))
        .withColumn("social_graph_shrink",
                    F.when(F.col("distinct_b_90d") > 0,
                           F.round(F.col("distinct_b_30d") * 3.0 / F.col("distinct_b_90d"), 4))
                     .otherwise(F.lit(None)))
        .withColumn("offnet_share_30d",
                    F.when(F.col("events_30d") > 0,
                           F.round(F.col("offnet_calls_30d") / F.col("events_30d"), 4))
                     .otherwise(F.lit(None)))
        # A zero only counts as a zero if the subscriber existed for the window.
        .withColumn("voice_sec_7d",
                    F.when(F.col("tenure_days") >= 7, F.col("voice_sec_7d")).otherwise(F.lit(None)))
        .withColumn("is_dormant_7d",
                    F.when(F.col("tenure_days") >= 7, F.col("days_since_last_event") >= 7)
                     .otherwise(F.lit(None)))
        .withColumn("feature_date", F.lit(run_date).cast("date"))
    )

    J.assert_not_empty(features, "usage features")
    J.assert_unique(spark, features, ["subscriber_id"], "usage features")

    (features.repartition(200)
             .write.mode("overwrite").parquet(args.output))
    J.banner(f"usage features written to {args.output}")
    spark.stop()


if __name__ == "__main__":
    main()
