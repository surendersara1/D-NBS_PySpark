"""
features_care.py — called by DAG 03, TaskGroup subscriber_360/features_care

Customer care contact history. A complaint about billing, followed by a second
contact within a week, is close to a churn declaration.

TECHNIQUE ON SHOW
  * escalation detection: repeat contacts on the same topic inside a window,
    found with a lag() over (subscriber, topic).
  * a categorical collapsed into a fixed set of indicator columns. Models need
    a stable schema; a pivot over whatever categories happened to appear this
    month produces a different column set every day and breaks inference.
"""
from __future__ import annotations

from pyspark.sql import Window
from pyspark.sql import functions as F

import job_common as J

# Fixed, not discovered. The model's schema must not change because a new
# reason code appeared in yesterday's data.
TOPICS = ["BILLING", "NETWORK", "DEVICE", "TARIFF", "ROAMING", "CANCELLATION", "OTHER"]


def main() -> None:
    p = J.base_args(__doc__)
    p.add_argument("--output", required=True)
    p.add_argument("--lookback-days", type=int, default=180)
    args = p.parse_args()

    spark = J.build_spark(f"features-care-{args.run_date}")
    care = J.fq("telco_prod_silver", "care_contacts")
    run_date = args.run_date

    J.banner(f"care features as of {run_date}")

    c = (
        spark.table(care)
        .where(F.col("contact_date") <= F.lit(run_date))
        .where(F.col("contact_date") > F.date_sub(F.lit(run_date), args.lookback_days))
        .withColumn("topic",
                    F.when(F.col("reason_category").isin(TOPICS), F.col("reason_category"))
                     .otherwise(F.lit("OTHER")))
        .withColumn("days_ago", F.datediff(F.lit(run_date), F.col("contact_date")))
    )

    # Escalation: a repeat contact on the same topic within 7 days.
    w = Window.partitionBy("subscriber_id", "topic").orderBy("contact_date")
    esc = (
        c.withColumn("prev_same_topic", F.lag("contact_date").over(w))
         .withColumn("is_repeat_7d",
                     F.datediff(F.col("contact_date"), F.col("prev_same_topic")) <= 7)
    )

    def topic_count(topic: str, days: int):
        return F.sum(F.when((F.col("topic") == topic) & (F.col("days_ago") < days), 1)
                     .otherwise(0))

    aggs = [
        F.count("*").alias("care_contacts_180d"),
        F.sum(F.when(F.col("days_ago") < 30, 1).otherwise(0)).alias("care_contacts_30d"),
        F.sum(F.when(F.col("days_ago") < 7, 1).otherwise(0)).alias("care_contacts_7d"),
        F.sum(F.when(F.col("is_repeat_7d"), 1).otherwise(0)).alias("repeat_contacts_180d"),
        F.min("days_ago").alias("days_since_last_contact"),
        F.round(F.avg("handling_time_sec"), 1).alias("avg_handling_time"),
        F.round(F.avg("csat_score"), 3).alias("avg_csat"),
        F.min("csat_score").alias("min_csat"),
        F.sum(F.when(F.col("resolved") == False, 1).otherwise(0)).alias("unresolved_180d"),  # noqa: E712
        F.countDistinct("channel").alias("distinct_channels"),
    ]
    aggs += [topic_count(t, 180).alias(f"care_{t.lower()}_180d") for t in TOPICS]
    aggs += [topic_count(t, 30).alias(f"care_{t.lower()}_30d") for t in TOPICS]

    agg = esc.groupBy("subscriber_id").agg(*aggs)

    features = (
        agg
        .withColumn("unresolved_rate",
                    F.when(F.col("care_contacts_180d") > 0,
                           F.round(F.col("unresolved_180d") / F.col("care_contacts_180d"), 4))
                     .otherwise(F.lit(None)))
        # The loudest single signal in the whole feature set.
        .withColumn("asked_about_cancellation",
                    F.col("care_cancellation_180d") > 0)
        .withColumn("recent_escalation",
                    (F.col("care_contacts_30d") >= 3) | (F.col("repeat_contacts_180d") >= 2))
        .withColumn("feature_date", F.lit(run_date).cast("date"))
    )

    J.assert_not_empty(features, "care features")
    J.assert_unique(spark, features, ["subscriber_id"], "care features")

    features.repartition(50).write.mode("overwrite").parquet(args.output)
    J.banner(f"care features written to {args.output}")
    spark.stop()


if __name__ == "__main__":
    main()
