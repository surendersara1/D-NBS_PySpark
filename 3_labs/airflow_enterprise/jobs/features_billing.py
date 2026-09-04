"""
features_billing.py — called by DAG 03, TaskGroup subscriber_360/features_billing

Billing and payment behaviour per subscriber. Late payment and bill shock are
two of the strongest churn predictors in any telco model.

TECHNIQUE ON SHOW
  * bill shock: the ratio of this month's bill to the trailing median, using a
    window function rather than a self-join. A subscriber who suddenly gets a
    bill 3x their normal is very likely to call, complain and leave.
  * percentile_approx for a robust centre. The mean is destroyed by one
    roaming month; the median is not.
"""
from __future__ import annotations

from pyspark.sql import Window
from pyspark.sql import functions as F

import job_common as J


def main() -> None:
    p = J.base_args(__doc__)
    p.add_argument("--output", required=True)
    p.add_argument("--months", type=int, default=12)
    args = p.parse_args()

    spark = J.build_spark(f"features-billing-{args.run_date}")
    bills = J.fq("telco_prod_silver", "billing_events")
    payments = J.fq("telco_prod_silver", "payment_events")
    run_date = args.run_date

    J.banner(f"billing features as of {run_date}, {args.months}-month history")

    b = (
        spark.table(bills)
        .where(F.col("bill_date") <= F.lit(run_date))
        .where(F.col("bill_date") > F.add_months(F.lit(run_date), -args.months))
    )

    # Trailing median per subscriber, excluding the current bill, in one pass.
    hist = Window.partitionBy("subscriber_id").orderBy("bill_date").rowsBetween(
        Window.unboundedPreceding, -1
    )
    latest = Window.partitionBy("subscriber_id").orderBy(F.col("bill_date").desc())

    shocked = (
        b.withColumn("median_prior",
                     F.expr("percentile_approx(bill_amount, 0.5)").over(hist))
         .withColumn("_rn", F.row_number().over(latest))
    )

    current = (
        shocked.where(F.col("_rn") == 1)
        .withColumn("bill_shock_ratio",
                    F.when(F.col("median_prior") > 0,
                           F.round(F.col("bill_amount") / F.col("median_prior"), 4))
                     .otherwise(F.lit(None)))
        .select("subscriber_id",
                F.col("bill_amount").alias("last_bill_amount"),
                F.col("median_prior").alias("median_bill_amount"),
                "bill_shock_ratio",
                F.col("bill_date").alias("last_bill_date"))
    )

    totals = b.groupBy("subscriber_id").agg(
        F.round(F.avg("bill_amount"), 2).alias("avg_bill_12m"),
        F.round(F.stddev("bill_amount"), 2).alias("stddev_bill_12m"),
        F.max("bill_amount").alias("max_bill_12m"),
        F.count("*").alias("bills_12m"),
        F.sum(F.when(F.col("has_roaming"), 1).otherwise(0)).alias("roaming_months_12m"),
        F.sum(F.when(F.col("has_overage"), 1).otherwise(0)).alias("overage_months_12m"),
    )

    pay = (
        spark.table(payments)
        .where(F.col("payment_date") <= F.lit(run_date))
        .where(F.col("payment_date") > F.add_months(F.lit(run_date), -args.months))
        .groupBy("subscriber_id").agg(
            F.round(F.avg("days_late"), 2).alias("avg_days_late_12m"),
            F.max("days_late").alias("max_days_late_12m"),
            F.sum(F.when(F.col("days_late") > 0, 1).otherwise(0)).alias("late_payments_12m"),
            F.sum(F.when(F.col("payment_status") == "FAILED", 1).otherwise(0))
             .alias("failed_payments_12m"),
            F.count("*").alias("payments_12m"),
        )
    )

    features = (
        totals.join(current, "subscriber_id", "left")
        .join(pay, "subscriber_id", "left")
        .withColumn("late_payment_rate_12m",
                    F.when(F.col("payments_12m") > 0,
                           F.round(F.col("late_payments_12m") / F.col("payments_12m"), 4))
                     .otherwise(F.lit(None)))
        .withColumn("bill_volatility",
                    F.when(F.col("avg_bill_12m") > 0,
                           F.round(F.col("stddev_bill_12m") / F.col("avg_bill_12m"), 4))
                     .otherwise(F.lit(None)))
        .withColumn("feature_date", F.lit(run_date).cast("date"))
    )

    J.assert_not_empty(features, "billing features")
    J.assert_unique(spark, features, ["subscriber_id"], "billing features")

    features.repartition(100).write.mode("overwrite").parquet(args.output)
    J.banner(f"billing features written to {args.output}")
    spark.stop()


if __name__ == "__main__":
    main()
