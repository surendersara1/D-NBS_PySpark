"""
features_device.py — called by DAG 03, TaskGroup subscriber_360/features_device

Handset and contract features. Device age against contract end is the most
mechanical churn driver there is: a subscriber whose 24-month commitment ends
next month, holding a four-year-old phone, is a competitor's target.

TECHNIQUE ON SHOW
  * a slowly changing dimension read correctly. The device table keeps history
    with valid_from / valid_to, so "the device as of the run date" is a range
    predicate, not a max(). Taking the latest row would leak the future into a
    backfill and quietly inflate model accuracy in training.
"""
from __future__ import annotations

from pyspark.sql import functions as F

import job_common as J


def main() -> None:
    p = J.base_args(__doc__)
    p.add_argument("--output", required=True)
    args = p.parse_args()

    spark = J.build_spark(f"features-device-{args.run_date}")
    devices = J.fq("telco_prod_silver", "subscriber_device_scd2")
    contracts = J.fq("telco_prod_silver", "subscriber_contract")
    run_date = args.run_date

    J.banner(f"device and contract features as of {run_date}")

    # AS OF the run date. This is the line that keeps a backfill honest.
    dev = (
        spark.table(devices)
        .where((F.col("valid_from") <= F.lit(run_date))
               & ((F.col("valid_to") > F.lit(run_date)) | F.col("valid_to").isNull()))
        .select("subscriber_id", "imei_tac", "device_model", "device_vendor",
                "device_launch_date", "device_since", "is_5g_capable", "os_family")
    )

    con = (
        spark.table(contracts)
        .where((F.col("valid_from") <= F.lit(run_date))
               & ((F.col("valid_to") > F.lit(run_date)) | F.col("valid_to").isNull()))
        .select("subscriber_id", "contract_type", "tariff_plan_id", "monthly_fee",
                "contract_start_date", "contract_end_date", "handset_subsidy",
                "is_family_plan", "num_lines_on_account")
    )

    features = (
        con.join(dev, "subscriber_id", "left")
        .withColumn("device_age_days",
                    F.datediff(F.lit(run_date), F.col("device_launch_date")))
        .withColumn("days_on_current_device",
                    F.datediff(F.lit(run_date), F.col("device_since")))
        .withColumn("days_to_contract_end",
                    F.datediff(F.col("contract_end_date"), F.lit(run_date)))
        .withColumn("contract_tenure_days",
                    F.datediff(F.lit(run_date), F.col("contract_start_date")))
        .withColumn("out_of_contract", F.col("days_to_contract_end") <= 0)
        # The window competitors actually target.
        .withColumn("in_churn_window",
                    F.col("days_to_contract_end").between(0, 90))
        .withColumn("device_upgrade_due",
                    (F.col("days_on_current_device") > 730)
                    & (F.col("days_to_contract_end") < 90))
        # A 5G tariff on a 4G handset is a subscriber paying for nothing, which
        # is a complaint and a cancellation waiting to happen.
        .withColumn("paying_for_unusable_5g",
                    (F.col("is_5g_capable") == False)                          # noqa: E712
                    & F.col("tariff_plan_id").rlike("(?i)5g"))
        .withColumn("feature_date", F.lit(run_date).cast("date"))
        .drop("contract_start_date", "device_launch_date", "device_since")
    )

    J.assert_not_empty(features, "device features")
    J.assert_unique(spark, features, ["subscriber_id"], "device features")

    features.repartition(50).write.mode("overwrite").parquet(args.output)
    J.banner(f"device features written to {args.output}")
    spark.stop()


if __name__ == "__main__":
    main()
