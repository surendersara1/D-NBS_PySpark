"""
subscriber_360_assemble.py — called by DAG 03, task subscriber_360/assemble_wide_table

Joins the six feature domains into one wide row per subscriber, writes it to
the gold Iceberg table, and emits the three CSV splits SageMaker consumes.

    spark-submit subscriber_360_assemble.py --run-date 2026-09-03 \
        --feature-root s3://telco-prod-lakehouse/ml/churn/features \
        --target-table glue_catalog.telco_prod_gold.subscriber_360

TECHNIQUES ON SHOW
  * the base table drives the join. Every feature domain is a LEFT join onto
    the active subscriber list, so a subscriber with no care contacts still
    gets a row. An inner join here would silently drop the quietest — and most
    churn-prone — subscribers from the training set.
  * the label is computed with a FORWARD window and is deliberately not
    available for the scoring split. Leakage is the failure mode of every
    first churn model; the code separates the two explicitly.
  * an as-of-date guard so a backfill cannot use tomorrow's data.
"""
from __future__ import annotations

from pyspark.sql import functions as F

import job_common as J

DOMAINS = ["usage", "billing", "topups", "network_experience", "care", "device"]


def main() -> None:
    p = J.base_args(__doc__)
    p.add_argument("--feature-root", required=True)
    p.add_argument("--target-table", required=True)
    p.add_argument("--label-horizon-days", type=int, default=30)
    args = p.parse_args()

    spark = J.build_spark(f"subscriber-360-{args.run_date}")
    run_date = args.run_date
    subs = J.fq("telco_prod_silver", "subscriber")

    J.banner(f"assembling subscriber 360 for {run_date}")

    # ---------------------------------------------------------------- base
    base = (
        spark.table(subs)
        .where(F.col("status").isin("ACTIVE", "SUSPENDED"))
        .where(F.col("activation_date") <= F.lit(run_date))
        .select("subscriber_id", "market", "segment", "status",
                "activation_date", "churn_date")
        .withColumn("tenure_days", F.datediff(F.lit(run_date), F.col("activation_date")))
    )
    base_n = J.assert_not_empty(base, "active subscribers")

    # ---------------------------------------------------------------- join the six
    wide = base
    for d in DOMAINS:
        path = f"{args.feature_root}/{d}/dt={run_date}/"
        try:
            f_df = spark.read.parquet(path).drop("feature_date")
        except Exception as exc:
            J.fail(f"feature domain '{d}' missing at {path}: {exc}")
        # Prefix every column so two domains cannot collide on a name.
        renamed = f_df.select(
            [F.col("subscriber_id")]
            + [F.col(c).alias(f"{d}__{c}") for c in f_df.columns if c != "subscriber_id"]
        )
        wide = wide.join(F.broadcast(renamed) if d in ("device",) else renamed,
                         "subscriber_id", "left")
        J.step(f"joined {d}: {len(renamed.columns) - 1} features")

    # ---------------------------------------------------------------- label
    # Churned within the horizon AFTER the feature date. Only defined for rows
    # old enough that the horizon has actually elapsed.
    horizon_end = F.date_add(F.lit(run_date), args.label_horizon_days)
    labelled = (
        wide
        .withColumn(
            "label_churned_30d",
            F.when(F.col("churn_date").isNull(), F.lit(0))
             .when((F.col("churn_date") > F.lit(run_date))
                   & (F.col("churn_date") <= horizon_end), F.lit(1))
             .when(F.col("churn_date") <= F.lit(run_date), F.lit(None))  # already gone
             .otherwise(F.lit(0)),
        )
        .withColumn("label_is_observable", horizon_end <= F.current_date())
        .withColumn("feature_date", F.lit(run_date).cast("date"))
        .withColumn("assembled_at", F.current_timestamp())
        .drop("churn_date")          # never let the raw churn date reach the model
    )

    J.assert_unique(spark, labelled, ["subscriber_id"], "subscriber 360")
    J.step(f"{base_n:,} subscribers x {len(labelled.columns)} columns")

    # ---------------------------------------------------------------- write gold
    (labelled.writeTo(args.target_table)
             .option("write.distribution-mode", "hash")
             .overwritePartitions())
    J.describe_commit(spark, args.target_table, "subscriber_360")

    # ---------------------------------------------------------------- ML splits
    # train/validation only from rows whose label is observable; inference from
    # everyone. Mixing the two is the leak.
    trainable = labelled.where(F.col("label_is_observable")
                               & F.col("label_churned_30d").isNotNull())
    feature_cols = [c for c in labelled.columns
                    if c not in ("subscriber_id", "market", "segment", "status",
                                 "activation_date", "feature_date", "assembled_at",
                                 "label_churned_30d", "label_is_observable")]

    # XGBoost on SageMaker wants the label FIRST and no header.
    train_df = trainable.select([F.col("label_churned_30d")]
                                + [F.col(c).cast("double") for c in feature_cols]).na.fill(0)
    train, validation = train_df.randomSplit([0.8, 0.2], seed=42)

    root = args.feature_root
    (train.write.mode("overwrite").option("header", False)
          .csv(f"{root}/train/dt={run_date}/"))
    (validation.write.mode("overwrite").option("header", False)
               .csv(f"{root}/validation/dt={run_date}/"))
    (labelled.select([F.col("subscriber_id")]
                     + [F.col(c).cast("double") for c in feature_cols]).na.fill(0)
             .write.mode("overwrite").option("header", False)
             .csv(f"{root}/inference/dt={run_date}/"))

    J.step(f"train={train.count():,} validation={validation.count():,} "
           f"inference={base_n:,} | {len(feature_cols)} features")
    J.banner(f"subscriber 360 complete for {run_date}")
    spark.stop()


if __name__ == "__main__":
    main()
