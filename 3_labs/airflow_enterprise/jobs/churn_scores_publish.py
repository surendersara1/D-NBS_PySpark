"""
churn_scores_publish.py — called by DAG 03, task scores_to_iceberg

Takes the raw SageMaker batch-transform output and turns it into the gold
churn_scores table the campaign system reads.

    spark-submit churn_scores_publish.py --run-date 2026-09-03 \
        --source s3://telco-prod-lakehouse/gold/churn_scores/dt=2026-09-03/ \
        --target-table glue_catalog.telco_prod_gold.churn_scores

TECHNIQUES ON SHOW
  * decile banding with ntile() — campaign teams work in "top 10% at risk",
    not in probabilities, and the band must be computed per market because
    base churn rates differ by country.
  * a sanity gate on the score DISTRIBUTION, not just on row count. A model
    that has silently broken usually still returns the right number of rows;
    what changes is that every score collapses to 0.5, or the mean jumps.
    Catching that here is what stops a broken model reaching a campaign.
"""
from __future__ import annotations

from pyspark.sql import Window
from pyspark.sql import functions as F

import job_common as J


def main() -> None:
    p = J.base_args(__doc__)
    p.add_argument("--source", required=True)
    p.add_argument("--target-table", required=True)
    p.add_argument("--min-mean-score", type=float, default=0.01)
    p.add_argument("--max-mean-score", type=float, default=0.45)
    p.add_argument("--min-score-stddev", type=float, default=0.02)
    args = p.parse_args()

    spark = J.build_spark(f"churn-publish-{args.run_date}")
    run_date = args.run_date

    J.banner(f"publishing churn scores for {run_date}")

    # Transform output is subscriber_id,score with no header.
    raw = (
        spark.read.option("header", False).csv(args.source)
        .toDF("subscriber_id", "churn_probability")
        .withColumn("churn_probability", F.col("churn_probability").cast("double"))
    )
    n = J.assert_not_empty(raw, "scored subscribers")

    # ---------------------------------------------------------------- distribution gate
    stats = raw.select(
        F.avg("churn_probability").alias("mean"),
        F.stddev("churn_probability").alias("sd"),
        F.min("churn_probability").alias("mn"),
        F.max("churn_probability").alias("mx"),
        F.sum(F.when(F.col("churn_probability").isNull(), 1).otherwise(0)).alias("nulls"),
    ).collect()[0]
    J.step(f"score distribution: mean={stats['mean']:.4f} sd={stats['sd']:.4f} "
           f"min={stats['mn']:.4f} max={stats['mx']:.4f} nulls={stats['nulls']}")

    if stats["nulls"]:
        J.fail(f"{stats['nulls']:,} null scores — the transform output is malformed")
    if not (args.min_mean_score <= stats["mean"] <= args.max_mean_score):
        J.fail(f"mean score {stats['mean']:.4f} outside the sane band "
               f"[{args.min_mean_score}, {args.max_mean_score}] — the model is broken, "
               "refusing to publish to the campaign system")
    if stats["sd"] < args.min_score_stddev:
        J.fail(f"score stddev {stats['sd']:.4f} below {args.min_score_stddev} — the model "
               "is returning a near-constant, refusing to publish")

    # ---------------------------------------------------------------- enrich and band
    subs = spark.table(J.fq("telco_prod_silver", "subscriber")).select(
        "subscriber_id", "market", "segment", "msisdn_hash")
    joined = raw.join(F.broadcast(subs), "subscriber_id", "left")

    # Deciles per market: churn base rates differ per country, so a global
    # decile would put an entire market in the top band.
    by_market = Window.partitionBy("market").orderBy(F.col("churn_probability").desc())

    scored = (
        joined
        .withColumn("risk_decile", F.ntile(10).over(by_market))
        .withColumn("risk_band",
                    F.when(F.col("risk_decile") == 1, "CRITICAL")
                     .when(F.col("risk_decile") <= 2, "HIGH")
                     .when(F.col("risk_decile") <= 5, "MEDIUM")
                     .otherwise("LOW"))
        .withColumn("score_date", F.lit(run_date).cast("date"))
        .withColumn("model_version", F.lit(f"churn-xgb-{run_date.replace('-', '')}"))
        .withColumn("scored_at", F.current_timestamp())
        .select("score_date", "subscriber_id", "msisdn_hash", "market", "segment",
                "churn_probability", "risk_decile", "risk_band",
                "model_version", "scored_at")
    )

    J.assert_unique(spark, scored, ["score_date", "subscriber_id"], "churn scores")

    (scored.writeTo(args.target_table)
           .option("write.distribution-mode", "hash")
           .overwritePartitions())
    J.describe_commit(spark, args.target_table, "churn_scores")

    dist = (scored.groupBy("risk_band").count()
                  .orderBy(F.col("count").desc()).collect())
    for r in dist:
        J.step(f"{r['risk_band']:>8}: {r['count']:,}")

    J.banner(f"{n:,} churn scores published for {run_date}")
    spark.stop()


if __name__ == "__main__":
    main()
