"""
interconnect_reconcile.py — called by DAG 04, task reconcile_ledgers

Matches our record of every cross-network call against the partner operator's
record of the same call, and quantifies the gap in euros.

    spark-submit interconnect_reconcile.py --run-date 2026-09-03 \
        --ours   glue_catalog.telco_prod_silver.billing_events \
        --theirs glue_catalog.telco_prod_silver.interconnect_partner_cdr \
        --fx-table glue_catalog.telco_prod_silver.fx_rates_daily \
        --target-table glue_catalog.telco_prod_gold.interconnect_variance

WHY THIS JOB IS HARD

Two operators time-stamp the same call from different clocks, round durations
with different rules, and identify it with different keys. There is no shared
call id. Matching therefore has to be fuzzy:

    key   = (a_number, b_number, start_time rounded to the minute)
    then  = accept a match if the durations agree within a tolerance

That key is SKEWED beyond anything else in this course. A partner's voicemail
hub or a call-centre number can appear in millions of rows, so a handful of
join keys carry a large share of the data. Three things handle it, and all
three are in the code below:

    1. AQE skew join, enabled at submit time
    2. an explicit salt on the known hot keys, identified from the data itself
       rather than hard-coded
    3. a broadcast of the FX dimension so it never joins the hot side

The output is a per-partner, per-day variance in EUR, which DAG 04 then judges
against a money threshold.
"""
from __future__ import annotations

from pyspark.sql import functions as F

import job_common as J

SALT_BUCKETS = 32
HOT_KEY_ROW_THRESHOLD = 100_000       # a b_number appearing more than this is a hub


def main() -> None:
    p = J.base_args(__doc__)
    p.add_argument("--ours", required=True)
    p.add_argument("--theirs", required=True)
    p.add_argument("--fx-table", required=True)
    p.add_argument("--target-table", required=True)
    p.add_argument("--duration-tolerance-sec", type=int, default=2)
    args = p.parse_args()

    spark = J.build_spark(f"interconnect-reconcile-{args.run_date}")
    run_date = args.run_date

    J.banner(f"reconciling interconnect ledgers for {run_date}")

    # ---------------------------------------------------------------- read both sides
    ours = (
        spark.table(args.ours)
        .where(F.col("event_date") == F.lit(run_date))
        .where(F.col("is_onnet") == False)                              # noqa: E712
        .select(
            F.col("partner_code"),
            F.col("a_number"), F.col("b_number"),
            F.date_trunc("minute", F.col("event_ts")).alias("call_minute"),
            F.col("duration_sec").alias("our_duration"),
            F.col("charged_units").alias("our_units"),
            F.col("charge_amount").alias("our_charge"),
            F.lit("EUR").alias("our_currency"),
        )
    )

    theirs = (
        spark.table(args.theirs)
        .where(F.col("settlement_date") == F.lit(run_date))
        .select(
            F.col("partner_code"),
            F.col("a_number"), F.col("b_number"),
            F.date_trunc("minute", F.col("event_ts")).alias("call_minute"),
            F.col("duration_sec").alias("their_duration"),
            F.col("charged_units").alias("their_units"),
            F.col("charge_amount").alias("their_charge"),
            F.col("currency").alias("their_currency"),
        )
    )

    J.assert_not_empty(ours, "our ledger")
    J.assert_not_empty(theirs, "partner ledger")

    # ---------------------------------------------------------------- FX to EUR
    # Small dimension, broadcast so it never participates in the skewed shuffle.
    fx = (
        spark.table(args.fx_table)
        .where(F.col("rate_date") == F.lit(run_date))
        .select("currency", "rate_to_eur")
    )
    theirs_eur = (
        theirs.join(F.broadcast(fx),
                    theirs.their_currency == fx.currency, "left")
        .withColumn("their_charge_eur",
                    F.round(F.col("their_charge") * F.coalesce(F.col("rate_to_eur"),
                                                              F.lit(1.0)), 5))
        .drop("currency", "rate_to_eur")
    )
    missing_fx = theirs_eur.where(F.col("their_charge_eur").isNull()).count()
    if missing_fx:
        J.fail(f"{missing_fx:,} partner rows have no FX rate for {run_date} — "
               "a settlement computed at rate 1.0 would be materially wrong")

    # ---------------------------------------------------------------- find hot keys
    # Discovered, not hard-coded, so it adapts as traffic changes.
    hot = (
        ours.groupBy("b_number").count()
        .where(F.col("count") > HOT_KEY_ROW_THRESHOLD)
        .select("b_number")
        .cache()
    )
    hot_n = hot.count()
    J.step(f"{hot_n} hot b_numbers above {HOT_KEY_ROW_THRESHOLD:,} rows "
           f"(salting these into {SALT_BUCKETS} buckets)")

    def salt(df, side: str):
        """Salt only the hot keys; everything else gets bucket 0.

        Salting every key would multiply the small side by 32 for no benefit.
        Salting only the hot ones keeps the explosion proportional to the
        actual problem.
        """
        marked = df.join(F.broadcast(hot), "b_number", "left_outer") \
                   .withColumn("_is_hot", F.col("b_number").isNotNull()
                               & F.lit(hot_n > 0))
        if side == "left":
            return marked.withColumn(
                "_salt",
                F.when(F.col("_is_hot"), (F.rand() * SALT_BUCKETS).cast("int"))
                 .otherwise(F.lit(0))).drop("_is_hot")
        # The right side must be replicated across every bucket for hot keys,
        # otherwise the salted left rows find no partner.
        return (marked
                .withColumn("_salt",
                            F.when(F.col("_is_hot"),
                                   F.explode(F.array([F.lit(i) for i in range(SALT_BUCKETS)])))
                             .otherwise(F.lit(0)))
                .drop("_is_hot"))

    left = salt(ours, "left")
    right = salt(theirs_eur, "right")

    # ---------------------------------------------------------------- the match
    join_keys = ["partner_code", "a_number", "b_number", "call_minute", "_salt"]
    matched = (
        left.join(right, join_keys, "full_outer")
        .withColumn("match_status",
                    F.when(F.col("our_duration").isNull(), "MISSING_ON_OUR_SIDE")
                     .when(F.col("their_duration").isNull(), "MISSING_ON_THEIR_SIDE")
                     .when(F.abs(F.col("our_duration") - F.col("their_duration"))
                           <= args.duration_tolerance_sec, "MATCHED")
                     .otherwise("DURATION_MISMATCH"))
        .drop("_salt")
    )

    # ---------------------------------------------------------------- aggregate
    variance = (
        matched.groupBy("partner_code")
        .agg(
            F.lit(run_date).cast("date").alias("settlement_date"),
            F.count("*").alias("total_legs"),
            F.sum(F.when(F.col("match_status") == "MATCHED", 1).otherwise(0))
             .alias("matched_legs"),
            F.sum(F.when(F.col("match_status") == "MISSING_ON_OUR_SIDE", 1).otherwise(0))
             .alias("missing_ours"),
            F.sum(F.when(F.col("match_status") == "MISSING_ON_THEIR_SIDE", 1).otherwise(0))
             .alias("missing_theirs"),
            F.sum(F.when(F.col("match_status") == "DURATION_MISMATCH", 1).otherwise(0))
             .alias("duration_mismatches"),
            F.round(F.sum(F.coalesce(F.col("our_duration"), F.lit(0))) / 60.0, 2)
             .alias("our_minutes"),
            F.round(F.sum(F.coalesce(F.col("their_duration"), F.lit(0))) / 60.0, 2)
             .alias("their_minutes"),
            F.round(F.sum(F.coalesce(F.col("our_charge"), F.lit(0.0))), 2)
             .alias("our_charge_eur"),
            F.round(F.sum(F.coalesce(F.col("their_charge_eur"), F.lit(0.0))), 2)
             .alias("their_charge_eur"),
        )
        .withColumn("match_rate",
                    F.round(F.col("matched_legs") / F.greatest(F.col("total_legs"),
                                                               F.lit(1)), 5))
        .withColumn("variance_eur",
                    F.round(F.col("our_charge_eur") - F.col("their_charge_eur"), 2))
        .withColumn("computed_at", F.current_timestamp())
    )

    for r in variance.orderBy(F.abs(F.col("variance_eur")).desc()).collect():
        J.step(f"{r['partner_code']:>8}: legs={r['total_legs']:,} "
               f"match_rate={r['match_rate']:.4f} variance=EUR {r['variance_eur']:,.2f}")

    (variance.writeTo(args.target_table)
             .option("write.distribution-mode", "hash")
             .overwritePartitions())
    J.describe_commit(spark, args.target_table, "interconnect_variance")

    hot.unpersist()
    J.banner(f"reconciliation complete for {run_date}")
    spark.stop()


if __name__ == "__main__":
    main()
