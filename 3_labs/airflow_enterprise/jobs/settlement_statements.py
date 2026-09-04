"""
settlement_statements.py — called by DAG 04, TaskGroup
monthly_close/settlement_statements

Rolls the daily variance rows into one signed statement per partner per month,
which is the document finance actually sends.

    spark-submit settlement_statements.py --month 2026-09 \
        --output s3://telco-prod-lakehouse/finance/statements/

WHY IT WRITES A HASH

The statement is a financial document. Anyone can re-run this job later and
must get byte-identical numbers, and if they do not, the discrepancy has to be
detectable. Each statement therefore carries:

    * the Iceberg SNAPSHOT ID of the source table it was computed from, so the
      exact input can be reconstructed with time travel
    * a SHA-256 over the sorted statement rows

That pair is what turns "we think the number was right" into "here is the
input, here is the checksum, reproduce it yourself". It is the same evidence
discipline the Iceberg labs teach, applied to money.
"""
from __future__ import annotations

import hashlib

from pyspark.sql import functions as F

import job_common as J


def main() -> None:
    p = J.base_args(__doc__)
    p.add_argument("--month", required=True, help="YYYY-MM")
    p.add_argument("--output", required=True)
    p.add_argument("--source-table", default=None)
    p.add_argument("--statement-table", default=None)
    args = p.parse_args()

    spark = J.build_spark(f"settlement-statements-{args.month}")
    src = args.source_table or J.fq("telco_prod_gold", "interconnect_variance")
    stmt_table = args.statement_table or J.fq("telco_prod_gold", "settlement_statement")

    month_start = f"{args.month}-01"
    J.banner(f"monthly settlement statements for {args.month}")

    # The snapshot this statement is computed from — recorded, not assumed.
    src_snapshot = J.snapshot_id(spark, src)
    J.step(f"source snapshot: {src_snapshot}")

    daily = (
        spark.table(src)
        .where(F.date_format(F.col("settlement_date"), "yyyy-MM") == F.lit(args.month))
    )
    days = daily.select("settlement_date").distinct().count()
    J.assert_not_empty(daily, f"daily variance rows for {args.month}")

    # A month with missing days would understate what we owe. Refuse it.
    import calendar
    y, m = int(args.month[:4]), int(args.month[5:7])
    expected_days = calendar.monthrange(y, m)[1]
    if days < expected_days:
        J.fail(f"only {days} of {expected_days} days present for {args.month} — "
               "a statement computed on a partial month would be wrong. "
               "Backfill the missing days of DAG 04 first.")

    statement = (
        daily.groupBy("partner_code")
        .agg(
            F.lit(month_start).cast("date").alias("statement_month"),
            F.sum("our_minutes").alias("our_minutes"),
            F.sum("their_minutes").alias("their_minutes"),
            F.round(F.sum("our_charge_eur"), 2).alias("we_billed_eur"),
            F.round(F.sum("their_charge_eur"), 2).alias("they_billed_eur"),
            F.round(F.sum("variance_eur"), 2).alias("net_variance_eur"),
            F.sum("total_legs").alias("total_legs"),
            F.sum("matched_legs").alias("matched_legs"),
            F.sum("missing_ours").alias("missing_ours"),
            F.sum("missing_theirs").alias("missing_theirs"),
            F.countDistinct("settlement_date").alias("days_included"),
        )
        .withColumn("match_rate",
                    F.round(F.col("matched_legs")
                            / F.greatest(F.col("total_legs"), F.lit(1)), 5))
        # Positive = they owe us, negative = we owe them.
        .withColumn("settlement_direction",
                    F.when(F.col("net_variance_eur") > 0, "PARTNER_OWES_US")
                     .when(F.col("net_variance_eur") < 0, "WE_OWE_PARTNER")
                     .otherwise("BALANCED"))
        .withColumn("source_snapshot_id", F.lit(str(src_snapshot)))
        .withColumn("generated_at", F.current_timestamp())
        .orderBy("partner_code")
    )

    rows = statement.collect()

    # Deterministic checksum over the numbers that matter, in a fixed order.
    digest = hashlib.sha256()
    for r in rows:
        digest.update(
            f"{r['partner_code']}|{r['our_minutes']}|{r['their_minutes']}|"
            f"{r['we_billed_eur']}|{r['they_billed_eur']}|{r['net_variance_eur']}\n"
            .encode()
        )
    checksum = digest.hexdigest()
    J.step(f"statement checksum: {checksum}")

    final = statement.withColumn("statement_checksum", F.lit(checksum))

    # Human-readable copy for finance, one CSV per month.
    (final.coalesce(1).write.mode("overwrite").option("header", True)
          .csv(f"{args.output.rstrip('/')}/month={args.month}/"))

    # Durable copy in the lakehouse.
    if J.table_exists(spark, stmt_table):
        final.writeTo(stmt_table).overwritePartitions()
    else:
        final.writeTo(stmt_table).partitionedBy("statement_month").create()
    J.describe_commit(spark, stmt_table, "settlement_statement")

    total = sum(r["net_variance_eur"] for r in rows)
    for r in rows:
        J.step(f"{r['partner_code']:>8}: {r['settlement_direction']:>16} "
               f"EUR {abs(r['net_variance_eur']):>12,.2f}  "
               f"match_rate={r['match_rate']:.4f}")
    J.banner(f"{len(rows)} statements for {args.month} | net position "
             f"EUR {total:,.2f} | checksum {checksum[:16]}…")
    spark.stop()


if __name__ == "__main__":
    main()
