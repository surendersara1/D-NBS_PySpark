"""
08_control.py  —  Family 8 of 8: DEDUP, ORDER, PARTITION CONTROL   (28-30)

  28  dropDuplicates / distinct        and why neither is enough on its own
  29  orderBy / sortWithinPartitions   global sort vs local sort
  30  repartition / coalesce           the two ways to change partition count

  BONUS  cache / unpersist             only when you reuse a DataFrame 3+ times
"""
from common import get_spark, read_table, block, show, check
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = get_spark("08_control")
bronze = read_table(spark, "bronze", "orders_raw")     # still contains the duplicate
orders = read_table(spark, "silver", "orders")
items = read_table(spark, "silver", "order_items")

# ===== 28 · dropDuplicates / distinct ======================================
block("28", "dropDuplicates / distinct", "bronze still holds ORD-1002 twice - "
      "same order_id, different ingested_at, DIFFERENT order_status.")
show(bronze.filter("order_id = 'ORD-1002'")
           .select("order_id", "order_status", "ingested_at"),
     label="the duplicate: which one is correct?")

print("bronze rows                       :", bronze.count())
print("distinct() - all columns compared :", bronze.drop("_src_file", "_bronze_at").distinct().count(),
      " <- keeps BOTH; the rows are not identical")
print("dropDuplicates(['order_id'])      :", bronze.dropDuplicates(["order_id"]).count(),
      " <- keeps ONE, but WHICH one is arbitrary")

# The deterministic pattern. This is what 00_setup.py used.
w = Window.partitionBy("order_id").orderBy(F.col("ingested_at").desc())
deduped = (bronze.withColumn("_rn", F.row_number().over(w))
                 .filter("_rn = 1").drop("_rn"))
show(deduped.filter("order_id = 'ORD-1002'")
            .select("order_id", "order_status", "ingested_at"),
     label="window + row_number: deterministic, and you CHOOSE the winner")
print("Use dropDuplicates() only when the rows are genuinely interchangeable.")
print("Any time 'latest wins' matters, use the window. Every time.")

# ===== 29 · orderBy / sortWithinPartitions =================================
block("29", "orderBy / sortWithinPartitions", "orderBy is GLOBAL and costs a "
      "range-partitioning shuffle. sortWithinPartitions is local and nearly free.")
print("--- orderBy: look for 'rangepartitioning' in the Exchange ---")
items.orderBy("net_amount").explain(mode="simple")
print("\n--- sortWithinPartitions: no Exchange at all ---")
items.sortWithinPartitions("net_amount").explain(mode="simple")
show(items.select("order_id", "sku", "net_amount")
          .orderBy(F.col("net_amount").desc()).limit(5),
     label="top 5 line items by value")
print("Never sort just to make output files 'look tidy' before a write.")
print("The consumer will re-sort anyway and you paid a full shuffle for nothing.")

# ===== 30 · repartition / coalesce =========================================
block("30", "repartition / coalesce", "repartition = full shuffle, can grow, can "
      "rebalance skew. coalesce = merge only, no shuffle, can only shrink.")
print("partitions as read      :", items.rdd.getNumPartitions())
print("after repartition(6)    :", items.repartition(6).rdd.getNumPartitions())
print("after repartition by key:", items.repartition(4, "department").rdd.getNumPartitions())
print("after coalesce(1)       :", items.coalesce(1).rdd.getNumPartitions())

show(items.repartition(4, "department")
          .withColumn("pid", F.spark_partition_id())
          .groupBy("pid").agg(F.collect_set("department").alias("departments"),
                              F.count("*").alias("rows"))
          .orderBy("pid"),
     label="repartition(4,'department') co-locates each department on one partition")

print("--- repartition: an Exchange (shuffle) appears ---")
items.repartition(4).explain(mode="simple")
print("\n--- coalesce: no Exchange ---")
items.coalesce(1).explain(mode="simple")
print("\nRule of thumb: coalesce() before a write so you do not emit 200 tiny files.")
print("repartition() when the DATA itself is unbalanced, or you need more tasks.")

# ===== BONUS · cache / unpersist ===========================================
block("B2", "cache / unpersist  (bonus, not one of the 30)",
      "costs executor memory and evicts other things. Worth it at 3+ reuses.")
base = items.filter("qty > 0").select("order_id", "department", "net_amount")
base.cache()
base.count()                                     # materialise it
print("is cached:", base.is_cached)
print("reuse 1 - rows      :", base.count())
print("reuse 2 - revenue   :", round(base.agg(F.sum("net_amount")).first()[0], 2))
print("reuse 3 - departments:", base.select("department").distinct().count())
base.unpersist()
print("after unpersist     :", base.is_cached)
print("Cached for 3 reuses = good trade. Cached and used once = you made it slower.")

# ===== verify ==============================================================
block("--", "VERIFY")
check(bronze.dropDuplicates(["order_id"]).count() == 12,
      "dropDuplicates on order_id collapses 13 bronze rows to 12")
check(deduped.filter("order_id = 'ORD-1002'").first()["order_status"] == "REFUNDED",
      "the window keeps the LATEST ingested version deterministically")
check(items.repartition(6).rdd.getNumPartitions() == 6,
      "repartition sets an exact partition count")
check(items.coalesce(1).rdd.getNumPartitions() == 1,
      "coalesce merges down to 1 without a shuffle")
_p = (items.repartition(4, "department").withColumn("pid", F.spark_partition_id())
           .groupBy("department").agg(F.countDistinct("pid").alias("n")).collect())
check(all(r["n"] == 1 for r in _p),
      "every department lands on exactly ONE partition after repartition by key")
spark.stop()
