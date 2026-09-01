"""
05_aggregate.py  —  Family 5 of 8: AGGREGATE   (functions 16-19)

  16  groupBy + agg              the workhorse. This is where the SHUFFLE happens.
  17  count / countDistinct      count("*") and count(col) are different numbers
  18  approx_count_distinct      HyperLogLog. Combinable, therefore cheap.
  19  collect_set / collect_list rows -> array. Unbounded: can OOM an executor.

  BONUS  pivot                   long -> wide
"""
from common import get_spark, read_table, block, show, check
from pyspark.sql import functions as F

spark = get_spark("05_aggregate")
orders = read_table(spark, "silver", "orders")
items = read_table(spark, "silver", "order_items")

# one wide table to aggregate over
sales = (orders.filter(F.col("order_status") == "completed")
               .join(items, "order_id", "inner"))

# ===== 16 · groupBy + agg ==================================================
block("16", "groupBy + agg", "every groupBy is a shuffle. Filter BEFORE it, "
                             "never after.")
show(sales.groupBy("region", "department")
          .agg(F.count("*").alias("lines"),
               F.sum("net_amount").alias("revenue"),
               F.round(F.avg("net_amount"), 2).alias("avg_line"),
               F.min("net_amount").alias("min_line"),
               F.max("net_amount").alias("max_line"))
          .orderBy("region", "department"),
     label="revenue by region x department (completed orders only)")

print("Count the shuffles in the plan below - there should be exactly one:")
sales.groupBy("region").agg(F.sum("net_amount")).explain(mode="simple")

# ===== 17 · count / countDistinct ==========================================
block("17", "count / countDistinct", 'count("*") counts ROWS. count(col) counts '
                                     "NON-NULL VALUES. The gap is a data-quality metric.")
show(orders.groupBy("region").agg(
        F.count("*").alias("orders"),
        F.count("customer_email").alias("with_email"),
        (F.count("*") - F.count("customer_email")).alias("missing_email"),
        F.countDistinct("customer_id").alias("distinct_customers"),
        F.countDistinct("channel", "order_status").alias("distinct_combos"),
     ).orderBy("region"),
     label="look at NA: 5 orders but only 4 emails")

# ===== 18 · approx_count_distinct ==========================================
block("18", "approx_count_distinct", "exact distinct cannot be partially aggregated "
                                     "before the shuffle. HyperLogLog sketches can.")
show(orders.agg(
        F.countDistinct("customer_id").alias("exact"),
        F.approx_count_distinct("customer_id").alias("approx_default_5pct"),
        F.approx_count_distinct("customer_id", 0.01).alias("approx_1pct_rsd")),
     label="identical here because the set is tiny - at 900M rows the cost is not")
print("At scale: exact distinct ships every raw value across the network.")
print("approx ships a fixed-size sketch per partition. That is the whole difference.")

# ===== 19 · collect_set / collect_list =====================================
block("19", "collect_set / collect_list", "rows -> array. set = deduplicated, "
                                          "list = keeps duplicates and order.")
show(sales.groupBy("order_id").agg(
        F.collect_list("department").alias("departments_list"),
        F.collect_set("department").alias("departments_set"),
        F.sort_array(F.collect_set("sku")).alias("skus_sorted"),
        F.size(F.collect_set("department")).alias("n_distinct_depts"),
     ).orderBy("order_id"),
     label="ORD-1007 bought from 3 departments")
print("DANGER: these arrays are UNBOUNDED and built in ONE executor's memory")
print("per group. One hot key (region='NA' at 900M rows) is an OOM waiting.")

# ===== BONUS · pivot =======================================================
block("B1", "pivot  (bonus, not one of the 30)", "long -> wide. ALWAYS pass the "
                                                 "value list, or Spark runs an extra job to discover it.")
show(sales.groupBy("region").pivot("department",
        ["Grocery", "Electronics", "Apparel", "Home", "Beauty"])
        .agg(F.round(F.sum("net_amount"), 2)).orderBy("region"),
     label="revenue matrix: region x department")

# ===== verify ==============================================================
block("--", "VERIFY")
na = orders.filter("region = 'NA'")
check(na.count() == 5 and na.filter(F.col("customer_email").isNotNull()).count() == 4,
      'count("*")=5 vs count(email)=4 for region NA')
check(orders.agg(F.countDistinct("customer_id")).first()[0] == 7,
      "7 distinct customers across 11 orders")
_r = (sales.groupBy("order_id").agg(F.size(F.collect_set("department")).alias("n"))
           .filter("order_id = 'ORD-1007'").first())
check(_r["n"] == 3, "ORD-1007 spans 3 distinct departments")
check(sales.groupBy("region").agg(F.sum("net_amount")).count() == 3,
      "3 region groups survive the shuffle")
check(sales.count() < orders.join(items, "order_id").count(),
      "filtering to completed orders BEFORE the join shrank the join input")
spark.stop()
