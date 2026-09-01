"""
07_windows.py  —  Family 7 of 8: WINDOW ANALYTICS   (functions 25-27)

  25  Window.partitionBy/orderBy + row_number   1,2,3,4 - no ties
  26  rank / dense_rank                         1,2,2,4  vs  1,2,2,3
  27  lag / lead + rowsBetween                  previous/next row, running totals

If you have written OVER (PARTITION BY ... ORDER BY ...) in Redshift,
Snowflake or Postgres, you already know this family. Same semantics.

THE MISTAKE THAT HALVES YOUR CLUSTER: a Window with no partitionBy sends
EVERY row to ONE executor to establish global order. 319 cores idle.
"""
from common import get_spark, read_table, block, show, check
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = get_spark("07_windows")
orders = read_table(spark, "silver", "orders")
items = read_table(spark, "silver", "order_items")

sales = (orders.filter("order_status = 'completed'")
               .join(items, "order_id", "inner"))

dept = (sales.groupBy("region", "department")
             .agg(F.round(F.sum("net_amount"), 2).alias("revenue"))
             .cache())
show(dept.orderBy("region", F.col("revenue").desc()), label="the base table for this file")

# ===== 25 · Window + row_number ============================================
block("25", "Window.partitionBy / orderBy + row_number", "rank within each group. "
      "SQL: ROW_NUMBER() OVER (PARTITION BY region ORDER BY revenue DESC)")
w_rev = Window.partitionBy("region").orderBy(F.col("revenue").desc())
show(dept.withColumn("rn", F.row_number().over(w_rev)).orderBy("region", "rn"),
     label="row_number restarts at 1 inside every region")
show(dept.withColumn("rn", F.row_number().over(w_rev)).filter("rn <= 2")
         .orderBy("region", "rn"),
     label="top-2 department per region - the single most common window pattern")

# ===== 26 · rank / dense_rank ==============================================
block("26", "rank / dense_rank", "how ties are numbered. Build a tie to see it.")
tied = spark.createDataFrame(
    [("NA", "A", 100.0), ("NA", "B", 90.0), ("NA", "C", 90.0), ("NA", "D", 80.0)],
    ["region", "dept", "revenue"])
w_t = Window.partitionBy("region").orderBy(F.col("revenue").desc())
show(tied.withColumn("row_number", F.row_number().over(w_t))
         .withColumn("rank", F.rank().over(w_t))
         .withColumn("dense_rank", F.dense_rank().over(w_t))
         .withColumn("percent_rank", F.round(F.percent_rank().over(w_t), 3))
         .withColumn("ntile_2", F.ntile(2).over(w_t)),
     label="B and C tie at 90: row_number 2,3 | rank 2,2 then 4 | dense_rank 2,2 then 3")

# ===== 27 · lag / lead / rowsBetween =======================================
block("27", "lag / lead / rowsBetween", "look at neighbouring rows, and accumulate")
w_time = Window.partitionBy("customer_id").orderBy("order_ts")
w_run = (Window.partitionBy("customer_id").orderBy("order_ts")
         .rowsBetween(Window.unboundedPreceding, Window.currentRow))

cust = orders.select("customer_id", "order_id", "order_ts", "region")
show(cust.withColumn("prev_order", F.lag("order_id").over(w_time))
         .withColumn("next_order", F.lead("order_id").over(w_time))
         .withColumn("mins_since_prev",
                     F.round((F.col("order_ts").cast("long")
                              - F.lag("order_ts").over(w_time).cast("long")) / 60, 1))
         .withColumn("seq", F.row_number().over(w_time))
         .orderBy("customer_id", "order_ts"),
     label="lag/lead are NULL at the edges of each partition - that is correct, not a bug")

spend = sales.select("customer_id", "order_id", "order_ts", "net_amount")
show(spend.withColumn("running_total",
                      F.round(F.sum("net_amount").over(w_run), 2))
          .withColumn("orders_so_far", F.count("*").over(w_run))
          .orderBy("customer_id", "order_ts").limit(12),
     label="running total per customer via rowsBetween(unboundedPreceding, currentRow)")

# ---- the mistake ----------------------------------------------------------
print("\nA window with NO partitionBy - read the warning Spark itself prints:")
w_bad = Window.orderBy("order_ts")
orders.withColumn("global_seq", F.row_number().over(w_bad)).explain(mode="simple")

# ===== verify ==============================================================
block("--", "VERIFY")
top = dept.withColumn("rn", F.row_number().over(w_rev)).filter("rn = 1")
check(top.count() == 3, "exactly one #1 department per region (3 regions)")
_t = tied.withColumn("r", F.rank().over(w_t)).withColumn("d", F.dense_rank().over(w_t))
check([r["r"] for r in _t.orderBy("revenue", ascending=False).collect()] == [1, 2, 2, 4],
      "rank leaves a gap after a tie: 1,2,2,4")
check([r["d"] for r in _t.orderBy("revenue", ascending=False).collect()] == [1, 2, 2, 3],
      "dense_rank does not: 1,2,2,3")
check(cust.withColumn("p", F.lag("order_id").over(w_time))
          .filter(F.col("p").isNull()).count()
      == cust.select("customer_id").distinct().count(),
      "lag is NULL exactly once per customer - the first row of each partition")
# NOTE: several rows share one order_ts (an order fans out to many items), so
# "the last row" is ambiguous. max() of the running total is the unambiguous
# way to ask for the final value in each partition.
_last = (spend.withColumn("rt", F.sum("net_amount").over(w_run))
              .groupBy("customer_id").agg(F.max("rt").alias("final"))
              .agg(F.round(F.sum("final"), 2)).first()[0])
check(abs(_last - spend.agg(F.round(F.sum("net_amount"), 2)).first()[0]) < 0.01,
      "the final running total per customer sums back to the grand total")
spark.stop()
