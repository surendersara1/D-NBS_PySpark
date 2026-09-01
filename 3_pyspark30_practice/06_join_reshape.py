"""
06_join_reshape.py  —  Family 6 of 8: JOIN & RESHAPE   (functions 20-24)

  20  join                       inner / left / right / full
  21  broadcast                  ship the small side everywhere. Kills the shuffle.
  22  left_anti / left_semi      "no match" and "has a match" - the QA workhorses
  23  unionByName                stack by NAME. union() is positional and dangerous.
  24  explode / explode_outer    array -> one row per element
"""
from common import get_spark, read_table, block, show, check
from pyspark.sql import functions as F

spark = get_spark("06_join_reshape")
orders = read_table(spark, "silver", "orders")
items = read_table(spark, "silver", "order_items")

# ===== 20 · join ===========================================================
block("20", "join", "on= a shared column name gives ONE joined column. "
                    "on= an expression gives BOTH.")
show(orders.select("order_id", "region", "order_status")
           .join(items.select("order_id", "line_no", "sku", "net_amount"),
                 on="order_id", how="inner")
           .orderBy("order_id", "line_no").limit(8),
     label="inner: only order_ids present on BOTH sides")

o = orders.select("order_id", "region")
i = items.select("order_id", "sku", "net_amount")
for how in ["inner", "left", "right", "full"]:
    print(f"{how:>6}: {o.join(i, 'order_id', how).count():>3} rows")
print("  orders=12  items=18 | ORD-9999 is an orphan item, ORD-1012 shipped nothing")

show(o.join(i, "order_id", "full")
      .filter(F.col("region").isNull() | F.col("sku").isNull())
      .orderBy("order_id"),
     label="the full-outer join exposes BOTH sides' unmatched rows")

# ===== 21 · broadcast ======================================================
block("21", "broadcast", "if one side fits in executor memory, ship it everywhere "
                         "and the shuffle disappears. Highest leverage word in PySpark.")
dim = orders.select("order_id", "region", "customer_id")   # the 'small' side

# Our tables are tiny, so Spark ALREADY auto-broadcasts them (both sides are far
# under the 10 MB threshold). To see the difference the hint makes, we first turn
# auto-broadcast OFF - which is exactly the situation you are in at real scale
# when Spark cannot estimate a side's size.
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
print("--- WITHOUT broadcast (auto-broadcast disabled) ---")
items.join(dim, "order_id").explain(mode="simple")
print("\n--- WITH an explicit broadcast() hint - it wins even with auto OFF ---")
items.join(F.broadcast(dim), "order_id").explain(mode="simple")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")
print("\nNote: at THIS data size Spark auto-broadcasts anyway - we disabled it above")
print("to make the contrast visible. At real scale, on a side Spark cannot size,")
print("the explicit hint is what saves you the shuffle.")
print("Threshold: spark.sql.autoBroadcastJoinThreshold =",
      spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))

# ===== 22 · left_anti / left_semi ==========================================
block("22", "left_anti / left_semi", "anti = rows with NO match. semi = rows WITH "
                                     "a match, no extra columns. Neither duplicates rows.")
show(items.join(orders, "order_id", "left_anti").orderBy("order_id"),
     label="ORPHANS: order_items with no parent order. Your daily QA check.")
show(orders.join(items, "order_id", "left_anti").select("order_id", "region", "order_status"),
     label="orders that shipped nothing")
print("left_semi row count:", orders.join(items, "order_id", "left_semi").count(),
      "| inner join row count:", orders.join(items, "order_id", "inner").count())
print("semi does NOT fan out - that is the point of it.")

# ===== 23 · unionByName ====================================================
block("23", "unionByName", "matches by NAME. union() matches by POSITION and will "
                           "cheerfully put emails in the amount column.")
na = orders.filter("region = 'NA'").select("order_id", "region", "order_status")
emea = orders.filter("region = 'EMEA'").select("order_status", "order_id", "region")  # DIFFERENT ORDER
show(na.unionByName(emea).orderBy("order_id"), label="unionByName - correct")
show(na.union(emea).orderBy("order_id").limit(4),
     label="union - SAME code, columns silently scrambled. Look at region.")

late = orders.filter("region = 'APAC'").select("order_id", "region")   # missing a column
show(na.unionByName(late, allowMissingColumns=True).orderBy("order_id").limit(8),
     label="allowMissingColumns=True fills the gap with NULL")

# ===== 24 · explode / explode_outer ========================================
block("24", "explode / explode_outer", "one array element per row. explode DROPS "
                                       "rows whose array is empty or null; explode_outer keeps them.")
show(orders.select("order_id", "promo_codes",
                   F.explode("promo_codes").alias("promo")).orderBy("order_id"),
     label="explode - 12 orders become 8 promo rows; the 6 with no promos VANISH")
show(orders.select("order_id", "promo_codes",
                   F.explode_outer("promo_codes").alias("promo")).orderBy("order_id"),
     label="explode_outer - 14 rows: the same 8, plus the 6 orders kept with promo=NULL")
show(orders.select("order_id", F.posexplode("promo_codes").alias("pos", "promo"))
           .orderBy("order_id"),
     label="posexplode also gives you the array index")

# ===== verify ==============================================================
block("--", "VERIFY")
check(o.join(i, "order_id", "inner").count() == 17,
      "inner join drops the orphan item -> 17 rows")
check(o.join(i, "order_id", "left").count() == 18,
      "left join keeps ORD-1012 (no items) -> 18 rows")
check(items.join(orders, "order_id", "left_anti").count() == 1,
      "exactly 1 orphan order_item (ORD-9999)")
check(orders.join(items, "order_id", "left_semi").count()
      < orders.join(items, "order_id", "inner").count(),
      "left_semi does not fan out; inner does")
check(orders.select(F.explode("promo_codes")).count() == 8
      and orders.select(F.explode_outer("promo_codes")).count() == 14,
      "explode gives 8 rows (6 orders silently dropped); explode_outer gives 14")
spark.stop()
