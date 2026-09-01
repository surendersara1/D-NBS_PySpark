"""
02_filter.py  —  Family 2 of 8: FILTER & CONDITIONAL   (functions 05-08)

  05  filter / where              keep rows. Identical methods.
  06  when / otherwise            SQL CASE WHEN
  07  coalesce                    first non-null of several columns
  08  fillna / dropna             bulk null handling
"""
from common import get_spark, read_table, block, show, check
from pyspark.sql import functions as F

spark = get_spark("02_filter")
orders = read_table(spark, "silver", "orders")
items = read_table(spark, "silver", "order_items")

# ===== 05 · filter / where =================================================
block("05", "filter / where", "the same method under two names. Pick one.")
show(orders.filter(F.col("region") == "NA").select("order_id", "region", "order_status"),
     label='filter: region == "NA"')

# & | ~ with brackets. NOT and/or/not - those raise.
show(orders.filter((F.col("region") == "EMEA") & (F.col("order_status") == "completed"))
           .select("order_id", "region", "order_status"),
     label="AND -> &   (each side in its own brackets)")
show(orders.filter(F.col("order_status").isin("refunded", "cancelled"))
           .select("order_id", "order_status"), label="isin")
show(items.filter(F.col("qty").between(1, 2)).select("order_id", "sku", "qty").limit(4),
     label="between (inclusive on both ends)")

# NULL is not a value. == None never matches.
print("orders where customer_email == None :",
      orders.filter(F.col("customer_email") == None).count(), " <- always 0")
print("orders where customer_email isNull():",
      orders.filter(F.col("customer_email").isNull()).count(), " <- correct")

# ===== 06 · when / otherwise ===============================================
block("06", "when / otherwise", "CASE WHEN. Chain as many .when() as you need.")
show(items.withColumn("band",
        F.when(F.col("qty") < 0, "return")
         .when(F.col("net_amount") >= 500, "high_value")
         .when(F.col("net_amount") >= 50, "standard")
         .otherwise("small"))
        .select("order_id", "sku", "qty", "net_amount", "band"),
     label="four-way classification")

# No otherwise() -> unmatched rows get NULL, not an error.
show(items.select("sku", F.when(F.col("qty") > 3, "bulk").alias("no_otherwise")).limit(4),
     label="omitting otherwise() yields NULL")

# ===== 07 · coalesce =======================================================
block("07", "coalesce", "first non-null argument. The null-fallback function - "
                        "unrelated to the .coalesce() partition method (#30).")
show(orders.select("order_id", "customer_email",
        F.coalesce(F.col("customer_email"),
                   F.concat(F.col("customer_id"), F.lit("@no-email.local")),
                   F.lit("UNKNOWN")).alias("contact")),
     label="cascade through three fallbacks")

# ===== 08 · fillna / dropna ================================================
block("08", "fillna / dropna", "bulk null handling. Type-aware: a string default "
                               "is ignored on numeric columns and vice versa.")
holes = orders.select("order_id", "customer_email", "channel", "promo_codes")
show(holes, label="before")
show(holes.na.fill({"customer_email": "missing@unknown", "channel": "unknown"}),
     label='na.fill with a dict - the ONLY form you should use')
show(holes.na.drop(subset=["customer_email"]),
     label="na.drop(subset=[...]) - drops 1 row")
print("na.drop(how='any') over all columns keeps:",
      holes.na.drop(how="any").count(), "of", holes.count(),
      " <- 'any' is aggressive; almost always pass subset=")

# ===== verify ==============================================================
block("--", "VERIFY")
check(orders.filter("region = 'NA'").count() == orders.filter(F.col("region") == "NA").count(),
      "SQL-string and Column filters agree")
check(orders.filter(F.col("customer_email") == None).count() == 0,
      "== None matches nothing - always use isNull()")
check(orders.filter(F.col("customer_email").isNull()).count() == 1,
      "isNull() finds the 1 order with no email")
check(items.withColumn("b", F.when(F.col("qty") > 3, "bulk"))
           .filter(F.col("b").isNull()).count() > 0,
      "when() without otherwise() leaves NULLs")
check(orders.select(F.coalesce("customer_email", F.lit("x")).alias("c"))
            .filter(F.col("c").isNull()).count() == 0,
      "coalesce removed every null")
spark.stop()
