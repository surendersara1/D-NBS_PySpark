"""
03_strings.py  —  Family 3 of 8: STRINGS   (functions 09-12)

  09  trim / lower / upper / initcap    normalisation
  10  split (+ getItem)                 string -> array -> element
  11  regexp_replace / regexp_extract   pattern work
  12  concat_ws / concat                joining columns

Spark ships ~90 string functions, all compiled to JVM bytecode. Reach for a
Python UDF here and you give up 10-100x for no benefit.
"""
from common import get_spark, read_table, block, show, check
from pyspark.sql import functions as F

spark = get_spark("03_strings")
bronze = read_table(spark, "bronze", "orders_raw")   # the DIRTY version
orders = read_table(spark, "silver", "orders")
items = read_table(spark, "silver", "order_items")

# ===== 09 · trim / lower / upper / initcap =================================
block("09", "trim / lower / upper / initcap", "run against BRONZE so you can see "
                                              "the dirt these functions remove")
show(bronze.select("order_id",
        F.concat(F.lit("["), F.col("customer_email"), F.lit("]")).alias("raw_email"),
        F.lower(F.trim("customer_email")).alias("clean_email"),
        F.concat(F.lit("["), F.col("region"), F.lit("]")).alias("raw_region"),
        F.upper(F.trim("region")).alias("clean_region")).limit(6),
     label="brackets added so you can SEE the padding")
show(items.select("department", F.initcap(F.lower("department")).alias("initcap")).limit(3),
     label="initcap - capitalises each word")
print("distinct regions in BRONZE:", bronze.select("region").distinct().count(),
      "| in SILVER after trim+upper:", orders.select("region").distinct().count())

# ===== 10 · split ==========================================================
block("10", "split", "string -> array. Then [n] or .getItem(n) to pull an element.")
show(orders.select("order_id", "customer_email",
        F.split("customer_email", "@").alias("parts"),
        F.split("customer_email", "@").getItem(0).alias("local_part"),
        F.split("customer_email", "@")[1].alias("domain"),
        F.size(F.split("customer_email", "@")).alias("n_parts")).limit(6),
     label="email -> local part + domain")
show(items.select("sku",
        F.split("sku", "-").getItem(1).alias("dept_code"),
        F.split("sku", "-").getItem(2).alias("item_no")).limit(4),
     label="splitting a structured key")

# ===== 11 · regexp_replace / regexp_extract ================================
block("11", "regexp_replace / regexp_extract", "replace vs capture")
show(items.select("sku",
        F.regexp_replace("sku", "[^A-Za-z0-9]", "").alias("stripped"),
        F.regexp_replace("sku", "^SKU-", "").alias("no_prefix"),
        F.regexp_extract("sku", r"SKU-([A-Z]{2})-(\d{3})", 1).alias("grp1_dept"),
        F.regexp_extract("sku", r"SKU-([A-Z]{2})-(\d{3})", 2).alias("grp2_num")).limit(4),
     label="regexp_extract group 0 = whole match, 1 = first capture group")
show(orders.select("customer_email",
        F.regexp_extract("customer_email", r"@(.+)$", 1).alias("domain")).limit(4),
     label="no match -> empty string, never an error")

# ===== 12 · concat_ws / concat =============================================
block("12", "concat_ws / concat", "concat_ws SKIPS nulls. concat returns NULL if "
                                  "ANY input is null. This surprises everyone once.")
show(orders.select("order_id", "region", "channel", "customer_email",
        F.concat_ws(" | ", "region", "channel", "customer_email").alias("concat_ws"),
        F.concat(F.col("region"), F.lit(" | "), F.col("customer_email")).alias("concat")
     ).limit(6), label="look at ORD-1009 (null email): concat_ws survives, concat does not")
show(orders.select(F.concat_ws("-", "region", "order_date").alias("surrogate_key")).limit(3),
     label="building a composite key")

# ===== verify ==============================================================
block("--", "VERIFY")
check(bronze.select("region").distinct().count() == 7
      and orders.select("region").distinct().count() == 3,
      "trim+upper collapsed 7 raw region spellings into 3")
check(orders.filter(F.split("customer_email", "@").getItem(1) == "example.com").count() == 2,
      "split+getItem found 2 orders on the example.com domain")
check(items.filter(F.regexp_extract("sku", r"SKU-([A-Z]{2})-", 1) == "EL").count() == 4,
      "regexp_extract capture group found 4 Electronics SKUs")
_row = orders.filter("order_id = 'ORD-1009'").select(
    F.concat_ws(" | ", "region", "customer_email").alias("ws"),
    F.concat(F.col("region"), F.col("customer_email")).alias("c")).first()
check(_row["ws"] == "NA" and _row["c"] is None,
      "concat_ws skipped the null; concat returned NULL for the whole row")
spark.stop()
