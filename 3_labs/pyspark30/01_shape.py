"""
01_shape.py  —  Family 1 of 8: SHAPE & SELECT   (functions 01-04)

  01  select                       pick columns; prunes the file read
  02  withColumn                   add or replace one column
  03  cast                         change a column's type
  04  withColumnRenamed / drop     rename and remove

Run the whole file, or copy one numbered block into a notebook.
    python 01_shape.py
"""
from common import get_spark, read_table, block, show, check
from pyspark.sql import functions as F

spark = get_spark("01_shape")
orders = read_table(spark, "silver", "orders")
items = read_table(spark, "silver", "order_items")

# ===== 01 · select =========================================================
block("01", "select", "pick columns. On Parquet/Iceberg this changes how many "
                      "bytes leave S3 - it is not cosmetic.")
show(orders.select("order_id", "region", "channel", "order_status").limit(5),
     label="four columns of eleven")

# three ways to name a column; all identical
show(orders.select(
        F.col("order_id"),                                  # explicit
        orders["region"],                                   # bracket
        F.col("order_status").alias("status"),              # renamed inline
        (F.col("order_id") == "ORD-1001").alias("is_first"),  # an expression
     ).limit(3), label="col / bracket / alias / expression")

print("Columns actually read by Spark:")
orders.select("order_id", "region").explain(mode="formatted")

# ===== 02 · withColumn =====================================================
block("02", "withColumn", "add a column, or replace one by reusing its name")
show(orders.select("order_id", "region")
           .withColumn("region_lc", F.lower("region"))          # NEW column
           .withColumn("region", F.concat(F.lit("R-"), "region"))  # REPLACED
           .limit(4), label="one added, one replaced")

# Adding many columns: one withColumns() beats forty chained withColumn()
show(orders.select("order_id")
           .withColumns({"layer": F.lit("silver"),
                         "loaded_by": F.lit("00_setup"),
                         "is_na": F.col("order_id").startswith("ORD-10")})
           .limit(3), label="withColumns - one plan node, not three")

# ===== 03 · cast ===========================================================
block("03", "cast", "change type. What a BAD value does depends on ANSI mode - "
                    "read the note below, it bites people moving Spark versions.")
show(items.select("order_id", "unit_price", "qty")
          .withColumn("price_int", F.col("unit_price").cast("int"))     # truncates
          .withColumn("qty_str", F.col("qty").cast("string"))
          .withColumn("price_dec", F.col("unit_price").cast("decimal(10,2)"))
          .limit(4), label="double -> int / string / decimal")

# ---------------------------------------------------------------------------
# cast() on a malformed value behaves DIFFERENTLY depending on ANSI mode:
#
#   spark.sql.ansi.enabled = false   -> returns NULL silently
#        (the default in Spark 3.x, and therefore in AWS Glue 5.x / EMR 7.x)
#   spark.sql.ansi.enabled = true    -> raises CAST_INVALID_INPUT
#        (the default in Spark 4.0+)
#
# try_cast() returns NULL in BOTH modes. Use it when you mean "best effort".
# ---------------------------------------------------------------------------
bad = spark.createDataFrame([("42",), ("not_a_number",), (None,)], ["v"])
print("ansi.enabled =", spark.conf.get("spark.sql.ansi.enabled"))
show(bad.withColumn("try_cast", F.col("v").try_cast("int")),
     label="try_cast - NULL on failure, in every Spark version")

spark.conf.set("spark.sql.ansi.enabled", "false")
show(bad.withColumn("cast_ansi_off", F.col("v").cast("int")),
     label="cast with ANSI off - the silent NULL. This is how bad data disappears.")
spark.conf.set("spark.sql.ansi.enabled", "true")
try:
    bad.withColumn("cast_ansi_on", F.col("v").cast("int")).collect()
except Exception as e:
    print("cast with ANSI on  ->", type(e).__name__,
          "- CAST_INVALID_INPUT. The job FAILS instead of hiding the row.")
spark.conf.unset("spark.sql.ansi.enabled")

# ===== 04 · withColumnRenamed / drop =======================================
block("04", "withColumnRenamed / drop", "rename and remove")
tidy = (orders
        .withColumnRenamed("customer_email", "email")
        .withColumnRenamed("order_ts", "event_time")
        .drop("promo_codes", "ingested_at", "currency"))
print("before:", len(orders.columns), "columns ->", orders.columns)
print("after :", len(tidy.columns), "columns ->", tidy.columns)
show(tidy.limit(3), label="renamed and slimmed")

# ===== verify ==============================================================
block("--", "VERIFY")
check(orders.select("order_id", "region").columns == ["order_id", "region"],
      "select returns exactly the requested columns, in order")
check("region_lc" in orders.withColumn("region_lc", F.lower("region")).columns,
      "withColumn adds a column")
check(len(orders.withColumn("region", F.lower("region")).columns) == len(orders.columns),
      "withColumn with an existing name REPLACES, it does not duplicate")
check(items.withColumn("x", F.col("sku").try_cast("int"))
           .filter("x IS NOT NULL").count() == 0,
      "try_cast produces NULL on failure in every Spark version")
check("promo_codes" not in tidy.columns, "drop removed the column")
spark.stop()
