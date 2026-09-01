"""
04_dates.py  —  Family 4 of 8: DATES & TIME   (functions 13-15)

  13  to_date / to_timestamp                   string -> real temporal type
  14  date_trunc + year/month/hour/dayofweek   roll up and extract
  15  datediff / date_add / add_months         arithmetic

THE BUG YOU WILL SHIP: spark.sql.session.timeZone defaults to the JVM's
timezone. Your laptop is not UTC. The Glue container might be. Rows then land
in the wrong day= partition and daily counts are quietly wrong at midnight.
Nothing errors. Nothing alerts. Set it explicitly, in every job.
"""
from common import get_spark, read_table, block, show, check
from pyspark.sql import functions as F

spark = get_spark("04_dates")
bronze = read_table(spark, "bronze", "orders_raw")
orders = read_table(spark, "silver", "orders")

print("session timeZone =", spark.conf.get("spark.sql.session.timeZone"))

# ===== 13 · to_date / to_timestamp =========================================
block("13", "to_date / to_timestamp", "BRONZE stores order_ts as a STRING. "
                                      "Nothing temporal works until you convert it.")
print("bronze order_ts type:", dict(bronze.dtypes)["order_ts"])
print("silver order_ts type:", dict(orders.dtypes)["order_ts"])
show(bronze.select("order_id", "order_ts",
        F.to_timestamp("order_ts").alias("as_timestamp"),
        F.to_date("order_ts").alias("as_date")).limit(4),
     label="string -> timestamp -> date")
# Same ANSI caveat as cast() in file 01: with ansi.enabled=true (Spark 4
# default) a malformed date RAISES; with it false (Spark 3.5 / Glue 5.x
# default) it returns NULL. try_to_date returns NULL either way.
spark.conf.set("spark.sql.ansi.enabled", "false")
show(spark.createDataFrame([("30/08/2026",), ("2026-08-30",)], ["s"])
          .select("s",
                  F.to_date("s", "dd/MM/yyyy").alias("explicit_format"),
                  F.to_date("s").alias("no_format_given")),
     label="give the format when the string is not ISO - otherwise NULL (ansi off)")
spark.conf.unset("spark.sql.ansi.enabled")

# ===== 14 · date_trunc + extractors ========================================
block("14", "date_trunc / year / month / hour / dayofweek", "roll up and extract")
show(orders.select("order_id", "order_ts",
        F.date_trunc("hour", "order_ts").alias("trunc_hour"),
        F.date_trunc("day", "order_ts").alias("trunc_day"),
        F.date_trunc("week", "order_ts").alias("trunc_week"),
        F.date_trunc("month", "order_ts").alias("trunc_month")).limit(5),
     label='date_trunc keeps the TIMESTAMP type, zeroing everything below the unit')
show(orders.select("order_id",
        F.year("order_ts").alias("yr"), F.month("order_ts").alias("mo"),
        F.dayofmonth("order_ts").alias("dy"), F.hour("order_ts").alias("hr"),
        F.dayofweek("order_ts").alias("dow_1_is_sun"),
        F.date_format("order_ts", "yyyy-MM-dd HH").alias("hour_key")).limit(5),
     label="extractors return INT; date_format returns a STRING you can partition on")

# the classic "orders by hour of day" roll-up
show(orders.groupBy(F.hour("order_ts").alias("hour_of_day"))
           .count().orderBy("hour_of_day"),
     label="orders by hour of day")

# ===== 15 · datediff / date_add / add_months ===============================
block("15", "datediff / date_add / add_months", "arithmetic. datediff returns "
                                                "an INT number of days (a - b).")
ref = F.to_date(F.lit("2026-09-15"))
show(orders.select("order_id", "order_date",
        F.datediff(ref, F.col("order_date")).alias("days_before_ref"),
        F.date_add("order_date", 30).alias("refund_window_ends"),
        F.date_sub("order_date", 7).alias("week_before"),
        F.add_months("order_date", 3).alias("plus_3_months"),
        F.last_day("order_date").alias("month_end")).limit(5),
     label="reference date = 2026-09-15")

# the rolling-window filter you will write a hundred times
recent = orders.filter(F.col("order_date") >= F.add_months(ref, -1))
print("orders within 1 month before the reference date:", recent.count(), "of", orders.count())

# ===== verify ==============================================================
block("--", "VERIFY")
check(spark.conf.get("spark.sql.session.timeZone") == "UTC",
      "session timezone is pinned to UTC (set in common.get_spark)")
check(dict(bronze.dtypes)["order_ts"] == "string"
      and dict(orders.dtypes)["order_ts"].startswith("timestamp"),
      "bronze keeps the raw string; silver holds a real timestamp")
check(orders.filter(F.to_date("order_ts") != F.col("order_date")).count() == 0,
      "order_date matches to_date(order_ts) for every row")
spark.conf.set("spark.sql.ansi.enabled", "false")
check(spark.range(1).select(F.to_date(F.lit("30/08/2026")).alias("d"))
           .first()["d"] is None,
      "with ANSI off, a non-ISO string without an explicit format parses to NULL")
spark.conf.unset("spark.sql.ansi.enabled")
check(orders.select(F.datediff(ref, "order_date").alias("d")).first()["d"] == 16,
      "datediff(2026-09-15, 2026-08-30) == 16 days")
spark.stop()
