"""
00_setup.py  —  run this ONCE before anything else.

  raw JSON + CSV  ->  BRONZE (as landed, plus audit columns)
                  ->  SILVER (typed, cleaned, deduplicated)

The 13 order records (12 distinct) and 18 order_items below are the seed set. They are
deliberately dirty: mixed-case regions, padded emails, a null channel,
a null email, an order that arrives TWICE with a corrected status, and
one order_item whose parent order does not exist.

Every one of those defects exists to make a specific function on the
handbook demonstrate something real.

    python 00_setup.py
"""
import json
import os
import random
import shutil

import config as C
from common import get_spark, raw_path, write_table, read_table, block, show, check
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               ArrayType, IntegerType, DoubleType)

# ===========================================================================
#  SEED DATA  —  the exact rows printed in the handbook
# ===========================================================================
ORDERS = [
    # order_id  cust      email                       region channel status      order_ts                currency promos                    city       state ingested_at
    ("ORD-1001", "CUST-77", "  Ana.Silva@Example.COM ", "na",   "web",  "COMPLETED", "2026-08-30 14:23:11", "USD", ["SUMMER10", "FREESHIP"], "Austin",    "TX",  "2026-08-30 23:05:00"),
    ("ORD-1002", "CUST-42", "bob@shop.io",              "NA",   "app",  "completed", "2026-08-30 09:02:44", "USD", [],                       "Denver",    "CO",  "2026-08-30 23:05:00"),
    ("ORD-1003", "CUST-19", "  Chen.W@mail.cn",         "apac", "store", "COMPLETED", "2026-08-30 22:41:05", "SGD", ["WELCOME5"],             "Singapore", "SG",  "2026-08-30 23:05:00"),
    ("ORD-1004", "CUST-77", "Ana.Silva@example.com",    "Na",   None,    "REFUNDED",  "2026-08-30 16:10:00", "USD", ["SUMMER10"],             "Austin",    "TX",  "2026-08-30 23:05:00"),
    ("ORD-1005", "CUST-88", "dana@corp.de",             "emea", "web",   "COMPLETED", "2026-08-30 07:55:12", "EUR", [],                       "Berlin",    "BE",  "2026-08-30 23:05:00"),
    ("ORD-1006", "CUST-42", "bob@shop.io ",             "NA",   "app",   "PENDING",   "2026-08-30 18:30:00", "USD", None,                     "Denver",    "CO",  "2026-08-30 23:05:00"),
    ("ORD-1007", "CUST-51", "eve@x.co.uk ",             "EMEA", "store", "COMPLETED", "2026-08-30 11:15:33", "GBP", ["VIP20", "FREESHIP"],    "London",    "LDN", "2026-08-30 23:05:00"),
    ("ORD-1008", "CUST-19", "chen.w@MAIL.CN",           "APAC", "web",   "COMPLETED", "2026-08-30 03:20:00", "SGD", [],                       "Singapore", "SG",  "2026-08-30 23:05:00"),
    ("ORD-1009", "CUST-63", None,                       "na",   "app",   "COMPLETED", "2026-08-30 13:47:29", "USD", ["SUMMER10"],             "Miami",     "FL",  "2026-08-30 23:05:00"),
    ("ORD-1010", "CUST-88", "dana@corp.de",             "emea", "web",   "CANCELLED", "2026-08-30 20:05:00", "EUR", [],                       "Berlin",    "BE",  "2026-08-30 23:05:00"),
    ("ORD-1011", "CUST-05", "frank@nordic.se",          "EMEA", "store", "COMPLETED", "2026-08-30 08:00:00", "EUR", ["WELCOME5"],             "Stockholm", "ST",  "2026-08-30 23:05:00"),
    # ---- an order that shipped NOTHING: no rows in order_items -----------
    ("ORD-1012", "CUST-51", "eve@x.co.uk",              "APAC", "web",   "COMPLETED", "2026-08-30 19:12:00", "SGD", [],                       "Singapore", "SG",  "2026-08-30 23:05:00"),
    # ---- the SAME order, re-sent later with a corrected status ----------
    ("ORD-1002", "CUST-42", "bob@shop.io",              "NA",   "app",   "REFUNDED",  "2026-08-30 09:02:44", "USD", [],                       "Denver",    "CO",  "2026-08-31 02:10:00"),
]

ITEMS = [
    # order_id  line sku            department     qty  unit_price  discount_pct
    ("ORD-1001", 1, "SKU-GR-088", "Grocery",      3,   4.50,  0.00),
    ("ORD-1001", 2, "SKU-EL-201", "Electronics",  1, 249.99,  0.10),
    ("ORD-1002", 1, "SKU-AP-410", "Apparel",      2,  39.00,  0.00),
    ("ORD-1003", 1, "SKU-EL-115", "Electronics",  1, 899.00,  0.05),
    ("ORD-1003", 2, "SKU-HO-007", "Home",         4,  12.25,  0.00),
    ("ORD-1004", 1, "SKU-GR-088", "Grocery",     -3,   4.50,  0.00),   # return
    ("ORD-1005", 1, "SKU-BE-330", "Beauty",       2,  18.75,  0.00),
    ("ORD-1005", 2, "SKU-AP-410", "Apparel",      1,  39.00,  0.20),
    ("ORD-1006", 1, "SKU-EL-201", "Electronics",  1, 249.99,  0.00),
    ("ORD-1007", 1, "SKU-AP-522", "Apparel",      3,  75.00,  0.15),
    ("ORD-1007", 2, "SKU-BE-330", "Beauty",       1,  18.75,  0.00),
    ("ORD-1007", 3, "SKU-HO-007", "Home",         2,  12.25,  0.00),
    ("ORD-1008", 1, "SKU-EL-115", "Electronics",  2, 899.00,  0.00),
    ("ORD-1009", 1, "SKU-GR-044", "Grocery",      5,   3.20,  0.00),
    ("ORD-1010", 1, "SKU-BE-901", "Beauty",       1,  55.00,  0.00),
    ("ORD-1011", 1, "SKU-HO-118", "Home",         1, 129.00,  0.10),
    ("ORD-1011", 2, "SKU-GR-044", "Grocery",      2,   3.20,  0.00),
    # ---- an item whose parent order was never delivered to us -----------
    ("ORD-9999", 1, "SKU-XX-000", "Unknown",      1,  10.00,  0.00),
]

ORDER_COLS = ["order_id", "customer_id", "customer_email", "region", "channel",
              "order_status", "order_ts", "currency", "promo_codes",
              "city", "state", "ingested_at"]
ITEM_COLS = ["order_id", "line_no", "sku", "department", "qty",
             "unit_price", "discount_pct"]


# ===========================================================================
#  1. WRITE THE RAW FILES  (this is what lands in S3 from your app)
# ===========================================================================
def write_raw_files():
    if C.MODE == "emr":
        print("MODE=emr: upload the raw files to S3 yourself, or run this "
              "script once locally and `aws s3 sync ./warehouse/raw ...`.")
        return

    o_dir, i_dir = raw_path("orders"), raw_path("order_items")
    for d in (o_dir, i_dir):
        shutil.rmtree(d, ignore_errors=True)
        os.makedirs(d, exist_ok=True)

    # --- orders: newline-delimited JSON with an array and a nested struct
    with open(os.path.join(o_dir, "orders_20260830.json"), "w") as fh:
        for r in ORDERS:
            fh.write(json.dumps({
                "order_id": r[0], "customer_id": r[1], "customer_email": r[2],
                "region": r[3], "channel": r[4], "order_status": r[5],
                "order_ts": r[6], "currency": r[7], "promo_codes": r[8],
                "ship_address": {"city": r[9], "state": r[10]},
                "ingested_at": r[11],
            }) + "\n")

    # --- order_items: plain CSV with a header
    with open(os.path.join(i_dir, "order_items_20260830.csv"), "w") as fh:
        fh.write(",".join(ITEM_COLS) + "\n")
        for r in ITEMS:
            fh.write(",".join(str(x) for x in r) + "\n")

    if C.SCALE_ROWS:
        _append_synthetic(o_dir, i_dir, C.SCALE_ROWS)

    print(f"raw written -> {o_dir}\n            -> {i_dir}")


def _append_synthetic(o_dir, i_dir, n):
    """Optional volume for shuffle/partition experiments. Seed rows stay first."""
    rnd = random.Random(369)
    regions = ["na", "NA", "emea", "EMEA", "apac", "APAC"]
    depts = ["Grocery", "Electronics", "Apparel", "Home", "Beauty"]
    with open(os.path.join(o_dir, "orders_synth.json"), "w") as fo, \
         open(os.path.join(i_dir, "order_items_synth.csv"), "w") as fi:
        fi.write(",".join(ITEM_COLS) + "\n")
        for k in range(n):
            oid = f"ORD-{100000 + k}"
            fo.write(json.dumps({
                "order_id": oid,
                "customer_id": f"CUST-{rnd.randint(1, 5000)}",
                "customer_email": f"user{k}@example.com",
                # deliberately skewed: ~60% NA, to make skew visible at scale
                "region": rnd.choice(regions[:2] * 3 + regions[2:]),
                "channel": rnd.choice(["web", "app", "store", None]),
                "order_status": "COMPLETED",
                "order_ts": f"2026-08-30 {rnd.randint(0,23):02d}:00:00",
                "currency": "USD", "promo_codes": [],
                "ship_address": {"city": "Synth", "state": "ZZ"},
                "ingested_at": "2026-08-30 23:05:00",
            }) + "\n")
            for line in range(1, rnd.randint(1, 4)):
                fi.write(f"{oid},{line},SKU-SY-{rnd.randint(1,999):03d},"
                         f"{rnd.choice(depts)},{rnd.randint(1,5)},"
                         f"{rnd.randint(2,900)}.00,0.00\n")
    print(f"  + {n:,} synthetic orders appended (SCALE_ROWS)")


# ===========================================================================
#  2. RAW -> BRONZE   land it exactly as it arrived, add audit columns only
# ===========================================================================
ORDER_SCHEMA = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("customer_email", StringType()),
    StructField("region", StringType()),
    StructField("channel", StringType()),
    StructField("order_status", StringType()),
    StructField("order_ts", StringType()),
    StructField("currency", StringType()),
    StructField("promo_codes", ArrayType(StringType())),
    StructField("ship_address", StructType([
        StructField("city", StringType()),
        StructField("state", StringType()),
    ])),
    StructField("ingested_at", StringType()),
])

ITEM_SCHEMA = StructType([
    StructField("order_id", StringType()),
    StructField("line_no", IntegerType()),
    StructField("sku", StringType()),
    StructField("department", StringType()),
    StructField("qty", IntegerType()),
    StructField("unit_price", DoubleType()),
    StructField("discount_pct", DoubleType()),
])


def build_bronze(spark):
    block(1, "RAW -> BRONZE", "declared schema, no cleaning, audit columns added")

    orders_raw = (spark.read.schema(ORDER_SCHEMA)
                  .json(raw_path("orders"))
                  .withColumn("_src_file", F.input_file_name())
                  .withColumn("_bronze_at", F.current_timestamp()))

    items_raw = (spark.read.schema(ITEM_SCHEMA)
                 .option("header", True)
                 .csv(raw_path("order_items"))
                 .withColumn("_src_file", F.input_file_name())
                 .withColumn("_bronze_at", F.current_timestamp()))

    write_table(orders_raw, "bronze", "orders_raw")
    write_table(items_raw, "bronze", "order_items_raw")
    print(f"bronze.orders_raw      {orders_raw.count():>4} rows")
    print(f"bronze.order_items_raw {items_raw.count():>4} rows")


# ===========================================================================
#  3. BRONZE -> SILVER   clean, type, deduplicate
# ===========================================================================
def build_silver(spark):
    block(2, "BRONZE -> SILVER", "clean + type + deduplicate on order_id")

    b = read_table(spark, "bronze", "orders_raw")

    cleaned = (b
        .withColumn("region", F.upper(F.trim("region")))
        .withColumn("customer_email", F.lower(F.trim("customer_email")))
        .withColumn("channel", F.coalesce(F.col("channel"), F.lit("unknown")))
        .withColumn("order_status", F.lower(F.trim("order_status")))
        .withColumn("order_ts", F.to_timestamp("order_ts"))
        .withColumn("ingested_at", F.to_timestamp("ingested_at"))
        .withColumn("order_date", F.to_date("order_ts"))
        .withColumn("city", F.col("ship_address.city"))
        .withColumn("state", F.col("ship_address.state")))

    # Deterministic dedup: keep the LATEST version of each order_id.
    # dropDuplicates() alone would keep an arbitrary one. See handbook #28.
    from pyspark.sql.window import Window
    w = Window.partitionBy("order_id").orderBy(F.col("ingested_at").desc())
    orders = (cleaned
              .withColumn("_rn", F.row_number().over(w))
              .filter(F.col("_rn") == 1)
              .drop("_rn", "_src_file", "_bronze_at", "ship_address"))

    items = (read_table(spark, "bronze", "order_items_raw")
             .withColumn("department", F.initcap(F.trim("department")))
             .withColumn("gross_amount",
                         F.round(F.col("qty") * F.col("unit_price"), 2))
             .withColumn("net_amount",
                         F.round(F.col("qty") * F.col("unit_price")
                                 * (1 - F.col("discount_pct")), 2))
             .drop("_src_file", "_bronze_at"))

    write_table(orders, "silver", "orders")
    write_table(items, "silver", "order_items")

    show(orders.orderBy("order_id"), 20, label="silver.orders")
    show(items.orderBy("order_id", "line_no"), 20, label="silver.order_items")
    return orders, items


# ===========================================================================
if __name__ == "__main__":
    write_raw_files()
    spark = get_spark("00_setup")
    build_bronze(spark)
    orders, items = build_silver(spark)

    block(3, "VERIFY")
    check(orders.count() == 12, "silver.orders has 12 rows (13 raw - 1 duplicate)")
    check(items.count() == 18, "silver.order_items has 18 rows")
    check(orders.filter("order_id = 'ORD-1002'").first()["order_status"] == "refunded",
          "ORD-1002 kept the LATER version (refunded, not completed)")
    check(orders.select("region").distinct().count() == 3,
          "region cleaned to exactly 3 values: NA / EMEA / APAC")
    check(orders.filter("channel = 'unknown'").count() == 1,
          "the null channel became 'unknown'")
    print("\nSetup complete. Now run 01_shape.py .. 08_control.py\n")
    spark.stop()
