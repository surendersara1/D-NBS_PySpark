"""
00_setup.py  —  run this ONCE before anything else.

Builds the Iceberg lakehouse these labs operate on, and proves the catalog
wiring is real by reading the metadata tree back off disk.

    python 00_setup.py

What you should take away:
  * A CREATE TABLE writes metadata and NOTHING else - no data, no manifest.
  * Every write after that appends a snapshot; nothing is ever mutated.
  * The catalog is a pointer. In local mode you can literally cat the file.
"""
import os
import random
import shutil
from datetime import datetime, timedelta

import config as C
from common import (get_spark, fq, meta, block, sql, check, print_tree,
                    warehouse_dir, file_count)
from pyspark.sql import Row

spark = get_spark("00_setup")

# ===========================================================================
block("00", "A CLEAN SLATE", "these labs mutate tables, so always start fresh")
# ===========================================================================
if C.MODE == "local_iceberg":
    root = os.path.abspath(C.LOCAL_ROOT)
    shutil.rmtree(root, ignore_errors=True)
    print(f"  removed {root}")

spark.sql(f"CREATE DATABASE IF NOT EXISTS {C.CATALOG}.{C.DB}")
print(f"  database ready: {C.CATALOG}.{C.DB}")

# ===========================================================================
block("01", "CREATE TABLE", "metadata only - there is no data layer yet")
# ===========================================================================
spark.sql(f"""
    CREATE TABLE {fq('orders')} (
        order_id     BIGINT,
        customer_id  BIGINT,
        region       STRING,
        department   STRING,
        status       STRING,
        order_amount DECIMAL(10,2),
        order_ts     TIMESTAMP)
    USING iceberg
    PARTITIONED BY (days(order_ts))
    TBLPROPERTIES ('format-version' = '2')
""")
print("  created", fq('orders'), "partitioned by days(order_ts)")

# format-version 2 is what enables delete files (lab 04). v1 cannot do MoR.
sql(spark, f"SHOW TBLPROPERTIES {fq('orders')}",
    label="table properties as created", n=30, truncate=False)

print("\n  the on-disk tree right after CREATE:")
print_tree("orders")

snaps_after_create = spark.sql(f"SELECT count(*) FROM {meta('snapshots')}").first()[0]
files_after_create = spark.sql(f"SELECT count(*) FROM {meta('files')}").first()[0]
print(f"\n  snapshots: {snaps_after_create}   data files: {files_after_create}")

# ===========================================================================
block("02", "SEED THE TABLE", f"{C.SEED_ORDERS} orders across 3 regions, 6 days")
# ===========================================================================
rnd = random.Random(1701)
REGIONS = ["NA", "EMEA", "APAC"]
DEPTS = ["Grocery", "Electronics", "Apparel", "Home", "Beauty"]
STATUS = ["completed", "completed", "completed", "pending", "refunded"]
BASE = datetime(2026, 3, 1, 8, 0, 0)

rows = []
for i in range(C.SEED_ORDERS):
    rows.append(Row(
        order_id=1000 + i,
        customer_id=rnd.randint(1, 40),
        # deliberately skewed ~60% NA, so lab 05's layout work has a target
        region=rnd.choice(REGIONS[:1] * 3 + REGIONS[1:]),
        department=rnd.choice(DEPTS),
        status=rnd.choice(STATUS),
        order_amount=float(rnd.randint(5, 900)),
        order_ts=BASE + timedelta(days=rnd.randint(0, 5),
                                  minutes=rnd.randint(0, 700))))

df = (spark.createDataFrame(rows)
      .withColumn("order_amount",
                  __import__("pyspark").sql.functions.col("order_amount")
                  .cast("decimal(10,2)")))

# Three separate appends on purpose: three snapshots, and enough small files
# for lab 05 to compact. This is what a trickle-feed pipeline looks like.
for part in range(3):
    lo, hi = part * (C.SEED_ORDERS // 3), (part + 1) * (C.SEED_ORDERS // 3)
    (df.filter(f"order_id >= {1000 + lo} AND order_id < {1000 + hi}")
       .writeTo(fq("orders")).append())
    print(f"  append {part + 1}/3 committed")

# ===========================================================================
block("03", "A SECOND TABLE", "the staging side for lab 02's MERGE")
# ===========================================================================
spark.sql(f"DROP TABLE IF EXISTS {fq('orders_staging')}")
spark.sql(f"""
    CREATE TABLE {fq('orders_staging')} (
        order_id     BIGINT,
        customer_id  BIGINT,
        region       STRING,
        department   STRING,
        status       STRING,
        order_amount DECIMAL(10,2),
        order_ts     TIMESTAMP)
    USING iceberg
""")
# 12 updates to existing orders + 8 brand new ones
upd = (spark.table(fq("orders")).orderBy("order_id").limit(12)
       .withColumn("order_amount",
                   __import__("pyspark").sql.functions.lit(999.99)
                   .cast("decimal(10,2)"))
       .withColumn("status",
                   __import__("pyspark").sql.functions.lit("refunded")))
new_rows = [Row(order_id=9000 + i, customer_id=rnd.randint(1, 40),
                region="APAC", department="Home", status="completed",
                order_amount=float(100 + i),
                order_ts=BASE + timedelta(days=7, minutes=i))
            for i in range(8)]
new_df = (spark.createDataFrame(new_rows)
          .withColumn("order_amount",
                      __import__("pyspark").sql.functions.col("order_amount")
                      .cast("decimal(10,2)"))
          .select(upd.columns))
upd.unionByName(new_df).writeTo(fq("orders_staging")).append()
print(f"  staging holds 12 updates + 8 inserts")

# ===========================================================================
block("04", "THE TREE, NOW THAT DATA EXISTS")
# ===========================================================================
print_tree("orders", max_files=8)

if C.MODE == "local_iceberg":
    # The Hadoop catalog IS the filesystem. This is the pointer, in the flesh.
    hint = os.path.join(warehouse_dir("orders"), "metadata", "version-hint.text")
    if os.path.exists(hint):
        with open(hint) as fh:
            print(f"\n  version-hint.text contains: {fh.read().strip()!r}"
                  f"   <- the entire catalog, for this table")

sql(spark, f"""
    SELECT committed_at, snapshot_id, parent_id, operation,
           summary['added-data-files'] AS added_files,
           summary['added-records']    AS added_rows
    FROM {meta('snapshots')} ORDER BY committed_at""",
    label="one snapshot per commit", truncate=False)

# ===========================================================================
block("05", "VERIFY")
# ===========================================================================
n_orders = spark.table(fq("orders")).count()
n_snaps = spark.sql(f"SELECT count(*) FROM {meta('snapshots')}").first()[0]
n_files = file_count(spark)
n_stage = spark.table(fq("orders_staging")).count()

check(snaps_after_create == 0,
      "CREATE TABLE produced ZERO snapshots - metadata only, no data layer")
check(files_after_create == 0,
      "CREATE TABLE produced ZERO data files")
check(n_orders == C.SEED_ORDERS,
      f"orders holds {C.SEED_ORDERS} rows after three appends")
check(n_snaps == 3,
      f"three appends produced exactly 3 snapshots (got {n_snaps})")
check(n_files >= 3,
      f"the table is spread over {n_files} data files - lab 05 will compact these")
check(n_stage == 20,
      "orders_staging holds 20 rows (12 updates + 8 inserts)")

print(f"\nSetup complete. Now run 01_anatomy.py .. 05_maintenance_wap.py\n")
spark.stop()
