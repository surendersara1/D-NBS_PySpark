"""
common.py — session builder, naming, and the little helpers every lab uses.
You should not need to change anything in here.
"""
import os
import sys

import config as C
from pyspark.sql import SparkSession


# ---------------------------------------------------------------- session
def get_spark(app_name="iceberg_deep"):
    b = (SparkSession.builder
         .appName(app_name)
         .config("spark.sql.session.timeZone", "UTC")
         # 200 is the default and it is wrong for a laptop. See deck 2, knob 2.
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.sql.adaptive.enabled", "true")
         # The extensions are MANDATORY here: every CALL procedure, every
         # ALTER TABLE ... ADD PARTITION FIELD and every CREATE BRANCH in
         # these labs is provided by them.
         .config("spark.sql.extensions",
                 "org.apache.iceberg.spark.extensions."
                 "IcebergSparkSessionExtensions"))

    if C.MODE == "local_iceberg":
        b = (b.master("local[*]")
             .config("spark.jars.packages", C.ICEBERG_PACKAGE)
             .config(f"spark.sql.catalog.{C.CATALOG}",
                     "org.apache.iceberg.spark.SparkCatalog")
             # shorthand style: type=hadoop. No external system at all.
             .config(f"spark.sql.catalog.{C.CATALOG}.type", "hadoop")
             .config(f"spark.sql.catalog.{C.CATALOG}.warehouse",
                     os.path.abspath(C.LOCAL_ROOT + "/iceberg")))

    elif C.MODE == "emr":
        # class-backed style: catalog-impl, and NO type= property.
        b = (b.config(f"spark.sql.catalog.{C.CATALOG}",
                      "org.apache.iceberg.spark.SparkCatalog")
             .config(f"spark.sql.catalog.{C.CATALOG}.catalog-impl",
                     "org.apache.iceberg.aws.glue.GlueCatalog")
             .config(f"spark.sql.catalog.{C.CATALOG}.warehouse",
                     f"s3://{C.S3_BUCKET}/warehouse")
             .config(f"spark.sql.catalog.{C.CATALOG}.io-impl",
                     "org.apache.iceberg.aws.s3.S3FileIO"))
    else:
        raise ValueError(f"Unknown MODE: {C.MODE!r} - these labs need Iceberg.")

    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# ---------------------------------------------------------------- naming
def fq(name):
    """Fully-qualified table identifier: lake.deep_db.orders"""
    return f"{C.CATALOG}.{C.DB}.{name}"


def meta(name, table="orders"):
    """A metadata table: lake.deep_db.orders.snapshots"""
    return f"{fq(table)}.{name}"


def warehouse_dir(table="orders"):
    """On-disk table location, local mode only."""
    if C.MODE != "local_iceberg":
        return None
    return os.path.abspath(
        os.path.join(C.LOCAL_ROOT, "iceberg", C.DB, table))


# ---------------------------------------------------------------- output
def block(num, title, note=""):
    bar = "=" * 74
    print(f"\n{bar}\n  {num:>2} - {title}\n{bar}")
    if note:
        print(f"  {note}\n")


def show(df, n=20, truncate=False, label=None):
    if label:
        print(f"--- {label} ---")
    df.show(n, truncate=truncate)


def sql(spark, q, label=None, n=20, truncate=False, quiet=False):
    """Run SQL, optionally print it, always return the DataFrame."""
    if label:
        print(f"--- {label} ---")
    df = spark.sql(q)
    if not quiet:
        df.show(n, truncate=truncate)
    return df


def check(condition, message):
    if condition:
        print(f"  PASS  {message}")
    else:
        print(f"  FAIL  {message}")
        sys.exit(1)


# ---------------------------------------------------------------- iceberg helpers
def snapshot_ids(spark, table="orders"):
    """Every snapshot id, oldest first."""
    rows = spark.sql(
        f"SELECT snapshot_id FROM {meta('snapshots', table)} "
        f"ORDER BY committed_at").collect()
    return [r[0] for r in rows]


def current_snapshot(spark, table="orders"):
    return spark.sql(
        f"SELECT snapshot_id FROM {meta('snapshots', table)} "
        f"ORDER BY committed_at DESC LIMIT 1").first()[0]


def file_count(spark, table="orders"):
    """Data files (content=0) the CURRENT snapshot references."""
    return spark.sql(
        f"SELECT count(*) FROM {meta('files', table)} WHERE content = 0"
    ).first()[0]


def delete_file_count(spark, table="orders"):
    """content 1 = position deletes, 2 = equality deletes."""
    row = spark.sql(
        f"SELECT "
        f"  sum(CASE WHEN content = 1 THEN 1 ELSE 0 END) AS pos, "
        f"  sum(CASE WHEN content = 2 THEN 1 ELSE 0 END) AS eq "
        f"FROM {meta('files', table)}").first()
    return (row["pos"] or 0), (row["eq"] or 0)


def print_tree(table="orders", max_files=6):
    """Walk the real on-disk metadata tree. Local mode only."""
    root = warehouse_dir(table)
    if root is None or not os.path.isdir(root):
        print("  (tree walk is local_iceberg only)")
        return
    for sub in ("metadata", "data"):
        d = os.path.join(root, sub)
        if not os.path.isdir(d):
            continue
        names = []
        for dirpath, _, filenames in os.walk(d):
            rel = os.path.relpath(dirpath, root)
            for f in sorted(filenames):
                if f.endswith(".crc"):      # Hadoop checksums, not Iceberg's
                    continue
                names.append(os.path.join(rel, f).replace("\\", "/"))
        names.sort()
        print(f"  {sub}/  ({len(names)} files)")
        for n in names[:max_files]:
            print(f"      {n}")
        if len(names) > max_files:
            print(f"      ... {len(names) - max_files} more")
