"""
common.py — session, table I/O, and the little formatting helpers every
example file uses. You should not need to change anything in here.
"""
import os
import sys

import config as C
from pyspark.sql import SparkSession


# ---------------------------------------------------------------- session
def get_spark(app_name="pyspark30"):
    b = (SparkSession.builder
         .appName(app_name)
         # Set this in EVERY job. See handbook page for to_date().
         .config("spark.sql.session.timeZone", "UTC")
         # 200 is the default and it is wrong for almost everyone.
         # With a 12-row seed set, 8 is plenty and keeps output readable.
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.sql.adaptive.enabled", "true"))

    if C.MODE == "local_parquet":
        b = b.master("local[*]")

    elif C.MODE == "local_iceberg":
        b = (b.master("local[*]")
             .config("spark.jars.packages", C.ICEBERG_PACKAGE)
             .config("spark.sql.extensions",
                     "org.apache.iceberg.spark.extensions."
                     "IcebergSparkSessionExtensions")
             .config(f"spark.sql.catalog.{C.CATALOG}",
                     "org.apache.iceberg.spark.SparkCatalog")
             .config(f"spark.sql.catalog.{C.CATALOG}.type", "hadoop")
             .config(f"spark.sql.catalog.{C.CATALOG}.warehouse",
                     os.path.abspath(C.LOCAL_ROOT + "/iceberg")))

    elif C.MODE == "emr":
        # On EMR Serverless / EMR on EC2 / Glue 5.x these are normally set
        # as job parameters. Setting them here too is harmless and makes
        # the script self-describing.
        b = (b.config("spark.sql.extensions",
                      "org.apache.iceberg.spark.extensions."
                      "IcebergSparkSessionExtensions")
             .config(f"spark.sql.catalog.{C.CATALOG}",
                     "org.apache.iceberg.spark.SparkCatalog")
             .config(f"spark.sql.catalog.{C.CATALOG}.catalog-impl",
                     "org.apache.iceberg.aws.glue.GlueCatalog")
             .config(f"spark.sql.catalog.{C.CATALOG}.warehouse",
                     f"s3://{C.S3_BUCKET}/warehouse")
             .config(f"spark.sql.catalog.{C.CATALOG}.io-impl",
                     "org.apache.iceberg.aws.s3.S3FileIO"))
    else:
        raise ValueError(f"Unknown MODE: {C.MODE}")

    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# ---------------------------------------------------------------- paths
_DBS = {"bronze": C.DB_BRONZE, "silver": C.DB_SILVER, "gold": C.DB_GOLD}


def fq(layer, name):
    """Fully-qualified table identifier for the current MODE."""
    return f"{C.CATALOG}.{_DBS[layer]}.{name}"


def raw_path(name):
    if C.MODE == "emr":
        return f"s3://{C.S3_BUCKET}/raw/{name}/dt={C.RAW_DATE}"
    return f"{C.LOCAL_ROOT}/raw/{name}/dt={C.RAW_DATE}"


def _table_path(layer, name):
    if C.MODE == "emr":
        return f"s3://{C.S3_BUCKET}/{layer}/{name}"
    return f"{C.LOCAL_ROOT}/{layer}/{name}"


# ---------------------------------------------------------------- table io
def write_table(df, layer, name, partition_by=None):
    """
    Iceberg modes  -> a real catalogued table (CREATE OR REPLACE).
    Parquet mode   -> a directory of Parquet files. Same data, no catalog.
    """
    if C.MODE == "local_parquet":
        w = df.write.mode("overwrite")
        if partition_by:
            w = w.partitionBy(partition_by)
        w.parquet(_table_path(layer, name))
        return

    spark = df.sparkSession
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {C.CATALOG}.{_DBS[layer]}")
    w = df.writeTo(fq(layer, name)).tableProperty("format-version", "2")
    if partition_by:
        w = w.partitionedBy(*[df[c] for c in [partition_by]])
    w.createOrReplace()


def read_table(spark, layer, name):
    if C.MODE == "local_parquet":
        return spark.read.parquet(_table_path(layer, name))
    return spark.read.table(fq(layer, name))


# ---------------------------------------------------------------- output
def block(num, title, note=""):
    """Prints the header that separates one function's example from the next."""
    bar = "=" * 74
    print(f"\n{bar}\n  {num:>2} - {title}\n{bar}")
    if note:
        print(f"  {note}\n")


def show(df, n=20, truncate=False, label=None):
    if label:
        print(f"--- {label} ---")
    df.show(n, truncate=truncate)


def check(condition, message):
    """Every example file ends with a few of these. If one fails, you broke it."""
    if condition:
        print(f"  PASS  {message}")
    else:
        print(f"  FAIL  {message}")
        sys.exit(1)
