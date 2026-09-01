"""
config.py — the ONLY file you edit to move between your laptop and AWS.

    MODE = "local_parquet"   no extra JARs, no AWS, no internet. Start here.
    MODE = "local_iceberg"   real Iceberg tables in ./warehouse. Needs the
                             Iceberg runtime JAR (downloaded once by Maven).
    MODE = "emr"             Glue Data Catalog + Iceberg + S3. Run on EMR
                             Serverless, EMR on EC2, or a Glue 5.x job.

Everything else in this project reads from here. No other file has a path,
a bucket name, or a catalog name hard-coded into it.
"""

MODE = "local_iceberg"

# ---------------------------------------------------------------- local
LOCAL_ROOT = "./warehouse"

# ---------------------------------------------------------------- aws
S3_BUCKET = "nb-lakehouse"
CATALOG = "glue_catalog"
DB_BRONZE = "bronze_db"
DB_SILVER = "silver_db"
DB_GOLD = "gold_db"

# ---------------------------------------------------------------- data
# The 13 order records (12 distinct) / 18 items seed set is ALWAYS generated. Every printed
# output in the handbook was produced from exactly this data, so your
# results should match character for character.
#
# Set SCALE_ROWS to something large (e.g. 2_000_000) when you want to feel
# a real shuffle, watch the Spark UI, or test partition tuning. The seed
# rows are still there; synthetic rows are appended after them.
SCALE_ROWS = 0

# Iceberg runtime for local_iceberg mode. Match this to your Spark version.
# Spark 3.5.x  ->  org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0
# Spark 4.0.x  ->  org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0
# Spark 4.1.x+ ->  org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"

RAW_DATE = "2026-08-30"
