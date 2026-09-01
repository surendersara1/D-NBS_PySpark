"""
config.py — the ONLY file you edit in this lab suite.

    MODE = "local_iceberg"   real Iceberg tables under ./warehouse, local Hadoop
                             catalog. Maven fetches the runtime JAR once.
    MODE = "emr"             Glue Data Catalog + S3. Submit the same files to
                             EMR Serverless, EMR on EC2, or a Glue 5.x job.

There is no Parquet mode here. Every lab in this folder is about Iceberg
behaviour that a plain Parquet directory cannot do at all.
"""

MODE = "local_iceberg"

# ---------------------------------------------------------------- local
LOCAL_ROOT = "./warehouse"

# ---------------------------------------------------------------- aws
S3_BUCKET = "nb-lakehouse"

# ---------------------------------------------------------------- names
CATALOG = "lake"
DB = "deep_db"

# ---------------------------------------------------------------- runtime
# MUST match your Spark AND Scala version. Mismatch fails at session start
# with IncompatibleClassChangeError, not at query time.
#   Spark 3.5.x  ->  org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0
#   Spark 4.0.x  ->  org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0
#   Spark 4.1.x  ->  org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0
# Verified working: PySpark 4.1.3 + the 4.1_2.13:1.11.0 runtime below.
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"

# Rows in the seed orders table. Small enough to print, big enough that
# compaction and delete files have something honest to act on.
SEED_ORDERS = 240
