"""
billing_normalise.py — the script behind the Glue job `telco-<env>-billing-normalise`
which DAG 04 starts with GlueJobOperator.

THIS IS A GLUE JOB, NOT AN EMR JOB, AND IT LOOKS DIFFERENT

    * arguments arrive through getResolvedOptions, not argparse
    * the entry point is GlueContext wrapping a SparkContext
    * Job.init / Job.commit bracket the run so Glue bookmarks work
    * there is no cluster to size — DPUs are set on the job, not in the code

Everything below the boilerplate is ordinary PySpark, which is the point: a
Glue job is a Spark job with a different wrapper. DAG 04 uses Glue here rather
than EMR because this step is a few hundred GB of well-shaped Parquet with no
custom native code, and Glue's one-minute start beats EMR's five.

    aws s3 cp billing_normalise.py s3://telco-prod-emr-code/glue/
    # then point the Glue job's Script location at it
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql import functions as F

# --------------------------------------------------------------------- boilerplate
args = getResolvedOptions(sys.argv, ["JOB_NAME", "run_date", "target_table"])
sc = SparkContext()
glue = GlueContext(sc)
spark = glue.spark_session
job = Job(glue)
job.init(args["JOB_NAME"], args)

RUN_DATE = args["run_date"]
TARGET = args["target_table"]           # e.g. telco_prod_silver.billing_events
CATALOG = "glue_catalog"

print(f"=== billing normalise for {RUN_DATE} -> {TARGET}")

# --------------------------------------------------------------------- read the CDC output
# DMS writes change records with an Op column: I insert, U update, D delete.
raw = (
    spark.read.parquet(f"s3://telco-prod-raw/cdc/billing/dt={RUN_DATE}/")
)

# --------------------------------------------------------------------- collapse the CDC stream
# A row can change several times in one day. Keep only the LAST change per
# primary key, which is what the target table should end up holding.
latest = Window.partitionBy("bill_line_id").orderBy(
    F.col("transact_seq").desc(), F.col("transact_ts").desc()
)
collapsed = (
    raw.withColumn("_rn", F.row_number().over(latest))
       .where(F.col("_rn") == 1)
       .drop("_rn")
)

normalised = (
    collapsed
    .withColumn("event_date", F.to_date("transact_ts"))
    .withColumn("bill_amount", F.col("bill_amount").cast("decimal(18,5)"))
    .withColumn("is_onnet", F.col("terminating_network") == F.lit("OWN"))
    .withColumn("partner_code",
                F.when(F.col("terminating_network") == F.lit("OWN"), F.lit(None))
                 .otherwise(F.col("terminating_network")))
    .withColumn("_deleted", F.col("Op") == F.lit("D"))
)

n_ins = normalised.where(~F.col("_deleted")).count()
n_del = normalised.where(F.col("_deleted")).count()
print(f"=== {n_ins:,} upserts, {n_del:,} deletes")

normalised.createOrReplaceTempView("billing_cdc")

# --------------------------------------------------------------------- MERGE
# One MERGE handles insert, update and delete. Doing it as three separate
# statements would leave the table inconsistent between them.
spark.sql(f"""
    MERGE INTO {CATALOG}.{TARGET} t
    USING billing_cdc s
      ON t.bill_line_id = s.bill_line_id
    WHEN MATCHED AND s._deleted THEN DELETE
    WHEN MATCHED AND s.transact_seq > t.transact_seq THEN UPDATE SET *
    WHEN NOT MATCHED AND NOT s._deleted THEN INSERT *
""")

snap = spark.sql(
    f"SELECT snapshot_id, summary FROM {CATALOG}.{TARGET}.snapshots "
    "ORDER BY committed_at DESC LIMIT 1"
).collect()
if snap:
    print(f"=== committed snapshot {snap[0]['snapshot_id']}: {snap[0]['summary']}")

job.commit()
