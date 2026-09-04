"""
interconnect_ingest.py — the script behind the Glue job
`telco-<env>-interconnect-ingest`, which DAG 04 runs once per partner as a
MAPPED task.

Twelve partners send settlement files in four different dialects. One job
handles all of them, selected by --source_format, because the difference is
only in the reader: everything after parsing is identical.

    tap3          the GSMA standard, delivered here as pre-decoded Parquet
    csv_v1        headerless CSV, DDMMYYYY dates, duration in seconds
    csv_v2        headered CSV, ISO dates, duration in minutes as a decimal
    fixed_width   COBOL-era positional records, still very much alive

THE LESSON
    Resist writing twelve jobs. Isolate the variation in ONE function that
    returns a common schema, and keep the rest shared. The Airflow side then
    maps a single task over a config list — which is what DAG 04 does — and
    adding a thirteenth partner is a config change, not a new job.
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (DecimalType, IntegerType, StringType, StructField,
                               StructType, TimestampType)

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "partner_code", "source_format", "currency", "run_date", "target_table"],
)
sc = SparkContext()
glue = GlueContext(sc)
spark = glue.spark_session
job = Job(glue)
job.init(args["JOB_NAME"], args)

PARTNER = args["partner_code"]
FMT = args["source_format"]
CURRENCY = args["currency"]
RUN_DATE = args["run_date"]
TARGET = args["target_table"]
CATALOG = "glue_catalog"
SRC = f"s3://telco-prod-raw/interconnect/{PARTNER}/dt={RUN_DATE}/"

# The one schema every dialect must produce.
COMMON = StructType([
    StructField("a_number", StringType()),
    StructField("b_number", StringType()),
    StructField("event_ts", TimestampType()),
    StructField("duration_sec", IntegerType()),
    StructField("charged_units", IntegerType()),
    StructField("charge_amount", DecimalType(18, 5)),
])

print(f"=== ingesting {PARTNER} ({FMT}, {CURRENCY}) for {RUN_DATE} from {SRC}")


def read_tap3():
    """GSMA TAP3, delivered pre-decoded to Parquet by the clearing house."""
    return (
        spark.read.parquet(SRC)
        .select(
            F.col("callingNumber").alias("a_number"),
            F.col("calledNumber").alias("b_number"),
            F.to_timestamp("callEventStartTimeStamp").alias("event_ts"),
            F.col("totalCallEventDuration").cast("int").alias("duration_sec"),
            F.col("chargedUnits").cast("int").alias("charged_units"),
            F.col("chargeAmount").cast("decimal(18,5)").alias("charge_amount"),
        )
    )


def read_csv_v1():
    """Headerless, DDMMYYYY + HHMMSS in two columns."""
    return (
        spark.read.option("header", False).csv(SRC)
        .toDF("a_number", "b_number", "call_date", "call_time",
              "duration_sec", "units", "amount")
        .select(
            "a_number", "b_number",
            F.to_timestamp(F.concat_ws(" ", F.col("call_date"), F.col("call_time")),
                           "ddMMyyyy HHmmss").alias("event_ts"),
            F.col("duration_sec").cast("int").alias("duration_sec"),
            F.col("units").cast("int").alias("charged_units"),
            F.col("amount").cast("decimal(18,5)").alias("charge_amount"),
        )
    )


def read_csv_v2():
    """Headered, ISO timestamps, duration in DECIMAL MINUTES."""
    return (
        spark.read.option("header", True).csv(SRC)
        .select(
            F.col("calling_msisdn").alias("a_number"),
            F.col("called_msisdn").alias("b_number"),
            F.to_timestamp("start_time").alias("event_ts"),
            # The trap: minutes, not seconds. Forgetting this makes the
            # partner look 60x cheaper and the variance report nonsense.
            F.round(F.col("duration_minutes").cast("double") * 60).cast("int")
             .alias("duration_sec"),
            F.col("billed_units").cast("int").alias("charged_units"),
            F.col("net_amount").cast("decimal(18,5)").alias("charge_amount"),
        )
    )


def read_fixed_width():
    """Positional records. Column boundaries come from the partner's spec."""
    return (
        spark.read.text(SRC)
        .select(
            F.trim(F.substring("value", 1, 15)).alias("a_number"),
            F.trim(F.substring("value", 16, 15)).alias("b_number"),
            F.to_timestamp(F.substring("value", 31, 14), "yyyyMMddHHmmss").alias("event_ts"),
            F.substring("value", 45, 8).cast("int").alias("duration_sec"),
            F.substring("value", 53, 8).cast("int").alias("charged_units"),
            # Implied 5 decimal places, no decimal point in the file.
            (F.substring("value", 61, 15).cast("decimal(18,5)") / F.lit(100000))
             .cast("decimal(18,5)").alias("charge_amount"),
        )
    )


READERS = {
    "tap3": read_tap3,
    "csv_v1": read_csv_v1,
    "csv_v2": read_csv_v2,
    "fixed_width": read_fixed_width,
}

if FMT not in READERS:
    raise ValueError(f"unknown source_format '{FMT}'; known: {sorted(READERS)}")

parsed = READERS[FMT]()

# --------------------------------------------------------------------- validate
# Fail on a malformed file rather than silently loading nulls into a ledger
# that decides how much money changes hands.
total = parsed.count()
bad_ts = parsed.where(F.col("event_ts").isNull()).count()
bad_amt = parsed.where(F.col("charge_amount").isNull()).count()
print(f"=== parsed {total:,} rows; {bad_ts:,} bad timestamps, {bad_amt:,} bad amounts")
if total == 0:
    raise ValueError(f"{PARTNER}: file parsed to 0 rows — wrong format or empty delivery")
if bad_ts > total * 0.001 or bad_amt > total * 0.001:
    raise ValueError(
        f"{PARTNER}: more than 0.1% unparseable rows "
        f"({bad_ts} timestamps, {bad_amt} amounts) — refusing to load into the ledger"
    )

enriched = (
    parsed
    .withColumn("partner_code", F.lit(PARTNER))
    .withColumn("currency", F.lit(CURRENCY))
    .withColumn("source_format", F.lit(FMT))
    .withColumn("settlement_date", F.lit(RUN_DATE).cast("date"))
    .withColumn("ingested_at", F.current_timestamp())
)

# Replace exactly this partner's slice of the day. A rerun of one partner must
# not touch the other eleven.
(enriched.writeTo(f"{CATALOG}.{TARGET}")
         .option("write.distribution-mode", "hash")
         .overwritePartitions())

print(f"=== {PARTNER}: {total:,} rows written to {TARGET} for {RUN_DATE}")
job.commit()
