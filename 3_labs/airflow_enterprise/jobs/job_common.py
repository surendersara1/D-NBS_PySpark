"""
job_common.py — shared plumbing for every Spark job the enterprise DAGs submit.

Shipped to the cluster with --py-files, so every job can `import job_common`.

WHY THESE JOBS LIVE OUTSIDE dags/
    Airflow's dag-processor imports every .py file under the dags folder to
    look for DAG objects. A PySpark job placed there would be imported by the
    scheduler on every parse — which at best wastes time and at worst starts a
    SparkSession inside the dag-processor. Spark code belongs in its own tree
    and is uploaded to S3; only the DAG files go in dags/.

        dags/    parsed by Airflow, never runs Spark
        jobs/    uploaded to s3://<code-bucket>/jobs/, only ever runs on EMR

    Deploy with:
        aws s3 sync jobs/ s3://telco-prod-emr-code/jobs/ --exclude "__pycache__/*"

WHAT EVERY JOB HERE HAS IN COMMON
    * it takes its run date from an argument, never from datetime.now(), so a
      backfill of last March produces exactly March's numbers
    * it writes with Iceberg MERGE or overwritePartitions, never append, so a
      rerun of the same interval is idempotent
    * it fails loudly. A job that catches everything and exits 0 shows green in
      Airflow while having done nothing.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta

from pyspark.sql import DataFrame, SparkSession

CATALOG = "glue_catalog"


# --------------------------------------------------------------------------- session
def build_spark(app_name: str) -> SparkSession:
    """Return the session.

    The Iceberg catalog config is deliberately NOT set here — it arrives as
    --conf from the submit (see telco_config.spark_submit), which is what lets
    the platform team change the warehouse location without editing 15 jobs.
    getOrCreate() picks all of that up.
    """
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    banner(f"{app_name}  |  Spark {spark.version}  |  "
           f"app {spark.sparkContext.applicationId}")
    return spark


def base_args(description: str) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=description)
    p.add_argument("--run-date", required=False, help="YYYY-MM-DD the run is FOR")
    p.add_argument("--run-hour", required=False, help="HH, for hourly jobs")
    return p


# --------------------------------------------------------------------------- logging
def banner(msg: str) -> None:
    line = "=" * min(len(msg) + 4, 100)
    print(f"\n{line}\n  {msg}\n{line}", flush=True)


def step(msg: str) -> None:
    print(f"  -> {msg}", flush=True)


def fail(msg: str) -> "NoReturn":  # noqa: F821
    """Exit non-zero so the Airflow task actually turns red."""
    print(f"\n!! FAILED: {msg}\n", flush=True)
    sys.exit(1)


# --------------------------------------------------------------------------- tables
def fq(db: str, table: str) -> str:
    return f"{CATALOG}.{db}.{table}"


def table_exists(spark: SparkSession, table: str) -> bool:
    try:
        spark.sql(f"DESCRIBE TABLE {table}")
        return True
    except Exception:
        return False


def snapshot_id(spark: SparkSession, table: str):
    """Current snapshot id — the thing to put in XCom and in an audit record."""
    row = spark.sql(
        f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1"
    ).collect()
    return row[0]["snapshot_id"] if row else None


def data_file_count(spark: SparkSession, table: str) -> int:
    """content = 0 is data, 1 is position deletes, 2 is equality deletes."""
    return spark.sql(f"SELECT count(*) c FROM {table}.files WHERE content = 0").collect()[0]["c"]


def delete_file_count(spark: SparkSession, table: str) -> int:
    return spark.sql(f"SELECT count(*) c FROM {table}.files WHERE content > 0").collect()[0]["c"]


def describe_commit(spark: SparkSession, table: str, label: str) -> None:
    """Print what the write actually did. This is the evidence in the task log."""
    if not table_exists(spark, table):
        step(f"{label}: {table} does not exist yet")
        return
    snap = spark.sql(
        f"SELECT snapshot_id, operation, summary FROM {table}.snapshots "
        "ORDER BY committed_at DESC LIMIT 1"
    ).collect()
    if not snap:
        return
    s = snap[0]
    summary = s["summary"] or {}
    interesting = {k: v for k, v in summary.items() if k in (
        "added-records", "deleted-records", "added-data-files", "deleted-data-files",
        "added-position-deletes", "total-records", "total-data-files")}
    step(f"{label}: snapshot={s['snapshot_id']} op={s['operation']} {interesting}")


# --------------------------------------------------------------------------- dates
def parse_date(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d")


def late_window(run_date: str, hours: int) -> tuple[str, str]:
    """The window a late-arriving record may still be merged into.

    Records for an hour keep trickling in from switches for hours afterwards.
    The job reprocesses a trailing window rather than only the current hour,
    and because the write is a MERGE, reprocessing is free of duplicates.
    """
    end = parse_date(run_date) + timedelta(days=1)
    start = end - timedelta(hours=hours + 24)
    return start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S")


# --------------------------------------------------------------------------- quality
def assert_not_empty(df: DataFrame, what: str) -> int:
    n = df.count()
    if n == 0:
        fail(f"{what} produced 0 rows — refusing to publish an empty partition")
    step(f"{what}: {n:,} rows")
    return n


def assert_unique(spark: SparkSession, df: DataFrame, keys: list[str], what: str) -> None:
    dupes = df.groupBy(*keys).count().filter("count > 1").count()
    if dupes:
        fail(f"{what}: {dupes:,} duplicate keys on {keys}")
    step(f"{what}: key {keys} is unique")
