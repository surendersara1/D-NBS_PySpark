"""
lab_config.py — the ONE place a DAG looks to decide local vs EMR.

Same idea as config.py in the pyspark30 and iceberg_deep labs: nothing in a
DAG file hard-codes a path, a bucket, or an application id.

    LAB_MODE=local   the Spark scripts run inside the Airflow container
    LAB_MODE=emr     the same scripts are submitted to EMR Serverless

Set it in .env (compose passes it through) or, on MWAA, as an Airflow Variable
or an environment variable in the MWAA configuration.
"""
import os

MODE = os.environ.get("LAB_MODE", "local").lower()

# ---------------------------------------------------------------- local
LAB_SCRIPTS = "/opt/airflow/labs/iceberg_deep"   # read-only mount of ../iceberg_deep
WORK_DIR    = "/opt/airflow/work"                # Spark warehouse + outputs
LANDING_DIR = "/opt/airflow/landing"             # drop files here for the sensor DAG

# ---------------------------------------------------------------- emr
EMR_APPLICATION_ID     = os.environ.get("EMR_APPLICATION_ID", "")
EMR_EXECUTION_ROLE_ARN = os.environ.get("EMR_EXECUTION_ROLE_ARN", "")
S3_BUCKET              = os.environ.get("LAB_S3_BUCKET", "")
AWS_REGION             = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

# The six iceberg_deep scripts, in the order they must run.
ICEBERG_STEPS = [
    "00_setup.py",
    "01_anatomy.py",
    "02_commits_time_travel.py",
    "03_evolution.py",
    "04_cow_mor_deletes.py",
    "05_maintenance_wap.py",
]


# ---------------------------------------------------------------- dates
# Airflow 3 rule: a SCHEDULED run has a logical date (its data interval);
# a MANUALLY TRIGGERED run has none — logical_date is None and `ds` is not
# even defined. Both of these work in either case; use them, not bare {{ ds }}.
DS = "{{ (dag_run.logical_date or dag_run.run_after) | ds }}"


def run_date(dag_run) -> str:
    """YYYY-MM-DD for this run, whether it was scheduled or triggered by hand."""
    return (dag_run.logical_date or dag_run.run_after).strftime("%Y-%m-%d")


def local_spark_cmd(script: str) -> str:
    """Run one lab script with Spark inside the container.

    cwd is WORK_DIR so the script's relative ./warehouse lands there, keeping
    the container's Linux warehouse separate from the Windows one on the host
    (Iceberg records absolute paths — they cannot be shared across OSes).
    """
    return (
        f"cd {WORK_DIR} && "
        f"PYSPARK_PYTHON=python3 python3 {LAB_SCRIPTS}/{script}"
    )


def emr_job_driver(script: str) -> dict:
    """The sparkSubmit block for EmrServerlessStartJobOperator."""
    return {
        "sparkSubmit": {
            "entryPoint": f"s3://{S3_BUCKET}/labs/iceberg_deep/{script}",
            "entryPointArguments": [],
            "sparkSubmitParameters": (
                "--conf spark.executor.cores=2 "
                "--conf spark.executor.memory=4g "
                "--conf spark.driver.memory=2g "
                "--conf spark.executor.instances=2 "
                f"--py-files s3://{S3_BUCKET}/labs/iceberg_deep/common.py,"
                f"s3://{S3_BUCKET}/labs/iceberg_deep/config.py"
            ),
        }
    }
