"""
04 · The integration — Airflow orchestrating the Iceberg labs

This is the DAG that connects the three weeks. It runs the six iceberg_deep
scripts as a bronze -> silver -> gold pipeline.

    LAB_MODE=local   BashOperator runs each script with Spark IN the container
    LAB_MODE=emr     EmrServerlessStartJobOperator submits the SAME script to
                     EMR Serverless and waits for it

    TaskGroup        visual + logical grouping in the UI (bronze / silver / gold)
    pool             "spark_local" has 1 slot: one Spark job at a time on a laptop
    retries          Spark jobs fail for transient reasons; retry once, wait 2 min

What to notice:
  * Not one line of the Spark scripts changes between modes. Airflow decides
    WHERE the work runs; the work itself is identical. That is the division of
    labour: Airflow = when/where/order, Spark = the actual transformation.
  * The DAG is idempotent because 00_setup.py wipes and rebuilds — re-running
    the whole thing produces the same tables.
"""
from datetime import datetime, timedelta

from airflow.sdk import DAG, TaskGroup
from airflow.providers.standard.operators.bash import BashOperator

import lab_config as C


def spark_step(task_id: str, script: str):
    """One lab script as one task, in whichever mode the lab is in."""
    if C.MODE == "emr":
        from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
        return EmrServerlessStartJobOperator(
            task_id=task_id,
            application_id=C.EMR_APPLICATION_ID,
            execution_role_arn=C.EMR_EXECUTION_ROLE_ARN,
            job_driver=C.emr_job_driver(script),
            configuration_overrides={
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {"logUri": f"s3://{C.S3_BUCKET}/emr-logs/"}
                }
            },
            wait_for_completion=True,
            deferrable=True,            # don't hold a worker while EMR runs
            name=f"iceberg_{task_id}",
        )
    return BashOperator(
        task_id=task_id,
        bash_command=C.local_spark_cmd(script),
        pool="spark_local",
        execution_timeout=timedelta(minutes=30),
    )


with DAG(
    dag_id="04_medallion_iceberg_pipeline",
    description="bronze -> silver -> gold over the iceberg_deep scripts, local or EMR",
    schedule="0 2 * * *",                 # 02:00 UTC nightly
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,                    # never two nightly runs at once
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["lab", "04-integration", C.MODE],
) as dag:

    with TaskGroup("bronze") as bronze:
        setup = spark_step("00_setup", "00_setup.py")

    with TaskGroup("silver") as silver:
        anatomy   = spark_step("01_anatomy",             "01_anatomy.py")
        commits   = spark_step("02_commits_time_travel", "02_commits_time_travel.py")
        evolution = spark_step("03_evolution",           "03_evolution.py")
        anatomy >> commits >> evolution           # 02 and 03 mutate the same table

    with TaskGroup("gold") as gold:
        cow_mor     = spark_step("04_cow_mor_deletes",  "04_cow_mor_deletes.py")
        maintenance = spark_step("05_maintenance_wap",  "05_maintenance_wap.py")
        # these two build their own tables — independent, so they could run in
        # parallel; the pool serialises them locally anyway

    bronze >> silver >> gold
