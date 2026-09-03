"""
02 · RAN performance KPIs — every 15 minutes on EMR Serverless

Radio Access Network counters from ~40,000 cells: dropped-call rate, handover
success, PRB utilisation, RRC setup failures, throughput per cell. The network
operations centre watches these on a dashboard and expects them fresh within
about ten minutes of the measurement period closing.

WHAT THIS DAG DEMONSTRATES
  * a genuinely short interval (15 min) and what that forces you to do:
    catchup off, a hard execution_timeout under the interval, and no cluster
    to start because there is no time to start one
  * EMR Serverless as the right answer for spiky short jobs — no cluster
    lifecycle at all, deferrable so the wait is free
  * dynamic mapping over regions with a bounded fan-out
  * an anomaly branch that pages the NOC and a normal branch that does not
  * self-healing small files: the DAG inspects Iceberg's own metadata tables
    and triggers compaction only when the partition actually needs it, which
    is the lesson the iceberg_deep lab surfaced the hard way
  * Redshift materialized view refresh for the serving layer

WHY EMR SERVERLESS AND NOT EC2
  Twelve minutes of work every fifteen minutes. A transient cluster would
  spend five of those minutes booting; a permanent cluster would idle 20% of
  the time and still cost 100%. Serverless bills the job, starts in seconds,
  and needs no bootstrap because everything it imports is in the image.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import DAG, Asset, task
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.providers.standard.operators.empty import EmptyOperator

import telco_config as C

RAN_KPI_GOLD = Asset(name="ran_kpi_gold", uri=f"s3://{C.LAKE_BUCKET}/gold/ran_cell_kpi")

REGIONS = ["north", "south", "east", "west", "capital"]

# The 15-minute measurement window this run owns, as an ISO timestamp pair.
WINDOW_START = "{{ (dag_run.logical_date or dag_run.run_after).isoformat() }}"
WINDOW_END = (
    "{{ ((dag_run.logical_date or dag_run.run_after) "
    "+ macros.timedelta(minutes=15)).isoformat() }}"
)


with DAG(
    dag_id="telco_02_ran_kpi_micro_batch",
    description="15-minute RAN cell KPI aggregation on EMR Serverless",
    schedule=timedelta(minutes=15),
    start_date=datetime(2026, 1, 1),
    catchup=False,                       # never backfill a 15-minute DAG by accident
    max_active_runs=1,                   # windows must not overlap or KPIs double-count
    dagrun_timeout=timedelta(minutes=14),  # a run must die before the next one is due
    default_args={
        **C.DEFAULT_ARGS,
        "retries": 1,                    # at this cadence, retry once then let it go
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(minutes=10),
    },
    tags=["telco", "ran", "emr-serverless", "near-real-time", C.ENV],
    doc_md=__doc__,
) as dag:

    # -- 1. aggregate counters, one job per region ----------------------------
    # partial() holds everything common; expand() varies only the job_driver.
    aggregate = EmrServerlessStartJobOperator.partial(
        task_id="aggregate_cell_counters",
        application_id=C.EMR_SERVERLESS_APP_ID,
        execution_role_arn=C.EXEC_ROLE,
        configuration_overrides=C.emrs_monitoring("ran-kpi"),
        wait_for_completion=True,
        deferrable=True,                 # 5 concurrent 8-minute waits, zero slots held
        pool=C.POOL_EMR_SERVERLESS,
        max_active_tis_per_dag=5,        # bound the fan-out even if REGIONS grows
    ).expand(
        job_driver=[
            C.spark_submit(
                "ran_kpi_aggregate.py",
                ["--region", r, "--window-start", WINDOW_START, "--window-end", WINDOW_END],
                executors=8, executor_cores=4, executor_memory="12g",
            )
            for r in REGIONS
        ]
    )

    # -- 2. anomaly detection over the aggregated window ---------------------
    @task(pool=C.POOL_EMR_SERVERLESS)
    def detect_anomalies(dag_run) -> dict:
        """Compare each cell's KPIs to its own 28-day baseline.

        Returns a small summary — never the offending rows. If the NOC needs
        the detail they open the Athena saved query the alert links to.
        """
        from airflow.providers.amazon.aws.hooks.athena import AthenaHook

        window = (dag_run.logical_date or dag_run.run_after)
        hook = AthenaHook(aws_conn_id="aws_default")
        sql = f"""
            WITH current AS (
              SELECT cell_id, region, drop_call_rate, prb_utilisation
              FROM {C.GLUE_DB_GOLD}.ran_cell_kpi
              WHERE window_start = TIMESTAMP '{window:%Y-%m-%d %H:%M:%S}'
            ),
            baseline AS (
              SELECT cell_id,
                     avg(drop_call_rate)    AS mu,
                     stddev(drop_call_rate) AS sigma
              FROM {C.GLUE_DB_GOLD}.ran_cell_kpi
              WHERE window_start >= TIMESTAMP '{window:%Y-%m-%d %H:%M:%S}' - INTERVAL '28' DAY
                AND window_start <  TIMESTAMP '{window:%Y-%m-%d %H:%M:%S}'
              GROUP BY cell_id
            )
            SELECT count(*) AS anomalous_cells
            FROM current c JOIN baseline b USING (cell_id)
            WHERE b.sigma > 0
              AND c.drop_call_rate > b.mu + {C.RAN_ANOMALY_SIGMA} * b.sigma
        """
        qid = hook.run_query(
            query=sql,
            query_context={"Database": C.GLUE_DB_GOLD},
            result_configuration={"OutputLocation": C.ATHENA_RESULTS},
            workgroup=C.ATHENA_WORKGROUP,
        )
        if hook.poll_query_status(qid, max_polling_attempts=40) != "SUCCEEDED":
            raise RuntimeError(f"anomaly query {qid} did not succeed")
        rows = hook.get_query_results(qid)["ResultSet"]["Rows"]
        n = int(rows[1]["Data"][0].get("VarCharValue", 0))
        print(f"{n} cells beyond {C.RAN_ANOMALY_SIGMA} sigma in window {window}")
        return {"anomalous_cells": n, "window": window.isoformat(), "query_id": qid}

    anomalies = detect_anomalies()

    @task.branch
    def anomaly_gate(summary: dict) -> list[str]:
        # A branch may return SEVERAL task ids — both paths run here when the
        # network is degraded, because the refresh still has to happen.
        if summary["anomalous_cells"] > 0:
            return ["page_noc", "refresh_serving_view"]
        return ["refresh_serving_view"]

    page_noc = SnsPublishOperator(
        task_id="page_noc",
        target_arn=C.SNS_DATA_ALERTS,
        subject=f"[{C.ENV}] RAN degradation detected",
        message=(
            "{{ ti.xcom_pull(task_ids='detect_anomalies')['anomalous_cells'] }} cells "
            "exceeded the drop-call baseline in window "
            "{{ ti.xcom_pull(task_ids='detect_anomalies')['window'] }}. "
            "Athena query id: {{ ti.xcom_pull(task_ids='detect_anomalies')['query_id'] }}"
        ),
    )

    # -- 3. self-healing small files -----------------------------------------
    # A 15-minute writer produces 96 commits a day per partition. Left alone,
    # read cost climbs until the dashboard times out. Rather than compact on a
    # blind schedule, ask Iceberg's own metadata how bad it actually is.
    @task
    def check_file_fragmentation() -> dict:
        from airflow.providers.amazon.aws.hooks.athena import AthenaHook

        hook = AthenaHook(aws_conn_id="aws_default")
        sql = f"""
            SELECT count(*) AS data_files
            FROM "{C.GLUE_DB_GOLD}"."ran_cell_kpi$files"
            WHERE content = 0
        """
        qid = hook.run_query(
            query=sql,
            query_context={"Database": C.GLUE_DB_GOLD},
            result_configuration={"OutputLocation": C.ATHENA_RESULTS},
            workgroup=C.ATHENA_WORKGROUP,
        )
        if hook.poll_query_status(qid, max_polling_attempts=40) != "SUCCEEDED":
            raise RuntimeError("file-count query failed")
        rows = hook.get_query_results(qid)["ResultSet"]["Rows"]
        files = int(rows[1]["Data"][0].get("VarCharValue", 0))
        print(f"ran_cell_kpi holds {files} data files "
              f"(compaction threshold {C.SMALL_FILE_COMPACTION_THRESHOLD})")
        return {"data_files": files}

    fragmentation = check_file_fragmentation()

    @task.branch
    def compaction_gate(frag: dict) -> str:
        if frag["data_files"] > C.SMALL_FILE_COMPACTION_THRESHOLD:
            return "compact_partitions"
        return "compaction_not_needed"

    compact = EmrServerlessStartJobOperator(
        task_id="compact_partitions",
        application_id=C.EMR_SERVERLESS_APP_ID,
        execution_role_arn=C.EXEC_ROLE,
        # rewrite_data_files with sort ordering, then the position deletes.
        # Plain rewrite_data_files does NOT reconcile merge-on-read deletes —
        # the behaviour lab 05 of iceberg_deep proves.
        job_driver=C.spark_submit(
            "iceberg_compaction.py",
            ["--table", f"glue_catalog.{C.GLUE_DB_GOLD}.ran_cell_kpi",
             "--strategy", "sort", "--sort-by", "region,cell_id",
             "--rewrite-position-deletes", "true"],
            executors=10, executor_memory="16g",
        ),
        configuration_overrides=C.emrs_monitoring("compaction"),
        wait_for_completion=True,
        deferrable=True,
        pool=C.POOL_EMR_SERVERLESS,
        # Must stay under dagrun_timeout (14 min) or the run is killed while
        # the EMR job keeps running and billing. A compaction that needs more
        # than this is not a micro-batch concern — it belongs to DAG 05, which
        # has a six-hour budget and a maintenance lock.
        execution_timeout=timedelta(minutes=9),
    )
    no_compaction = EmptyOperator(task_id="compaction_not_needed")

    # -- 4. serving layer -----------------------------------------------------
    refresh_view = RedshiftDataOperator(
        task_id="refresh_serving_view",
        workgroup_name=C.REDSHIFT_WORKGROUP,
        database=C.REDSHIFT_DB,
        sql="REFRESH MATERIALIZED VIEW noc.mv_cell_health_15min;",
        wait_for_completion=True,
        deferrable=True,
        poll_interval=10,
        pool=C.POOL_REDSHIFT,
    )

    @task(outlets=[RAN_KPI_GOLD], trigger_rule="none_failed_min_one_success")
    def publish_window(dag_run):
        """Marks the Asset updated so downstream DAGs can subscribe to it."""
        print(f"RAN KPI window published: {C.ds_of(dag_run)} {C.hour_of(dag_run)}")

    published = publish_window()

    aggregate >> anomalies >> anomaly_gate(anomalies) >> [page_noc, refresh_view]
    aggregate >> fragmentation >> compaction_gate(fragmentation) >> [compact, no_compaction]
    [page_noc, refresh_view, compact, no_compaction] >> published
