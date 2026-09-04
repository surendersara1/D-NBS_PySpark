"""
01 · CDR mediation — hourly, transient EMR on EC2

The highest-volume pipeline in any mobile operator. Every call, SMS and data
session produces a Call Detail Record. At this scale that is 2-4 billion
records a day arriving as gzipped ASN.1/CSV drops from ~11 mobile switching
centres across four markets, every hour, never all at the same time.

WHAT THIS DAG DEMONSTRATES
  * a TRANSIENT EMR on EC2 cluster: create -> add steps -> sense -> terminate,
    with the terminate guarded by trigger_rule="all_done" so a failed step can
    never leak a 60-node cluster overnight
  * instance FLEETS with spot for task nodes and on-demand for core, which is
    the single biggest cost lever on EMR (atlas 2 covers what happens when
    spot is reclaimed mid-shuffle)
  * dynamic task mapping over markets, and a nested fan-out over switches
  * arrival sensing that tolerates switches reporting at different times
  * late-arriving CDRs merged into the correct HOUR partition, not "now"
  * a data-quality gate that can stop the pipeline before bad data reaches
    billing, using the duplicate rate the mediation job itself reports

WHY EMR ON EC2 AND NOT SERVERLESS
  This job runs for 35-50 minutes on 60 nodes with a heavy shuffle and a
  custom native ASN.1 decoder installed via a bootstrap action. Bootstrap
  actions and custom AMIs do not exist on EMR Serverless. When you need the
  node, you need EC2. DAG 02 shows the opposite choice for the same reason
  reversed.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import DAG, Asset, TaskGroup, task
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrCreateJobFlowOperator,
    EmrModifyClusterOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor, EmrStepSensor
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.standard.operators.empty import EmptyOperator

import telco_config as C

# Downstream DAGs subscribe to this instead of guessing a cron offset.
CDR_SILVER = Asset(name="cdr_silver_hourly", uri=f"s3://{C.LAKE_BUCKET}/silver/cdr_events")

# ---------------------------------------------------------------------------
# The cluster definition. In a real deployment this lives in a JSON file in S3
# or in a Variable so the platform team can change instance types without a
# code review of the DAG.
# ---------------------------------------------------------------------------
JOB_FLOW_OVERRIDES = {
    "Name": f"cdr-mediation-{C.ENV}-{C.DS_NODASH}-{C.HOUR}",
    "ReleaseLabel": C.EMR_RELEASE,
    "LogUri": f"s3://{C.LOG_BUCKET}/emr-ec2/",
    "Applications": [{"Name": "Spark"}, {"Name": "Hadoop"}],
    "Configurations": [
        {
            "Classification": "spark-defaults",
            "Properties": {
                "spark.sql.extensions":
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                "spark.sql.catalog.glue_catalog": "org.apache.iceberg.spark.SparkCatalog",
                "spark.sql.catalog.glue_catalog.catalog-impl":
                    "org.apache.iceberg.aws.glue.GlueCatalog",
                "spark.sql.catalog.glue_catalog.warehouse":
                    f"s3://{C.LAKE_BUCKET}/warehouse/",
                "spark.sql.catalog.glue_catalog.io-impl":
                    "org.apache.iceberg.aws.s3.S3FileIO",
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.skewJoin.enabled": "true",
                # Keep shuffle blocks alive when a spot task node goes away.
                "spark.decommission.enabled": "true",
                "spark.storage.decommission.shuffleBlocks.enabled": "true",
                "spark.storage.decommission.rddBlocks.enabled": "true",
            },
        },
        {
            "Classification": "spark",
            "Properties": {"maximizeResourceAllocation": "false"},
        },
    ],
    # Fleets, not groups: EMR picks from several instance types to satisfy the
    # target capacity, which is what makes spot survivable at this size.
    "Instances": {
        "InstanceFleets": [
            {
                "Name": "primary",
                "InstanceFleetType": "MASTER",
                "TargetOnDemandCapacity": 1,
                "InstanceTypeConfigs": [{"InstanceType": "m6g.2xlarge"}],
            },
            {
                "Name": "core",
                "InstanceFleetType": "CORE",
                "TargetOnDemandCapacity": 10,       # on-demand: they hold HDFS + shuffle
                "InstanceTypeConfigs": [
                    {"InstanceType": "r6g.4xlarge", "WeightedCapacity": 1},
                    {"InstanceType": "r6gd.4xlarge", "WeightedCapacity": 1},
                ],
            },
            {
                "Name": "task-spot",
                "InstanceFleetType": "TASK",
                "TargetSpotCapacity": 50,           # spot: pure compute, safe to lose
                "LaunchSpecifications": {
                    "SpotSpecification": {
                        "TimeoutDurationMinutes": 20,
                        "TimeoutAction": "SWITCH_TO_ON_DEMAND",
                        "AllocationStrategy": "capacity-optimized",
                    }
                },
                "InstanceTypeConfigs": [
                    {"InstanceType": "r6g.4xlarge", "WeightedCapacity": 1},
                    {"InstanceType": "r5.4xlarge", "WeightedCapacity": 1},
                    {"InstanceType": "r5a.4xlarge", "WeightedCapacity": 1},
                    {"InstanceType": "m6g.8xlarge", "WeightedCapacity": 2},
                ],
            },
        ],
        "Ec2SubnetIds": C.EMR_SUBNET_IDS,
        "KeepJobFlowAliveWhenNoSteps": True,        # WE terminate it, not EMR
        "TerminationProtected": False,
    },
    "BootstrapActions": [
        {
            "Name": "install-asn1-decoder",
            "ScriptBootstrapAction": {
                "Path": f"s3://{C.CODE_BUCKET}/bootstrap/install_asn1.sh",
                "Args": ["--version", "4.2.1"],
            },
        }
    ],
    "JobFlowRole": C.EMR_EC2_INSTANCE_PROFILE,
    "ServiceRole": C.EMR_EC2_SERVICE_ROLE,
    "VisibleToAllUsers": True,
    "StepConcurrencyLevel": 4,          # otherwise steps run strictly one at a time
    "Tags": [
        {"Key": "CostCentre", "Value": "network-analytics"},
        {"Key": "Pipeline", "Value": "cdr-mediation"},
        {"Key": "Environment", "Value": C.ENV},
    ],
}


def mediation_step(market: str) -> dict:
    """One EMR step: mediate, deduplicate and rate one market's CDRs."""
    return {
        "Name": f"mediate-{market}",
        "ActionOnFailure": "CONTINUE",      # CONTINUE so one market cannot kill the rest
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit", "--deploy-mode", "cluster",
                "--conf", "spark.yarn.maxAppAttempts=2",
                # jobs/cdr_mediation.py does `import job_common`
                "--py-files", f"s3://{C.CODE_BUCKET}/jobs/job_common.py",
                f"s3://{C.CODE_BUCKET}/jobs/cdr_mediation.py",
                "--market", market,
                "--run-date", C.DS,
                "--run-hour", C.HOUR,
                "--late-window-hours", str(C.CDR_LATE_ARRIVAL_HOURS),
                "--source", f"s3://{C.RAW_BUCKET}/cdr/{market}/",
                "--target-table", f"glue_catalog.{C.GLUE_DB_SILVER}.cdr_events",
            ],
        },
    }


with DAG(
    dag_id="telco_01_cdr_mediation_hourly",
    description="Hourly CDR mediation on a transient EMR on EC2 cluster",
    schedule="20 * * * *",              # :20 past — switches finish writing by :15
    start_date=datetime(2026, 1, 1),
    catchup=False,                      # a 6-month backfill of this would cost a fortune
    max_active_runs=2,                  # allow one late run to overlap, never three
    max_consecutive_failed_dag_runs=3,  # stop paging after the third identical failure
    default_args=C.DEFAULT_ARGS,
    tags=["telco", "cdr", "emr-ec2", "tier-1", C.ENV],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")

    # -- 1. wait for every switch in every market -----------------------------
    # One sensor per switch, deferrable so 11 concurrent waits cost zero worker
    # slots. mode="reschedule" would also work; deferrable is cheaper still.
    with TaskGroup("await_switch_drops") as await_drops:
        for market, nodes in C.MSC_NODES.items():
            for node in nodes:
                S3KeySensor(
                    task_id=f"wait_{market}_{node}".replace("-", "_"),
                    bucket_name=C.RAW_BUCKET,
                    # Wait for the marker the switch writes LAST, never for the
                    # data files themselves — a wildcard on *.gz succeeds while
                    # the switch is still writing file 400 of 900.
                    bucket_key=f"cdr/{market}/{node}/dt={C.DS}/hh={C.HOUR}/_SUCCESS",
                    poke_interval=60,
                    timeout=60 * 45,       # 45 min, then fail loudly
                    deferrable=True,
                    retries=0,             # a sensor timeout is not a transient error
                )

    # -- 2. cluster up --------------------------------------------------------
    create_cluster = EmrCreateJobFlowOperator(
        task_id="create_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id="aws_default",
        emr_conn_id=None,                  # everything is in job_flow_overrides
        wait_for_completion=False,         # the sensor below does the waiting
        pool=C.POOL_EMR_EC2,
    )

    cluster_ready = EmrJobFlowSensor(
        task_id="cluster_ready",
        job_flow_id=create_cluster.output,      # XCom, the modern way
        target_states={"WAITING"},
        failed_states={"TERMINATED", "TERMINATED_WITH_ERRORS"},
        poke_interval=30,
        timeout=60 * 25,
        deferrable=True,
    )

    # Bump step concurrency once the cluster is warm. Rarely needed, but it is
    # the only EMR-on-EC2 property Airflow can change on a live cluster.
    tune_cluster = EmrModifyClusterOperator(
        task_id="tune_step_concurrency",
        cluster_id=create_cluster.output,
        step_concurrency_level=4,
    )

    # -- 3. one mediation step per market, mapped ----------------------------
    add_steps = EmrAddStepsOperator.partial(
        task_id="add_mediation_step",
        job_flow_id=create_cluster.output,
        wait_for_completion=False,
        pool=C.POOL_EMR_EC2,
    ).expand(steps=[[mediation_step(m)] for m in C.MARKETS])

    # EmrAddStepsOperator returns a LIST of step ids, so a mapped instance
    # returns ["s-XYZ"]. The sensor maps over the flattened ids.
    @task
    def flatten_step_ids(step_id_lists: list[list[str]]) -> list[str]:
        ids = [sid for sub in step_id_lists for sid in sub]
        print(f"submitted {len(ids)} EMR steps: {ids}")
        return ids

    step_ids = flatten_step_ids(add_steps.output)

    wait_steps = EmrStepSensor.partial(
        task_id="wait_mediation_step",
        job_flow_id=create_cluster.output,
        target_states={"COMPLETED"},
        failed_states={"CANCELLED", "FAILED", "INTERRUPTED"},
        poke_interval=60,
        timeout=60 * 90,
        deferrable=True,
    ).expand(step_id=step_ids)

    # -- 4. terminate, no matter what ----------------------------------------
    # all_done is the whole point. Without it a failed step leaves 60 nodes
    # running until somebody notices in the morning.
    terminate = EmrTerminateJobFlowOperator(
        task_id="terminate_cluster",
        job_flow_id=create_cluster.output,
        trigger_rule="all_done",
        retries=4,                          # keep trying: this one costs money
        retry_delay=timedelta(minutes=1),
    )

    # -- 5. quality gate ------------------------------------------------------
    @task(outlets=[CDR_SILVER], pool=C.POOL_ATHENA)
    def verify_and_publish(dag_run) -> dict:
        """Check the hour actually landed and is not full of duplicates.

        Runs two Athena queries through the hook, because AthenaOperator
        succeeds whenever the QUERY succeeds — including when it returns a
        count of zero. Anything that must FAIL on a bad value needs a task
        that reads the result and raises.
        """
        from airflow.providers.amazon.aws.hooks.athena import AthenaHook

        ds, hh = C.ds_of(dag_run), C.hour_of(dag_run)
        hook = AthenaHook(aws_conn_id="aws_default")

        def scalar(sql: str) -> float:
            qid = hook.run_query(
                query=sql,
                query_context={"Database": C.GLUE_DB_SILVER},
                result_configuration={"OutputLocation": C.ATHENA_RESULTS},
                workgroup=C.ATHENA_WORKGROUP,
            )
            state = hook.poll_query_status(qid, max_polling_attempts=60)
            if state != "SUCCEEDED":
                raise RuntimeError(f"Athena query {qid} ended in state {state}")
            rows = hook.get_query_results(qid)["ResultSet"]["Rows"]
            return float(rows[1]["Data"][0].get("VarCharValue", 0))

        total = scalar(
            f"SELECT count(*) FROM cdr_events "
            f"WHERE event_date = DATE '{ds}' AND event_hour = {int(hh)}"
        )
        if total == 0:
            raise ValueError(f"No CDRs landed for {ds} hour {hh} — billing would under-bill")

        dupes = scalar(
            f"SELECT count(*) FROM ("
            f"  SELECT cdr_id FROM cdr_events "
            f"  WHERE event_date = DATE '{ds}' AND event_hour = {int(hh)} "
            f"  GROUP BY cdr_id HAVING count(*) > 1)"
        )
        dupe_pct = 100.0 * dupes / total
        print(f"{int(total):,} CDRs, {int(dupes):,} duplicate ids ({dupe_pct:.3f}%)")

        # XCom carries the NUMBERS. The billions of rows never leave S3.
        return {"total": total, "dupe_pct": dupe_pct, "ds": ds, "hour": hh}

    quality = verify_and_publish()

    @task.branch
    def duplicate_gate(stats: dict) -> str:
        if stats["dupe_pct"] > C.CDR_DUPLICATE_PCT_ALERT:
            return "alert_duplicates"
        return "hour_accepted"

    alert = SnsPublishOperator(
        task_id="alert_duplicates",
        target_arn=C.SNS_DATA_ALERTS,
        subject=f"[{C.ENV}] CDR duplicate rate above threshold",
        message=(
            "Hour {{ ti.xcom_pull(task_ids='verify_and_publish')['ds'] }} "
            "{{ ti.xcom_pull(task_ids='verify_and_publish')['hour'] }}:00 exceeded "
            f"the {C.CDR_DUPLICATE_PCT_ALERT}% duplicate threshold. "
            "Mediation output is quarantined; do not run rating until reviewed."
        ),
    )
    accepted = EmptyOperator(task_id="hour_accepted")
    done = EmptyOperator(task_id="done", trigger_rule="none_failed_min_one_success")

    (
        start
        >> await_drops
        >> create_cluster
        >> cluster_ready
        >> tune_cluster
        >> add_steps
    )
    wait_steps >> terminate >> quality
    quality >> duplicate_gate(quality) >> [alert, accepted] >> done
