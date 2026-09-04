"""
telco_config.py — one place for every account id, ARN, bucket and tuning knob
used by the five enterprise DAGs.

Modelled on a multi-country European mobile operator at Yettel scale:
roughly 10 million subscribers across four markets (HU, BG, RS, PK-style),
~40,000 radio cells, 2-4 billion call detail records per day at peak, and a
lakehouse of Iceberg tables on S3 catalogued in Glue.

Nothing here is real. Every ARN, bucket and application id is a placeholder.
These DAGs are written to PARSE and to be read as production-shaped reference
code; running them needs a real AWS account, so treat them as the answer to
"what does a serious Airflow deployment actually look like?", not as a lab you
execute on a laptop. The runnable lab is ../airflow_local.

Every value is read from an ENVIRONMENT VARIABLE with a literal default, not
from an Airflow Variable. That is deliberate and it is the single most
important performance rule in a big Airflow deployment:

    Variable.get() at the top level of a DAG file executes on EVERY PARSE.
    The dag-processor re-parses every file every ~30 seconds. Fifty DAGs each
    fetching three Variables at import time is 18,000 round trips an hour that
    do nothing. It is the classic way a new team makes their scheduler crawl.

Read Airflow Variables in one of the two safe places instead:
    inside a task    ->  Variable.get("telco_raw_bucket", default="fallback")
    in a template    ->  "{{ var.value.telco_raw_bucket }}"     (resolved at run time)

AIRFLOW 3 TRAP, verified against 3.3.1: the Task SDK renamed the argument.
    Airflow 2:  Variable.get("k", default_var="x")   # airflow.models.Variable
    Airflow 3:  Variable.get("k", default="x")       # airflow.sdk.Variable
Passing default_var to the SDK Variable raises TypeError at parse time and
takes the whole DAG out of the UI.

On MWAA these environment variables are set in the environment configuration,
or backed by Secrets Manager through the secrets backend.
"""
from __future__ import annotations

import os


def cfg(name: str, default: str) -> str:
    """Config from the environment. Cheap, and safe to call at parse time.

    Contrast with Variable.get(), which is a network call — see the module
    docstring for why that must never happen at import time.
    """
    return os.environ.get(name.upper(), default)


# ---------------------------------------------------------------- environment
ENV = cfg("telco_env", "prod")                 # dev | uat | prod
REGION = cfg("telco_region", "eu-central-1")

# ---------------------------------------------------------------- markets
# Every market is a separate legal entity with its own retention rules and its
# own switch estate. Almost every DAG fans out over this list with .expand().
MARKETS = ["HU", "BG", "RS", "PK"]

# Mobile switching centres that drop call detail records into the landing zone.
# In reality this comes from a config table; a literal keeps the DAG readable.
MSC_NODES = {
    "HU": ["msc-bud-01", "msc-bud-02", "msc-deb-01"],
    "BG": ["msc-sof-01", "msc-sof-02", "msc-var-01"],
    "RS": ["msc-bgd-01", "msc-bgd-02"],
    "PK": ["msc-khi-01", "msc-lhr-01", "msc-isb-01"],
}

# ---------------------------------------------------------------- storage
RAW_BUCKET = cfg("telco_raw_bucket", f"telco-{ENV}-raw-{REGION}")
LAKE_BUCKET = cfg("telco_lake_bucket", f"telco-{ENV}-lakehouse-{REGION}")
CODE_BUCKET = cfg("telco_code_bucket", f"telco-{ENV}-emr-code")
LOG_BUCKET = cfg("telco_log_bucket", f"telco-{ENV}-emr-logs")
ATHENA_RESULTS = f"s3://telco-{ENV}-athena-results/"

GLUE_DB_BRONZE = f"telco_{ENV}_bronze"
GLUE_DB_SILVER = f"telco_{ENV}_silver"
GLUE_DB_GOLD = f"telco_{ENV}_gold"

# ---------------------------------------------------------------- compute
# Three EMR deployment models, one per workload shape. Chapter 5 of the
# operator catalog explains why each workload lands where it does.
EMR_SERVERLESS_APP_ID = cfg("telco_emrs_app_id", "00fabcdefghij1k2")
EMR_EKS_VIRTUAL_CLUSTER_ID = cfg("telco_emr_eks_vc_id", "abc1def2ghi3jkl4mno5pqr6s")
EMR_RELEASE = "emr-7.5.0"                 # 7.5+ is the S3 Tables / Iceberg floor
EMR_EKS_RELEASE = "emr-7.5.0-latest"

EXEC_ROLE = cfg("telco_emr_exec_role", f"arn:aws:iam::111122223333:role/telco-{ENV}-emr-execution")
EMR_EC2_SERVICE_ROLE = "EMR_DefaultRole"
EMR_EC2_INSTANCE_PROFILE = "EMR_EC2_DefaultRole"
EMR_SUBNET_IDS = ["subnet-0a1b2c3d4e5f60718", "subnet-0a1b2c3d4e5f60719"]

# ---------------------------------------------------------------- warehouse
REDSHIFT_WORKGROUP = cfg("telco_redshift_workgroup", f"telco-{ENV}-serving")
REDSHIFT_DB = "analytics"
ATHENA_WORKGROUP = cfg("telco_athena_workgroup", f"telco-{ENV}-wg")

# ---------------------------------------------------------------- messaging
SNS_DATA_ALERTS = cfg("telco_sns_alerts", "arn:aws:sns:eu-central-1:111122223333:telco-data-platform-alerts")
SNS_REVENUE_ALERTS = cfg("telco_sns_revenue", "arn:aws:sns:eu-central-1:111122223333:telco-revenue-assurance")
SQS_DLQ = cfg("telco_sqs_dlq", "https://sqs.eu-central-1.amazonaws.com/111122223333/telco-pipeline-dlq")

# ---------------------------------------------------------------- ML
SAGEMAKER_ROLE = f"arn:aws:iam::111122223333:role/telco-{ENV}-sagemaker"
CHURN_MODEL_PACKAGE_GROUP = "telco-churn-xgboost"
SFN_DISPUTE_WORKFLOW = (
    f"arn:aws:states:eu-central-1:111122223333:stateMachine:telco-{ENV}-carrier-dispute"
)

# ---------------------------------------------------------------- thresholds
# Business rules that make the branches in these DAGs mean something.
CDR_LATE_ARRIVAL_HOURS = 6          # how far back a late CDR can still be merged
CDR_DUPLICATE_PCT_ALERT = 0.5       # % duplicate CDRs that triggers an alert
RAN_ANOMALY_SIGMA = 3.0             # cell KPI deviation that pages the NOC
SMALL_FILE_COMPACTION_THRESHOLD = 500   # data files per partition before compaction
CHURN_DRIFT_PSI_THRESHOLD = 0.20    # population stability index that forces retrain
REVENUE_LEAKAGE_ALERT_EUR = 50_000  # monthly interconnect variance that opens a dispute
GDPR_ERASURE_SLA_DAYS = 30          # regulatory deadline for a deletion request

# ---------------------------------------------------------------- pools
# Pools are the platform team's only real defence against one team's backfill
# starving everybody else. Slot counts are set in Admin -> Pools.
POOL_EMR_SERVERLESS = "emr_serverless"     # e.g. 20 slots
POOL_EMR_EC2 = "emr_ec2_transient"         # e.g. 4 slots — clusters are expensive
POOL_REDSHIFT = "redshift_serving"         # e.g. 6 slots — protect the BI users
POOL_ATHENA = "athena_queries"             # e.g. 25 slots — DML concurrency limit


# ---------------------------------------------------------------- helpers
def ds_of(dag_run) -> str:
    """Run date as YYYY-MM-DD, correct for scheduled AND manual runs.

    Airflow 3: a manually triggered run has NO logical date, so `ds` is
    undefined rather than None. Every DAG here goes through this helper.
    """
    return (dag_run.logical_date or dag_run.run_after).strftime("%Y-%m-%d")


def hour_of(dag_run) -> str:
    return (dag_run.logical_date or dag_run.run_after).strftime("%H")


# Jinja equivalents of the two helpers above, for templated operator fields.
DS = "{{ (dag_run.logical_date or dag_run.run_after) | ds }}"
DS_NODASH = "{{ (dag_run.logical_date or dag_run.run_after).strftime('%Y%m%d') }}"
HOUR = "{{ (dag_run.logical_date or dag_run.run_after).strftime('%H') }}"


def spark_submit(entry: str, args: list[str], *, executors: int = 20,
                 executor_cores: int = 4, executor_memory: str = "16g",
                 driver_memory: str = "8g", extra_conf: str = "") -> dict:
    """job_driver for EmrServerlessStartJobOperator, with Iceberg wired in.

    The Iceberg extension and catalog conf are identical to what the
    iceberg_deep lab sets in a SparkSession — the difference is only that
    EMR Serverless takes it as --conf on the submit rather than in code.
    """
    conf = (
        f"--conf spark.executor.instances={executors} "
        f"--conf spark.executor.cores={executor_cores} "
        f"--conf spark.executor.memory={executor_memory} "
        f"--conf spark.driver.memory={driver_memory} "
        "--conf spark.sql.extensions="
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "
        "--conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog "
        "--conf spark.sql.catalog.glue_catalog.catalog-impl="
        "org.apache.iceberg.aws.glue.GlueCatalog "
        f"--conf spark.sql.catalog.glue_catalog.warehouse=s3://{LAKE_BUCKET}/warehouse/ "
        "--conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO "
        "--conf spark.sql.adaptive.enabled=true "
        "--conf spark.sql.adaptive.skewJoin.enabled=true "
        # Every job does `import job_common`, so the shared module has to be
        # shipped with the submit. Without --py-files the job dies on the
        # cluster with ModuleNotFoundError while working fine locally.
        f"--py-files s3://{CODE_BUCKET}/jobs/job_common.py "
        f"{extra_conf}"
    ).strip()
    return {
        "sparkSubmit": {
            "entryPoint": f"s3://{CODE_BUCKET}/jobs/{entry}",
            "entryPointArguments": args,
            "sparkSubmitParameters": conf,
        }
    }


def emrs_monitoring(prefix: str) -> dict:
    """configuration_overrides for EMR Serverless — always ship the logs."""
    return {
        "monitoringConfiguration": {
            "s3MonitoringConfiguration": {"logUri": f"s3://{LOG_BUCKET}/{prefix}/"}
        }
    }


def notify_platform(context) -> None:
    """on_failure_callback used by every DAG here.

    Publishing to SNS from a callback needs a hook, not an operator: callbacks
    are plain functions, they are not tasks and cannot have operators inside.
    That distinction catches everyone once.
    """
    from airflow.providers.amazon.aws.hooks.sns import SnsHook

    ti = context["task_instance"]
    dag_run = context["dag_run"]
    subject = f"[{ENV}] Airflow FAILED: {ti.dag_id}.{ti.task_id}"[:100]
    body = (
        f"dag_id       : {ti.dag_id}\n"
        f"task_id      : {ti.task_id}\n"
        f"run_id       : {dag_run.run_id}\n"
        f"try_number   : {ti.try_number}\n"
        f"logical_date : {dag_run.logical_date}\n"
        f"log_url      : {getattr(ti, 'log_url', 'see the Airflow UI')}\n"
    )
    try:
        SnsHook(aws_conn_id="aws_default").publish_to_target(
            target_arn=SNS_DATA_ALERTS, message=body, subject=subject
        )
    except Exception as exc:                       # never let alerting fail a task
        print(f"SNS publish failed, falling back to log only: {exc}\n{body}")


DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": __import__("datetime").timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": __import__("datetime").timedelta(minutes=30),
    "on_failure_callback": notify_platform,
    "depends_on_past": False,
}
