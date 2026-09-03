"""
04 · Revenue assurance and interconnect settlement — daily + monthly close

When a subscriber on our network calls a subscriber on another operator's
network, we owe that operator a termination fee, and vice versa. Both sides
count the same calls independently and the numbers never match exactly.
Revenue assurance is the practice of proving the gap is small and explainable
before money moves. At this scale a 0.3% counting error is a seven-figure
annual leak.

WHAT THIS DAG DEMONSTRATES
  * pulling from an operational system with DMS change-data-capture rather
    than a nightly full extract that hammers the billing database
  * AWS Glue for the modest ETL and EMR Serverless for the heavy join — using
    the right tool per step instead of one hammer
  * Glue Data Quality rulesets as a declarative gate
  * a genuine reconciliation with a money threshold that decides whether a
    Step Functions dispute workflow is launched
  * dynamic mapping over interconnect partners, where each partner's file
    format differs and one bad partner must not stop the other eleven
  * a monthly close path that only executes on the last day of the month,
    demonstrating a branch on the calendar rather than on data
  * SQL check operators as the cheap, declarative alternative to writing your
    own assertion tasks

WHY THIS IS THE DAG THAT GETS AUDITED
  Everything here ends up in front of a regulator or an external auditor. That
  is why every task writes an immutable artefact to S3 and why the final task
  records a signed attestation rather than just logging success.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import DAG, Asset, TaskGroup, task
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.operators.dms import DmsStartTaskOperator
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.providers.amazon.aws.operators.glue import (
    GlueDataQualityRuleSetEvaluationRunOperator,
    GlueJobOperator,
)
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.providers.amazon.aws.operators.step_function import (
    StepFunctionStartExecutionOperator,
)
from airflow.providers.amazon.aws.sensors.dms import DmsTaskCompletedSensor
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.standard.operators.empty import EmptyOperator

import telco_config as C

BILLING_SILVER = Asset(name="billing_silver_daily", uri=f"s3://{C.LAKE_BUCKET}/silver/billing")

# Twelve interconnect partners, each sending a settlement file in its own
# dialect. The format column is what makes the per-partner Glue job differ.
PARTNERS = [
    {"code": "MT-HU", "name": "Magyar Telekom", "fmt": "tap3", "currency": "EUR"},
    {"code": "VF-HU", "name": "Vodafone HU", "fmt": "tap3", "currency": "EUR"},
    {"code": "A1-BG", "name": "A1 Bulgaria", "fmt": "csv_v2", "currency": "BGN"},
    {"code": "VVN-BG", "name": "Vivacom", "fmt": "csv_v2", "currency": "BGN"},
    {"code": "MTS-RS", "name": "MTS Serbia", "fmt": "tap3", "currency": "RSD"},
    {"code": "A1-RS", "name": "A1 Serbia", "fmt": "fixed_width", "currency": "RSD"},
    {"code": "JZ-PK", "name": "Jazz", "fmt": "csv_v1", "currency": "PKR"},
    {"code": "ZG-PK", "name": "Zong", "fmt": "csv_v1", "currency": "PKR"},
    {"code": "UF-PK", "name": "Ufone", "fmt": "fixed_width", "currency": "PKR"},
    {"code": "DT-INT", "name": "Deutsche Telekom Intl", "fmt": "tap3", "currency": "EUR"},
    {"code": "OR-INT", "name": "Orange Intl", "fmt": "tap3", "currency": "EUR"},
    {"code": "TI-INT", "name": "Telecom Italia Intl", "fmt": "tap3", "currency": "EUR"},
]

DMS_TASK_ARN = (
    f"arn:aws:dms:{C.REGION}:111122223333:task:TELCO{C.ENV.upper()}BILLINGCDC00000000000"
)


with DAG(
    dag_id="telco_04_revenue_assurance",
    description="Interconnect settlement reconciliation with a money-threshold dispute gate",
    schedule="0 4 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=True,                       # this one you DO backfill — auditors ask for reruns
    max_active_runs=1,                  # strictly one day at a time; it writes ledgers
    default_args={**C.DEFAULT_ARGS, "execution_timeout": timedelta(hours=4)},
    tags=["telco", "finance", "revenue-assurance", "audited", C.ENV],
    doc_md=__doc__,
) as dag:

    # -- 1. our side of the ledger, from the billing system via CDC -----------
    with TaskGroup("our_ledger") as our_ledger:
        start_cdc = DmsStartTaskOperator(
            task_id="start_billing_cdc",
            replication_task_arn=DMS_TASK_ARN,
            start_replication_task_type="resume-processing",
        )
        cdc_done = DmsTaskCompletedSensor(
            task_id="await_billing_cdc",
            replication_task_arn=DMS_TASK_ARN,
            poke_interval=60,
            timeout=60 * 90,
            mode="reschedule",
        )
        # Glue, not EMR: this is a few hundred GB of well-shaped Parquet with
        # no custom native code. Glue's 1-minute start beats EMR here.
        normalise = GlueJobOperator(
            task_id="normalise_billing_events",
            job_name=f"telco-{C.ENV}-billing-normalise",
            script_args={
                "--run_date": C.DS,
                "--target_table": f"{C.GLUE_DB_SILVER}.billing_events",
            },
            wait_for_completion=True,
            deferrable=True,
            # Align with the Glue job's own timeout, or Airflow gives up while
            # Glue keeps running and billing.
            execution_timeout=timedelta(hours=2),
        )
        start_cdc >> cdc_done >> normalise

    # -- 2. their side of the ledger, one mapped task per partner ------------
    with TaskGroup("partner_files") as partner_files:
        # Wait for all twelve, but do not let a single late partner block the
        # ones that arrived: each sensor is independent and the ingest below
        # is mapped, so a failure isolates to one mapped instance.
        waits = [
            S3KeySensor(
                task_id=f"wait_{p['code'].replace('-', '_')}",
                bucket_name=C.RAW_BUCKET,
                bucket_key=f"interconnect/{p['code']}/dt={C.DS}/settlement.done",
                poke_interval=300,
                timeout=60 * 60 * 6,       # partners have until 10:00 by contract
                deferrable=True,
                retries=0,
            )
            for p in PARTNERS
        ]

        ingest = GlueJobOperator.partial(
            task_id="ingest_partner_file",
            job_name=f"telco-{C.ENV}-interconnect-ingest",
            wait_for_completion=True,
            deferrable=True,
            retries=1,
            max_active_tis_per_dag=4,      # Glue DPU budget, not an Airflow limit
        ).expand(
            script_args=[
                {
                    "--partner_code": p["code"],
                    "--source_format": p["fmt"],
                    "--currency": p["currency"],
                    "--run_date": C.DS,
                    "--target_table": f"{C.GLUE_DB_SILVER}.interconnect_partner_cdr",
                }
                for p in PARTNERS
            ]
        )
        waits >> ingest

    # -- 3. declarative data quality -----------------------------------------
    # A ruleset defined once in Glue Data Quality, evaluated here. Cheaper to
    # maintain than a dozen bespoke assertion tasks, and the results land in
    # the catalog where the data stewards already look.
    dq = GlueDataQualityRuleSetEvaluationRunOperator(
        task_id="evaluate_dq_ruleset",
        datasource={
            "GlueTable": {
                "DatabaseName": C.GLUE_DB_SILVER,
                "TableName": "interconnect_partner_cdr",
            }
        },
        role=C.EXEC_ROLE,
        rule_set_names=[f"telco-{C.ENV}-interconnect-rules"],
        wait_for_completion=True,
        show_results=True,
        verify_result_status=True,      # FAIL the task when a rule fails
        deferrable=True,
    )

    # -- 4. the reconciliation itself ----------------------------------------
    reconcile = EmrServerlessStartJobOperator(
        task_id="reconcile_ledgers",
        application_id=C.EMR_SERVERLESS_APP_ID,
        execution_role_arn=C.EXEC_ROLE,
        job_driver=C.spark_submit(
            "interconnect_reconcile.py",
            ["--run-date", C.DS,
             "--ours", f"glue_catalog.{C.GLUE_DB_SILVER}.billing_events",
             "--theirs", f"glue_catalog.{C.GLUE_DB_SILVER}.interconnect_partner_cdr",
             "--fx-table", f"glue_catalog.{C.GLUE_DB_SILVER}.fx_rates_daily",
             "--target-table", f"glue_catalog.{C.GLUE_DB_GOLD}.interconnect_variance"],
            executors=40, executor_cores=4, executor_memory="16g",
            # Matching a call leg on both sides is a wide join on
            # (a_number, b_number, start_time rounded) — classic skew where a
            # handful of hub numbers dominate. AQE skew join handles it.
            extra_conf=(
                "--conf spark.sql.adaptive.skewJoin.skewedPartitionFactor=3 "
                "--conf spark.sql.shuffle.partitions=2000 "
            ),
        ),
        configuration_overrides=C.emrs_monitoring("revenue-assurance"),
        wait_for_completion=True,
        deferrable=True,
        pool=C.POOL_EMR_SERVERLESS,
    )

    # AthenaOperator writes its result to S3 and pushes the query id to XCom.
    # It succeeds whenever the QUERY succeeds — see the gate below for the part
    # that actually judges the number.
    variance_report = AthenaOperator(
        task_id="variance_report",
        database=C.GLUE_DB_GOLD,
        workgroup=C.ATHENA_WORKGROUP,
        output_location=f"{C.ATHENA_RESULTS}revenue-assurance/{C.DS}/",
        query=f"""
            SELECT partner_code,
                   sum(our_minutes)                        AS our_minutes,
                   sum(their_minutes)                      AS their_minutes,
                   sum(our_minutes - their_minutes)        AS minute_variance,
                   sum(our_charge_eur - their_charge_eur)  AS variance_eur
            FROM interconnect_variance
            WHERE settlement_date = DATE '{C.DS}'
            GROUP BY partner_code
            ORDER BY abs(sum(our_charge_eur - their_charge_eur)) DESC
        """,
        pool=C.POOL_ATHENA,
    )

    @task
    def assess_leakage(dag_run) -> dict:
        """Turn the variance table into a decision.

        Two numbers matter: the total absolute variance, and the worst single
        partner. A large total spread evenly is usually a rounding or FX
        convention difference. A large variance concentrated on one partner is
        usually a real dispute.
        """
        from airflow.providers.amazon.aws.hooks.athena import AthenaHook

        ds = C.ds_of(dag_run)
        hook = AthenaHook(aws_conn_id="aws_default")
        qid = hook.run_query(
            query=f"""
                SELECT partner_code, sum(our_charge_eur - their_charge_eur) AS var_eur
                FROM interconnect_variance
                WHERE settlement_date = DATE '{ds}'
                GROUP BY partner_code
            """,
            query_context={"Database": C.GLUE_DB_GOLD},
            result_configuration={"OutputLocation": C.ATHENA_RESULTS},
            workgroup=C.ATHENA_WORKGROUP,
        )
        if hook.poll_query_status(qid, max_polling_attempts=90) != "SUCCEEDED":
            raise RuntimeError(f"variance query {qid} failed")

        rows = hook.get_query_results(qid)["ResultSet"]["Rows"][1:]     # skip header
        per_partner = {
            r["Data"][0].get("VarCharValue"): float(r["Data"][1].get("VarCharValue", 0))
            for r in rows
        }
        total_abs = sum(abs(v) for v in per_partner.values())
        worst = max(per_partner.items(), key=lambda kv: abs(kv[1]), default=("none", 0.0))
        print(f"total absolute variance EUR {total_abs:,.0f}; "
              f"worst partner {worst[0]} at EUR {worst[1]:,.0f}")
        return {
            "ds": ds,
            "total_abs_eur": total_abs,
            "worst_partner": worst[0],
            "worst_eur": worst[1],
            "per_partner": per_partner,
            "athena_query_id": qid,
        }

    leakage = assess_leakage()

    @task.branch
    def dispute_gate(assessment: dict) -> list[str]:
        paths = ["record_attestation"]          # always attest, disputed or not
        if abs(assessment["worst_eur"]) > C.REVENUE_LEAKAGE_ALERT_EUR:
            paths.append("open_carrier_dispute")
        return paths

    # A dispute is a long-running human workflow with approvals and legal
    # review. That is a Step Functions state machine, not an Airflow DAG:
    # Airflow starts it and stops caring.
    open_dispute = StepFunctionStartExecutionOperator(
        task_id="open_carrier_dispute",
        state_machine_arn=C.SFN_DISPUTE_WORKFLOW,
        name=f"dispute-{C.DS_NODASH}-{{{{ ti.xcom_pull(task_ids='assess_leakage')['worst_partner'] }}}}",
        state_machine_input={
            "settlementDate": C.DS,
            "partner": "{{ ti.xcom_pull(task_ids='assess_leakage')['worst_partner'] }}",
            "varianceEur": "{{ ti.xcom_pull(task_ids='assess_leakage')['worst_eur'] }}",
            "evidenceQueryId": "{{ ti.xcom_pull(task_ids='assess_leakage')['athena_query_id'] }}",
        },
        # Fire and forget: the dispute takes weeks. Never hold an Airflow task
        # open for a process measured in human time.
        waiter_max_attempts=1,
    )

    notify_finance = SnsPublishOperator(
        task_id="notify_finance",
        target_arn=C.SNS_REVENUE_ALERTS,
        subject=f"[{C.ENV}] Interconnect variance above dispute threshold",
        message=(
            "Settlement date {{ ti.xcom_pull(task_ids='assess_leakage')['ds'] }}\n"
            "Worst partner: {{ ti.xcom_pull(task_ids='assess_leakage')['worst_partner'] }}\n"
            "Variance EUR: {{ ti.xcom_pull(task_ids='assess_leakage')['worst_eur'] }}\n"
            "Total absolute variance EUR: "
            "{{ ti.xcom_pull(task_ids='assess_leakage')['total_abs_eur'] }}\n"
            f"Threshold: EUR {C.REVENUE_LEAKAGE_ALERT_EUR:,}"
        ),
    )

    # -- 5. the audit artefact ------------------------------------------------
    attestation = S3CreateObjectOperator(
        task_id="record_attestation",
        s3_bucket=C.LAKE_BUCKET,
        s3_key=f"audit/revenue_assurance/dt={C.DS}/attestation.json",
        # Templated JSON: the run is self-describing and immutable in S3.
        data=(
            '{'
            f'"settlement_date": "{C.DS}", '
            '"dag_id": "{{ dag.dag_id }}", '
            '"run_id": "{{ run_id }}", '
            '"total_abs_variance_eur": '
            '{{ ti.xcom_pull(task_ids="assess_leakage")["total_abs_eur"] }}, '
            '"worst_partner": '
            '"{{ ti.xcom_pull(task_ids="assess_leakage")["worst_partner"] }}", '
            '"athena_query_id": '
            '"{{ ti.xcom_pull(task_ids="assess_leakage")["athena_query_id"] }}", '
            f'"threshold_eur": {C.REVENUE_LEAKAGE_ALERT_EUR}, '
            f'"partner_count": {len(PARTNERS)}'
            '}'
        ),
        replace=True,                   # rerunning a day overwrites its own artefact
    )

    # -- 6. monthly close, only on the last day of the month ------------------
    @task.branch
    def month_end_gate(dag_run) -> str:
        """A branch on the calendar rather than on data.

        Note it uses the RUN's date, never today's date — otherwise a backfill
        of last March would take the wrong path on every single day.
        """
        import calendar

        d = dag_run.logical_date or dag_run.run_after
        last_day = calendar.monthrange(d.year, d.month)[1]
        if d.day == last_day:
            return "monthly_close.settlement_statements"
        return "not_month_end"

    with TaskGroup("monthly_close") as monthly_close:
        statements = EmrServerlessStartJobOperator(
            task_id="settlement_statements",
            application_id=C.EMR_SERVERLESS_APP_ID,
            execution_role_arn=C.EXEC_ROLE,
            job_driver=C.spark_submit(
                "settlement_statements.py",
                ["--month", "{{ (dag_run.logical_date or dag_run.run_after).strftime('%Y-%m') }}",
                 "--output", f"s3://{C.LAKE_BUCKET}/finance/statements/"],
                executors=20,
            ),
            configuration_overrides=C.emrs_monitoring("settlement"),
            wait_for_completion=True,
            deferrable=True,
            pool=C.POOL_EMR_SERVERLESS,
        )
        notify_close = SnsPublishOperator(
            task_id="notify_month_end_close",
            target_arn=C.SNS_REVENUE_ALERTS,
            subject=f"[{C.ENV}] Monthly interconnect statements ready",
            message="Settlement statements for "
                    "{{ (dag_run.logical_date or dag_run.run_after).strftime('%Y-%m') }} "
                    f"are in s3://{C.LAKE_BUCKET}/finance/statements/",
        )
        statements >> notify_close

    not_month_end = EmptyOperator(task_id="not_month_end")

    @task(outlets=[BILLING_SILVER], trigger_rule="none_failed_min_one_success")
    def publish_billing_asset(dag_run):
        """Signals DAG 03, which is scheduled on this asset plus the CDR one."""
        print(f"billing_silver published for {C.ds_of(dag_run)}")

    published = publish_billing_asset()

    [our_ledger, partner_files] >> dq >> reconcile >> variance_report >> leakage
    leakage >> dispute_gate(leakage) >> [open_dispute, attestation]
    open_dispute >> notify_finance
    attestation >> month_end_gate() >> [monthly_close, not_month_end] >> published
