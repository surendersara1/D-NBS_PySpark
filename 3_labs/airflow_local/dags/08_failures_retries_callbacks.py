"""
08 · Failure handling — the part that separates a demo from production

    retries + retry_exponential_backoff   transient failures heal themselves
    execution_timeout                     a hung task is killed, not waited on
    on_failure_callback                   page someone / post to Slack / write a ticket
    ShortCircuitOperator                  "nothing to do today" -> skip the rest cleanly
    trigger_rule="all_done"               cleanup that runs even when things failed

The flaky task below fails on its first TWO attempts and succeeds on the third.
Watch the task instance detail: try_number 1, 2 fail; 3 succeeds. Then look at
the logs of `notify_on_failure` — the callback fired on each failure.
"""
from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.providers.standard.operators.bash import BashOperator


def notify_on_failure(context):
    # In production: Slack webhook, PagerDuty, SNS. Here: a loud log line.
    ti = context["task_instance"]
    print(f"!!! FAILURE  dag={ti.dag_id}  task={ti.task_id}  "
          f"try={ti.try_number}  logical_date={context.get('logical_date')}")


@dag(
    dag_id="08_failures_retries_callbacks",
    schedule=None,
    start_date=datetime(2026, 9, 1),
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=10),
        "retry_exponential_backoff": True,      # 10s, 20s, 40s ...
        "max_retry_delay": timedelta(minutes=2),
        "on_failure_callback": notify_on_failure,
    },
    tags=["lab", "08-failures"],
)
def failures_demo():

    @task(execution_timeout=timedelta(minutes=1))
    def flaky(**context):
        n = context["task_instance"].try_number
        print(f"attempt {n}")
        if n < 3:
            raise RuntimeError(f"simulated transient failure on attempt {n}")
        return "succeeded on attempt 3"

    def has_work_today(**context):
        # e.g. "did any file land?" — False short-circuits everything downstream
        # to SKIPPED (not failed). Change to False to see it.
        return True

    gate = ShortCircuitOperator(
        task_id="anything_to_do",
        python_callable=has_work_today,
    )

    @task
    def real_work(msg: str):
        print(f"doing the work because upstream said: {msg}")

    # Runs whatever happened above — success, failure, or skip.
    cleanup = BashOperator(
        task_id="cleanup_always",
        bash_command="echo 'cleanup ran regardless of upstream state'",
        trigger_rule="all_done",
    )

    result = flaky()
    gate >> real_work(result) >> cleanup
    result >> gate


failures_demo()
