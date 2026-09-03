"""
01 · The anatomy of a DAG

    DAG          a schedule + a set of tasks + their dependencies
    task         one unit of work; an instance of an operator
    operator     the *kind* of work: run bash, run python, wait for a file, ...
    >>           "then" — a dependency edge
    logical date the data interval this run is FOR, not when it happened
    manual run   has NO logical date in Airflow 3 — {{ ds }} is undefined
                 and a task asking for `ds` fails. See print_dates for the idiom.

What to look at in the UI:
  * Grid view: one column per run, one row per task
  * Click a task -> Logs.  Find the printed {{ ds }} and data interval.
  * Trigger the DAG manually twice; note that both runs share the same
    logical date shape but are distinct run_ids.
"""
from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

import lab_config as C


def python_hello(dag_run, **_):
    # Context is injected by name. dag_run always exists; its logical_date is
    # None for a manual run, so derive the date defensively.
    print(f"logical_date           : {dag_run.logical_date}   (None => manual trigger)")
    print(f"run_after              : {dag_run.run_after}")
    print(f"data interval          : {dag_run.data_interval_start} -> {dag_run.data_interval_end}")
    print(f"run date used for paths: {C.run_date(dag_run)}")
    print("A daily DAG scheduled at 00:00 on the 3rd processes the 2nd; its logical date is the 2nd.")
    return "hello from python"


with DAG(
    dag_id="01_hello_dag",
    description="DAG anatomy: two operators, one dependency chain, templating",
    schedule="@daily",                       # cron string, preset, timedelta, or None
    start_date=datetime(2026, 9, 1),
    catchup=False,                           # do NOT backfill missed days on unpause
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["lab", "01-anatomy"],
) as dag:

    print_dates = BashOperator(
        task_id="print_dates",
        # Jinja templating. These are the variables you will use constantly.
        # Jinja templating. dag_run.* is safe for scheduled AND manual runs;
        # bare {{ ds }} / {{ data_interval_start }} exist only for scheduled runs.
        bash_command=(
            'echo "run_id={{ run_id }}  logical_date={{ dag_run.logical_date }}  '
            'run_after={{ dag_run.run_after }}  '
            'interval={{ dag_run.data_interval_start }} -> {{ dag_run.data_interval_end }}  '
            f'ds={C.DS}"'
        ),
    )

    say_hello = PythonOperator(
        task_id="python_hello",
        python_callable=python_hello,
    )

    finish = BashOperator(
        task_id="finish",
        bash_command="sleep 2 && echo 'all three tasks ran, in order'",
    )

    # Dependencies. Read as "then".
    print_dates >> say_hello >> finish
