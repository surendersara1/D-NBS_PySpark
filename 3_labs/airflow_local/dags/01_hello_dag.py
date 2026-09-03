"""
01 · The anatomy of a DAG

    DAG          a schedule + a set of tasks + their dependencies
    task         one unit of work; an instance of an operator
    operator     the *kind* of work: run bash, run python, wait for a file, ...
    >>           "then" — a dependency edge
    logical date the data interval this run is FOR, not when it happened

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


def python_hello(ds, data_interval_start, data_interval_end, **_):
    # Context is injected by name. `ds` is the logical date as YYYY-MM-DD.
    print(f"logical date (ds)      : {ds}")
    print(f"data_interval_start    : {data_interval_start}")
    print(f"data_interval_end      : {data_interval_end}")
    print("An hourly DAG that runs at 09:00 processes the 08:00-09:00 interval.")
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
        bash_command=(
            'echo "ds={{ ds }}  ds_nodash={{ ds_nodash }}  '
            'interval={{ data_interval_start }} -> {{ data_interval_end }}  '
            'run_id={{ run_id }}"'
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
