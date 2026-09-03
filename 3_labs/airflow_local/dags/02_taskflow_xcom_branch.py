"""
02 · TaskFlow, XCom, branching, trigger rules

    @task         a Python function IS the task; return value goes to XCom
    XCom          small cross-task values (ids, counts, paths) — never DataFrames
    @task.branch  returns the task_id(s) to run next; the rest are skipped
    trigger_rule  when does a task run given its upstream states?
                  default all_success  ->  a skipped parent blocks the join

This is how 90% of real DAGs are written today. Compare with 01, which uses
the classic operator style — both are valid, TaskFlow is shorter.
"""
from datetime import datetime

from airflow.sdk import dag, task
from airflow.providers.standard.operators.empty import EmptyOperator


@dag(
    dag_id="02_taskflow_xcom_branch",
    schedule=None,                       # manual trigger only
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["lab", "02-taskflow"],
)
def taskflow_demo():

    @task
    def extract() -> dict:
        # In real life: count rows landed, list files, read a watermark.
        rows = 1_240_331
        print(f"extracted {rows} rows")
        return {"rows": rows, "source": "orders_2026-09-02.json"}   # -> XCom

    @task
    def transform(payload: dict) -> int:
        # The argument IS the upstream XCom. No ti.xcom_pull() boilerplate.
        rows = payload["rows"]
        print(f"transforming {rows} rows from {payload['source']}")
        return rows

    @task.branch
    def decide(rows: int) -> str:
        # Return the task_id to follow. Everything else downstream is SKIPPED.
        return "big_load_path" if rows > 1_000_000 else "small_load_path"

    big   = EmptyOperator(task_id="big_load_path")
    small = EmptyOperator(task_id="small_load_path")

    # The join: one of its parents will be SKIPPED. With the default
    # all_success it would never run. This rule says "run if nobody failed and
    # at least one parent succeeded".
    join = EmptyOperator(
        task_id="join",
        trigger_rule="none_failed_min_one_success",
    )

    @task
    def load(rows: int):
        print(f"loaded {rows} rows. XCom carried a number, not the data.")

    payload = extract()
    rows = transform(payload)
    decide(rows) >> [big, small] >> join >> load(rows)


taskflow_demo()
