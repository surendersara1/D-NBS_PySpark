"""
07 · Assets — data-aware scheduling (Airflow 3; "Datasets" in Airflow 2)

    Asset          a named thing that a task PRODUCES (outlets=[...])
    schedule=[a]   a DAG that runs WHEN the asset is updated, not on a clock
    TriggerDagRun  the older, explicit way: "now start that other DAG"

Two DAGs in this file:
    07a_produce_silver_orders   runs on a schedule, declares it updated the asset
    07b_consume_silver_orders   has NO cron — it runs because 07a updated the asset

This is how you chain pipelines across teams without one giant DAG and
without a cron that guesses when upstream finished. In the UI, look at the
Assets page and the dependency graph between the two DAGs.
"""
from datetime import datetime

from airflow.sdk import dag, task, Asset
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

import lab_config as C

silver_orders = Asset(
    name="silver_orders",
    uri=f"file://{C.WORK_DIR}/silver/orders",
)


@dag(
    dag_id="07a_produce_silver_orders",
    schedule=None,                        # trigger it by hand to see 07b fire
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["lab", "07-assets", "producer"],
)
def producer():

    @task(outlets=[silver_orders])        # <-- "I updated this asset"
    def write_silver(ds: str):
        import os
        d = f"{C.WORK_DIR}/silver/orders"
        os.makedirs(d, exist_ok=True)
        with open(f"{d}/{ds}.txt", "w") as fh:
            fh.write("silver orders written\n")
        print(f"silver_orders updated for {ds}")

    # The explicit alternative, for comparison. Fires the same consumer by name.
    explicit = TriggerDagRunOperator(
        task_id="explicit_trigger_alternative",
        trigger_dag_id="07b_consume_silver_orders",
        wait_for_completion=False,
    )

    write_silver() >> explicit


@dag(
    dag_id="07b_consume_silver_orders",
    schedule=[silver_orders],             # <-- no cron. Runs when the asset updates.
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["lab", "07-assets", "consumer"],
)
def consumer():

    @task
    def build_gold():
        print("upstream silver_orders changed -> rebuilding gold")


producer()
consumer()
