"""
03 · Sensors — waiting for the world

    sensor          a task that polls until a condition is true, then succeeds
    poke_interval   seconds between checks
    timeout         give up after this many seconds (task fails)
    mode=reschedule release the worker slot between pokes — ALWAYS use this
                    for anything that waits more than a minute
    deferrable      hand the wait to the triggerer process; zero slot cost

Local:  FileSensor watches ./landing/<ds>/orders.csv
EMR:    S3KeySensor watches s3://<bucket>/raw/<ds>/orders.csv

To unblock the local run, from the airflow_local folder:
    mkdir -p landing/2026-09-02
    echo "order_id,amount" > landing/2026-09-02/orders.csv
(replace the date with the run's ds — see the task log for the exact path)
"""
from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.providers.standard.sensors.filesystem import FileSensor

import lab_config as C


@dag(
    dag_id="03_sensor_wait_for_file",
    schedule=None,
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["lab", "03-sensors"],
)
def sensor_demo():

    if C.MODE == "emr":
        from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
        wait = S3KeySensor(
            task_id="wait_for_orders_file",
            bucket_name=C.S3_BUCKET,
            bucket_key="raw/{{ ds }}/orders.csv",
            poke_interval=30,
            timeout=60 * 60,
            mode="reschedule",
            deferrable=True,            # the triggerer does the waiting
        )
        path_expr = f"s3://{C.S3_BUCKET}/raw/{{{{ ds }}}}/orders.csv"
    else:
        wait = FileSensor(
            task_id="wait_for_orders_file",
            filepath=f"{C.LANDING_DIR}/{{{{ ds }}}}/orders.csv",
            poke_interval=10,
            timeout=60 * 10,            # 10 minutes, then FAIL — never wait forever
            mode="reschedule",
        )
        path_expr = f"{C.LANDING_DIR}/{{{{ ds }}}}/orders.csv"

    @task
    def process(path: str):
        print(f"file arrived: {path}")
        if C.MODE == "local":
            with open(path) as fh:
                n = sum(1 for _ in fh) - 1
            print(f"{n} data rows")
            return n
        return -1

    wait >> process(path_expr)


sensor_demo()
