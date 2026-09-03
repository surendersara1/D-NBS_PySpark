"""
05 · Dynamic task mapping — fan out over data you only know at run time

    .expand()      one task definition -> N task instances, one per input
    .partial()     fixed arguments shared by every mapped instance
    fan-in         a downstream task receives the LIST of all mapped results

Use it for: one task per partition / per file / per table / per region.
Do NOT use it for: one task per ROW. XCom and the scheduler are not built for
a million mapped instances. If the list is large, map over batches.

In the UI: the mapped task shows "[N]" and expands to one row per instance.
"""
from datetime import datetime, timedelta

from airflow.sdk import dag, task

import lab_config as C


@dag(
    dag_id="05_dynamic_task_mapping",
    schedule=None,
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["lab", "05-mapping"],
)
def mapping_demo():

    @task
    def list_partitions(data_interval_end=None) -> list[str]:
        # Real life: list S3 prefixes, query the Iceberg `partitions` metadata
        # table, or read a manifest. Here: the last 5 days.
        end = data_interval_end or datetime(2026, 9, 2)
        days = [(end - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(5, 0, -1)]
        print(f"partitions to process: {days}")
        return days

    @task(max_active_tis_per_dag=2)       # at most 2 of the mapped instances at once
    def process_partition(day: str, target: str) -> dict:
        import os, time
        out_dir = f"{target}/mapped"
        os.makedirs(out_dir, exist_ok=True)
        path = f"{out_dir}/{day}.txt"
        with open(path, "w") as fh:            # overwrite -> idempotent
            fh.write(f"processed {day}\n")
        time.sleep(2)
        return {"day": day, "path": path, "rows": 1000 + hash(day) % 500}

    @task
    def summarise(results: list[dict]):
        total = sum(r["rows"] for r in results)
        print(f"{len(results)} partitions, {total} rows total")
        for r in results:
            print(f"  {r['day']} -> {r['path']} ({r['rows']} rows)")
        return total

    days = list_partitions()
    done = process_partition.partial(target=C.WORK_DIR).expand(day=days)
    summarise(done)


mapping_demo()
