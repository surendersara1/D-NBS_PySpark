"""
06 · catchup, backfill, and the data interval

    catchup=True   on unpause, the scheduler creates ONE RUN PER MISSED INTERVAL
                   from start_date to now. This DAG's start_date is 7 days ago,
                   so unpausing it creates 7 runs immediately. Watch the grid.
    backfill       the CLI version of the same thing, for a date range you pick:
                     docker compose run --rm airflow-cli \
                       airflow backfill create --dag-id 06_backfill_and_catchup \
                       --from-date 2026-08-20 --to-date 2026-08-25
    data interval  the run for logical date D covers [D, D+1). Write outputs
                   keyed by that interval, never by "now", or backfills lie.

The task is written so re-running any interval overwrites its own output —
that idempotency is what makes backfill SAFE. Compare with an append.
"""
from datetime import datetime, timedelta

from airflow.sdk import dag, task

import lab_config as C


@dag(
    dag_id="06_backfill_and_catchup",
    schedule="@daily",
    start_date=datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
               - timedelta(days=7),
    catchup=True,                         # <-- the whole lesson
    max_active_runs=3,                    # bound how many catch-up runs run at once
    tags=["lab", "06-backfill"],
)
def backfill_demo():

    @task
    def write_partition(ds: str, data_interval_start=None, data_interval_end=None):
        import os
        out_dir = f"{C.WORK_DIR}/backfill"
        os.makedirs(out_dir, exist_ok=True)
        path = f"{out_dir}/{ds}.txt"
        # OVERWRITE, keyed by the interval. Run it ten times, same file.
        with open(path, "w") as fh:
            fh.write(f"interval {data_interval_start} -> {data_interval_end}\n")
            fh.write(f"written at {datetime.utcnow().isoformat()}Z\n")
        print(f"wrote {path}")
        return path

    write_partition()


backfill_demo()
