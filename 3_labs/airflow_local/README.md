# Airflow local lab — 2 days from zero to orchestrating the Iceberg pipeline

Apache Airflow 3.3.1 in Docker, the same way `pyspark30` and `iceberg_deep` run
locally. Eight DAGs, each teaching one idea, ending with a DAG that runs the six
`iceberg_deep` scripts as a bronze → silver → gold pipeline — locally, or on
EMR Serverless by flipping one flag.

Read `2_atlases/8_Airflow_Orchestration_Atlas.html` first (what it is, what it
is not, how it fits EMR/Iceberg, vs dbt). Keep
`2_atlases/9_Airflow_The_30_Building_Blocks.html` open while you work.

## Prerequisites

- Docker Desktop with **at least 6 GB free on the drive that holds Docker's disk
  image** (Settings → Resources → Advanced → *Disk image location*). The Airflow
  image plus Java plus PySpark is ~3.5 GB; Postgres ~0.4 GB.
- Nothing else. No Python on the host, no winutils, no Java — it is all in the
  container. (That is also why the Iceberg warehouse the DAGs build lives inside
  `./work/`, separate from the Windows warehouse in `../iceberg_deep/warehouse/`
  — Iceberg stores absolute paths and cannot share one warehouse across OSes.)

## Start

```bash
cd 3_labs/airflow_local
docker compose up -d --build        # first time ~4-6 min: pulls airflow, adds JRE + pyspark
docker compose ps                   # wait until apiserver/scheduler/dag-processor/triggerer say healthy
```

Open http://localhost:8080 — no login (SimpleAuthManager, all admins; a lab
setting, never production). All DAGs start **paused**; unpause the one you are
working on.

```bash
docker compose logs -f airflow-scheduler      # what the brain is doing
docker compose logs airflow-dag-processor     # import errors show up here
docker compose down                           # stop; keeps the DB and the Ivy cache
docker compose down -v                        # stop and wipe everything
```

## What the compose file is

The official Airflow 3.3.1 compose, cut down: `LocalExecutor` (no Redis, no
Celery worker, no Flower), `SimpleAuthManager`, a custom image
(`Dockerfile`) that adds a JRE and PySpark 4.1.3 so a `BashOperator` can run
Spark in-container. Five Airflow processes remain — `api-server`, `scheduler`,
`dag-processor`, `triggerer`, plus the one-shot `airflow-init` — and Postgres.
Atlas 8 §02 explains each.

Bind mounts:

| host                 | container                        | purpose |
|----------------------|----------------------------------|---------|
| `./dags`             | `/opt/airflow/dags`              | edit a DAG, it reloads in ~15 s |
| `./logs`             | `/opt/airflow/logs`              | task logs, also viewable in the UI |
| `./work`             | `/opt/airflow/work`              | Spark warehouse and DAG outputs |
| `./landing`          | `/opt/airflow/landing`           | drop files here for the sensor DAG |
| `../iceberg_deep`    | `/opt/airflow/labs/iceberg_deep` | read-only; DAG 04 runs these |

## The DAGs, in order

| DAG | teaches | do this |
|-----|---------|---------|
| `01_hello_dag` | DAG anatomy, `>>`, `{{ ds }}`, data interval | unpause, trigger, read all three logs |
| `02_taskflow_xcom_branch` | `@task`, XCom, `@task.branch`, trigger rules | trigger; note one path is *skipped* and the join still runs |
| `03_sensor_wait_for_file` | sensors, `mode="reschedule"`, timeout | trigger, then create `landing/<ds>/orders.csv` and watch it unblock |
| `04_medallion_iceberg_pipeline` | **the integration**: TaskGroups, pools, the local/EMR switch | trigger; ~8 min locally; inspect `work/warehouse/`. Then set `LAB_MODE=emr` in `.env` |
| `05_dynamic_task_mapping` | `expand()` / `partial()`, fan-in | trigger; expand the `[5]` mapped task in the grid |
| `06_backfill_and_catchup` | `catchup=True`, idempotent interval writes, backfill CLI | **unpause and watch 7 runs appear**; then run the backfill command in its docstring |
| `07a/07b_*_silver_orders` | Assets (data-aware scheduling) vs `TriggerDagRunOperator` | unpause both; trigger 07a; 07b runs twice — once per mechanism. Explain why |
| `08_failures_retries_callbacks` | retries + backoff, timeout, callbacks, short-circuit, `all_done` | trigger; watch try 1 and 2 fail, 3 succeed; find the callback line in the log |

The DAGs read `dags/lab_config.py` for every path and id. Nothing is hard-coded
in a DAG file — the same discipline as `config.py` in the other labs.

## Sensor DAG: unblocking it

The log of `wait_for_orders_file` prints the exact path it polls. On the host:

```bash
mkdir -p landing/2026-09-02            # use the ds from the log
printf 'order_id,amount\n1,10\n2,20\n' > landing/2026-09-02/orders.csv
```

## DAG 04 on EMR Serverless

1. Upload the scripts: `aws s3 cp ../iceberg_deep/ s3://<bucket>/labs/iceberg_deep/ --recursive --exclude "warehouse/*"`.
2. In `../iceberg_deep/config.py` set `MODE="emr"` (Glue catalog + S3FileIO) before uploading.
3. Fill `EMR_APPLICATION_ID`, `EMR_EXECUTION_ROLE_ARN`, `LAB_S3_BUCKET` in `.env`,
   set `LAB_MODE=emr`, and give the container AWS credentials
   (`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` in `.env`, or mount `~/.aws`).
4. `docker compose up -d` again. DAG 04's tag changes to `emr`; every Spark task
   is now an `EmrServerlessStartJobOperator` with `deferrable=True`.

On MWAA you would instead copy `dags/` to the environment's S3 `dags/` prefix,
put `apache-airflow-providers-amazon` in `requirements.txt`, and set `LAB_MODE`
as an environment variable or Airflow Variable. No image, no Java — which is
exactly why the Spark work goes to EMR.

## Troubleshooting

| symptom | look at |
|---------|---------|
| DAG not in the list | `docker compose logs airflow-dag-processor` — an import error |
| tasks stay *queued* | pool `spark_local` is full (1 slot) — intended for DAG 04; or the scheduler is unhealthy |
| DAG 04 task fails immediately | `docker compose exec airflow-scheduler java -version` — the image build must have installed the JRE |
| first DAG 04 run slow | Ivy is downloading the Iceberg runtime jar into the `ivy-cache` volume; once |
| `Permission denied` on `logs/` | `AIRFLOW_UID` in `.env` — 50000 is correct on Windows/macOS |
| UI shows a login page | `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS` was removed from the compose env |
