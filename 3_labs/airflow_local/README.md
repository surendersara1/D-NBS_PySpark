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

### Run everything at once

```bash
bash run_all.sh      # seeds the sensor's landing file, unpauses and triggers all DAGs, polls until they settle
```

## Verified on this machine (2026-09-03)

Windows 10, Docker Desktop 28.5.1, image `apache/airflow:3.3.1-python3.12`
plus OpenJDK 17 and PySpark 4.1.3. Every DAG ran to success through the real
scheduler (not `dags test`):

| DAG | result | what was checked |
|-----|--------|------------------|
| 01 | 2 manual + 1 scheduled run, all success | manual run logs `logical_date=None`, `ds=2026-09-03` from the fallback idiom |
| 02 | success | `small_load_path` skipped, `join` still ran (`none_failed_min_one_success`) |
| 03 | success | `file arrived: /opt/airflow/landing/2026-09-03/orders.csv`, `3 data rows` |
| 04 | 1 manual + 1 scheduled (02:00 UTC) run, both success | six Iceberg scripts, one at a time through the `spark_local` pool: 00_setup 42 s, 01 26 s, 02 39 s, 03 36 s, 05 88 s, 04 80 s — **about 5 min 15 s** end to end; tables `orders`, `orders_staging`, `t_cow`, `t_mor`, `t_maint`, `t_wap` under `work/warehouse/iceberg/deep_db/` |
| 05 | success | 5 mapped instances, fan-in summary |
| 06 | 8 runs, all success | catchup created a run per day from 2026-08-27; one file per interval in `work/backfill/` |
| 07a / 07b | 07a success; **07b ran twice**, `asset_triggered__…` and `manual__…` | Asset scheduling and the explicit `TriggerDagRunOperator` each fired once |
| 08 | success | `flaky` attempts 1 and 2 failed, attempt 3 succeeded; callback logged each failure |

Two things the first run surfaced that the docs gloss over — both now handled in
the DAGs and documented in atlases 8 and 9:

1. **A manual run in Airflow 3 has no logical date.** `{{ ds }}` is undefined
   (not None) and a `@task` declaring `ds` fails with "missing argument".
   `lab_config.DS` and `lab_config.run_date(dag_run)` fall back to
   `dag_run.run_after`. Scheduled runs never hit this, which is why it hides.
2. **Airflow 3 seeds no default connections.** `FileSensor` needs `fs_default`;
   `airflow-init` now creates it.

Setup note: the first image build failed with `unable to find user root` — a
Docker image-store layer corrupted by a pull interrupted when the host disk
filled, which survived `docker rmi` and re-pull. Pulling the `-python3.12` tag
(different layers) sidestepped it; that is why the Dockerfile pins that tag.

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
| `03_sensor_wait_for_file` | sensors, `mode="reschedule"`, timeout | trigger, watch it poke, then create `landing/<date>/orders.csv` (path is in the log) and watch it unblock |
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
| DAG 03 fails with `conn_id fs_default isn't defined` | Airflow 3 seeds no default connections; `airflow-init` creates it — re-run `docker compose up airflow-init` or `airflow connections add fs_default --conn-type fs --conn-extra '{"path": "/"}'` |
| a manual run fails on `{{ ds }}` / a task asking for `ds` | Airflow 3: manual runs have no logical date. Use `lab_config.DS` / `lab_config.run_date(dag_run)`, or trigger with `-l 2026-09-02` |
| UI shows a login page | `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS` was removed from the compose env |
