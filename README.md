# Data Engineering Foundations — PySpark, EMR & Iceberg on AWS

Teaching material for the three-module course by **Surender Sara**, NorthBay Solutions.
Everything is ordered by the sequence it is taught in: read `1_decks` top to bottom,
run `3_labs` alongside module 3, and keep `2_atlases` open on the second screen.

```
1_decks/       the three lecture PDFs, in teaching order
2_atlases/     ten diagram atlases, numbered in learning order
3_labs/        runnable PySpark + Iceberg (103 assertions) + Airflow in Docker + enterprise DAGs
4_reference/   source material and deeper reading
```

---

## 1 · Decks

| # | File | Covers | Length |
|---|---|---|---|
| 1 | [1_PySpark_on_AWS_Lecture.pdf](1_decks/1_PySpark_on_AWS_Lecture.pdf) | **Zero to Architecture** — why distributed compute exists. HDFS, MapReduce, the shuffle, the S3 flip, Parquet and partitioning, one real job end to end, five ways your job dies. | ~50 slides |
| 2 | [2_Mindset_EMR_101.pdf](1_decks/2_Mindset_EMR_101.pdf) | **Mindset: EMR 101** — Glue vs EMR vs EMR Serverless with real money, three kinds of DataFrame, the 30 functions, *the 1 TB question*, Iceberg and the write, the four knobs, reviewing generated code. | 61 slides · 90 min |
| 3 | [3_PySpark_30_Functions_Handbook.pdf](1_decks/3_PySpark_30_Functions_Handbook.pdf) | **The 30 Functions** — eight families of PySpark functions, each with a runnable example and its real captured output. | handbook |

Module 1 answers *why*, module 2 answers *how*, module 3 is the daily vocabulary.
Decks 1 and 2 label themselves "Module 01/02" internally, matching these numbers.

## 2 · Visual atlases

Ten self-contained HTML pages — no build step, no server. Open the file directly.
**Numbered in learning order**: the engine, then the cluster it runs on, then how data
moves, then the table format, then the managed service, then the boundary between
storage and compute, then where to run the job in the first place — and finally the
orthestrator that ties every step together.

| # | Atlas | What it is |
|---|---|---|
| 1 | [How Spark Runs 1 TB on EMR](2_atlases/1_How_Spark_Runs_1TB_On_EMR.html) | Companion to **deck 2**. Eleven progressive diagrams: the AWS platform, EMR cluster anatomy, Py4J, then the full 1 TB story — waves → map-side combine → hash routing → the global answer → range sort → skew → the Iceberg commit → the four knobs. **Start here.** |
| 2 | [EMR Cluster & Spot Recovery](2_atlases/2_EMR_Cluster_And_Spot_Recovery.html) | **What runs where, and what happens when spot takes it back.** Processes per node role, the life of one task, the two spot-loss scenarios and their very different costs, how much work is actually redone (almost never "start again"), cache vs checkpoint, streaming restart, one cluster serving many jobs. |
| 3 | [Spark Partitioning Strategies](2_atlases/3_Spark_Partitioning_Strategies.html) | **Seven strategies drawn identically so they compare directly** — hash, range, round-robin, coalesce, broadcast, storage partitioning, bucketing. Opens by separating the three unrelated things Spark calls "partition"; closes with a symptom → move decision table. |
| 4 | [Iceberg Table Format Internals](2_atlases/4_Iceberg_Table_Format_Internals.html) | Sixteen deep diagrams of **Apache Iceberg with Spark**: metadata tree, write path, commit protocol, pruning cascade, time travel, hidden partitioning, schema evolution, copy-on-write vs merge-on-read, compaction, maintenance, catalogs, branching/WAP, migration, streaming, EMR wiring. Traced from the Definitive Guide. |
| 5 | [S3 Tables — Managed Iceberg](2_atlases/5_S3_Tables_Managed_Iceberg.html) | Architecture, the **three automatic maintenance jobs with their real defaults**, DDL that works in Athena vs Spark, metadata tables, documented limits, views. Every number read from AWS docs. |
| 6 | [Storage vs Compute Boundary](2_atlases/6_Storage_vs_Compute_Boundary.html) | Where each of the seven distribution mechanisms **actually executes**. Five live only in the engine; two are recorded in table metadata. Answers "can I set hash partitioning on an S3 Table?" |
| 7 | [AWS Compute & Trigger Selection](2_atlases/7_AWS_Compute_And_Trigger_Selection.html) | The 15-minute wall, a **verified timeout table** for Lambda/Glue/Batch/Fargate/EMR, five ways to run a script straight from S3 with exact CLI calls, and the trigger patterns — including the Lambda shim you can drop. |
| 8 | [Airflow Orchestration Atlas](2_atlases/8_Airflow_Orchestration_Atlas.html) | **What Apache Airflow does and does not do.** Five processes and a database, the life of one DAG run and why it starts after its interval, twenty-word vocabulary, the EMR → Iceberg → Athena/Redshift pipeline as a DAG, local vs MWAA, the honest comparison with dbt, EventBridge and Step Functions, and the Airflow 2 → 3 renames that break copied code. |
| 9 | [Airflow: The 30 Building Blocks](2_atlases/9_Airflow_The_30_Building_Blocks.html) | The **30-functions treatment for Airflow**: DAG parameters, TaskFlow, operators, XCom/Variables/Connections, sensors, the amazon-provider operators for EMR Serverless / EMR on EC2 / Glue / Athena / Redshift, branching, dynamic mapping, Assets, backfill, retries and pools. Each with the Airflow 3.3 import, a pasteable snippet and its gotcha. |
| 10 | [Airflow Integration Catalog](2_atlases/10_Airflow_Integration_Catalog.html) | **Everything Airflow can talk to.** The four kinds of integration, a decision table, the standard and common-sql operators, all three EMR models, and every AWS operator/sensor/transfer grouped by service — **268 classes read out of the installed provider**, not from documentation. Plus the ~90 non-AWS providers, MWAA differences, and an anti-pattern table. Flipped from [the markdown source](4_reference/airflow_integration_catalog.md). |

Atlases 8–9 are the companion to the `airflow_local` lab. Atlases **5–7** were built by validating every claim against AWS documentation through the
AWS docs MCP server. Each ends with a verification log marking claims
VERIFIED / CORRECTED / FALSE — nine widely-repeated statements did not survive that check.
All seven are theme-aware (light/dark) and print cleanly for handouts.

## 3 · Labs

Four suites. Run `pyspark30` alongside deck 3, then `iceberg_deep` alongside
deck 2's Iceberg sections and atlas 4, then `airflow_local` with atlases 8–9,
and read `airflow_enterprise` with atlas 10.

| Suite | Covers | Checks |
|---|---|---|
| [pyspark30/](3_labs/pyspark30/) | The 30 DataFrame functions — the daily vocabulary. Companion to deck 3. | 44 |
| [iceberg_deep/](3_labs/iceberg_deep/) | **Spark × Iceberg interactions** — MERGE, snapshots, time travel, rollback, schema and partition evolution, delete files, CoW vs MoR, compaction, expiry, orphans, write-audit-publish. | 59 |
| [airflow_local/](3_labs/airflow_local/) | **Apache Airflow 3.3 in Docker** — eight DAGs from anatomy to failure handling, ending with a DAG that orchestrates the six `iceberg_deep` scripts locally or on EMR Serverless by one flag. | 8 DAGs |
| [airflow_enterprise/](3_labs/airflow_enterprise/) | **Telco-scale reference DAGs plus the Spark jobs they submit** — CDR mediation on transient EMR, 15-minute RAN KPIs, asset-scheduled churn ML, revenue assurance, GDPR erasure. Not runnable without AWS; verified to parse. | 5 DAGs / 98 tasks + 19 jobs |

### pyspark30 — the 30 functions

Every output printed in the handbook was captured from a real run of these files.

```bash
cd 3_labs/pyspark30
pip install pyspark
python 00_setup.py      # raw -> bronze -> silver
python 01_shape.py      # then 02 … 08
./run_all.sh            # everything, stops at the first failed check
```

**44 assertions across 9 files.** `config.py` is the only file you edit:

| `MODE` | Storage | Needs |
|---|---|---|
| `local_parquet` | Parquet dirs under `./warehouse` | nothing |
| `local_iceberg` | **real Iceberg tables**, local Hadoop catalog | the Iceberg runtime JAR (Maven fetches it once) |
| `emr` | Glue Data Catalog + S3 | an AWS account; set `S3_BUCKET` |

Not one line of example code changes between them — submit the same files to
EMR Serverless, EMR on EC2, or a Glue 5.x job.

### Verified working combination

Both modes were run end to end on Windows: **44/44 checks pass in `local_parquet`
and 44/44 in `local_iceberg`**, plus MERGE, time travel, `.snapshots`/`.history`
and DELETE against the local Iceberg catalog.

- **PySpark 4.1.3** with `iceberg-spark-runtime-4.1_2.13:1.11.0` — this pair works.
  There is no Iceberg runtime for Spark 4.2 yet, and the 4.1 runtime fails on it
  with `IncompatibleClassChangeError`.
- **Windows:** run `call windows_env.bat` once per shell first. It sets `HADOOP_HOME`
  (winutils — without it every write dies with `NativeIO$Windows.access0`),
  the `JAVA_TOOL_OPTIONS` security-manager flag for Java 17+, and pins
  `PYSPARK_PYTHON` so workers don't start the wrong interpreter.
- Use `python -m pip install …`, not bare `pip`, if several Pythons are installed.
- **If you move or clone this repo, delete `warehouse/` and re-run `00_setup.py`.**
  Iceberg manifests record **absolute** file paths, so a warehouse built at an old
  path fails with `NotFoundException: Failed to open input stream for file: …`.
  That is the format behaving correctly, and it is a good live demo of why the
  metadata tree points at real locations rather than relative directories.

### iceberg_deep — Spark × Iceberg internals

```bash
cd 3_labs/iceberg_deep
call windows_env.bat        # Windows, once per shell
./run_all.sh                # 59 checks, six files, in order
```

Six labs that make the Iceberg atlas concrete — see
[its README](3_labs/iceberg_deep/README.md) for the file-by-file map and for
**five behaviours these labs surfaced that the documentation glosses over**,
including the big one: a plain `rewrite_data_files` does **not** reconcile
merge-on-read delete files, so a naive compaction schedule silently lets read
cost grow forever.

### airflow_local — orchestrating the pipeline

```bash
cd 3_labs/airflow_local
docker compose up -d --build     # Airflow 3.3.1 + Postgres; the image adds a JRE and PySpark
# open http://localhost:8080, unpause 01_hello_dag, trigger it
```

Docker Desktop is the only prerequisite, and the drive holding its disk image
needs ~6 GB free. Eight numbered DAGs, each a lesson; DAG 04 runs the
`iceberg_deep` scripts as bronze → silver → gold, and `LAB_MODE=emr` in `.env`
turns every Spark task into an `EmrServerlessStartJobOperator` without touching
the DAG. **Verified end to end on 2026-09-03:** all eight DAGs green through the
real scheduler, DAG 04 running the six Iceberg scripts in about 5 minutes.
See [its README](3_labs/airflow_local/README.md) for the results table and two
Airflow 3 behaviours the run surfaced.

### airflow_enterprise — what it looks like at scale

Five production-shaped DAGs for a fictional multi-country mobile operator
(~10M subscribers, ~40k cells, billions of CDRs a day). **Reference code, not a
runnable lab** — every ARN is a placeholder — but verified to parse against the
real Airflow 3.3.1 image: **5 DAGs, 98 tasks, 0 import errors,
`dag.validate()` clean**.

Covers all three EMR deployment models, mapping over a table inventory read at
run time, Assets instead of cron offsets, money- and drift-threshold branches,
setup/teardown locks, and audit attestations.

`dags/` holds the orchestration; [`jobs/`](3_labs/airflow_enterprise/jobs/)
holds the 15 PySpark jobs, 2 Glue scripts, SageMaker drift entrypoint and EMR
bootstrap action they submit — dedup and MERGE, discovered-hot-key salting,
label-leakage avoidance, CoW vs MoR erasure, and compaction that reconciles
delete files. Every PySpark function used was verified against the installed
API. See
[its README](3_labs/airflow_enterprise/README.md) for the five things these
teach that a tutorial will not.

## 4 · Reference

| File | Notes |
|---|---|
| `apache-iceberg-TDG_ER1.PDF` | *Apache Iceberg: The Definitive Guide* (Aakulov, Merced & Gidon, O'Reilly). Source for the Iceberg atlas. **Not in the repo** — gitignored as third-party material. Bring your own copy to `4_reference/`. |
| [s3_tables_iceberg_metadata_guide.md](4_reference/s3_tables_iceberg_metadata_guide.md) | S3 Tables / Iceberg metadata notes. |
| [airflow_integration_catalog.md](4_reference/airflow_integration_catalog.md) | Markdown source for atlas 10. Regenerate the HTML after editing it. |

---

## The dataset used throughout

Two dirty source files land in `raw/`, then flow to bronze and silver:
**orders** (13 records, 12 distinct — JSON with an array and a struct) and
**order_items** (18 rows, CSV). Twelve defects are planted deliberately so that
specific functions have something honest to demonstrate — `region` spelled seven
ways, padded and mixed-case emails, a null channel, an order that arrives twice
with a corrected status, an order with zero line items, an orphan line item, a
negative quantity, timestamps stored as strings.

Set `SCALE_ROWS = 2_000_000` in `config.py` to append synthetic rows (skewed ~60%
to one region) when you want to feel a real shuffle and see the skew from
deck 2's section 07 on your own machine.
