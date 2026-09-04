# Enterprise DAGs — what Airflow looks like at telco scale

Five production-shaped DAGs for a fictional multi-country mobile operator,
modelled on the scale a European group operator such as Yettel runs at:
roughly **10 million subscribers across four markets, ~40,000 radio cells, and
2-4 billion call detail records a day**.

These are **reference code, not a runnable lab**. Every ARN, bucket and
application id is a placeholder; running them needs a real AWS account with
EMR, Glue, Athena, Redshift, SageMaker, DMS and Step Functions. The runnable
lab is [`../airflow_local`](../airflow_local/). Read these the way you would
read a well-written codebase you just joined.

They are, however, **verified to parse**. See the bottom of this file.

## Read in this order

| # | DAG | schedule | shape | the lesson |
|---|-----|----------|-------|------------|
| 1 | [`01_cdr_mediation_hourly`](dags/01_cdr_mediation_hourly.py) | hourly | **transient EMR on EC2**: create → steps → sense → terminate | cluster lifecycle, spot instance fleets, and the `all_done` terminate that stops you leaking 60 nodes overnight |
| 2 | [`02_ran_kpi_micro_batch`](dags/02_ran_kpi_micro_batch.py) | every 15 min | **EMR Serverless**, deferrable | what a short interval forces on you, and self-healing compaction driven by Iceberg's own metadata |
| 3 | [`03_subscriber_360_churn_ml`](dags/03_subscriber_360_churn_ml.py) | **no cron** — Assets | EMR Serverless + SageMaker | data-aware scheduling on two upstream assets, and a drift gate that only retrains when the population actually moved |
| 4 | [`04_revenue_assurance_interconnect`](dags/04_revenue_assurance_interconnect.py) | daily + month-end | Glue, DMS, Athena, Step Functions | a money threshold that decides whether a dispute workflow opens, and the audit artefact that survives a regulator |
| 5 | [`05_gdpr_erasure_lakehouse_maintenance`](dags/05_gdpr_erasure_lakehouse_maintenance.py) | weekly | **EMR on EKS** + maintenance | why the ORDER of delete → rewrite → expire → orphan-removal is a legal requirement, not a preference |

All five share [`dags/telco_config.py`](dags/telco_config.py) — every ARN,
bucket, threshold and pool in one place, the same discipline as `config.py` in
the other labs.

## The two halves: `dags/` and `jobs/`

The DAGs decide **when, where and in what order**. They submit work; they never
touch a row. The actual data work lives in [`jobs/`](jobs/) — 15 PySpark jobs,
2 Glue scripts, a SageMaker processing entrypoint and an EMR bootstrap action,
one for every script the DAGs reference.

```
dags/    parsed by Airflow every ~30s.  Never runs Spark.
jobs/    uploaded to S3.  Only ever runs on EMR / Glue / SageMaker.
```

Spark code deliberately does **not** live under `dags/`: the dag-processor
imports every `.py` there on every parse, so a job placed in that folder gets
imported by the scheduler repeatedly and can even start a SparkSession inside
it. See [`jobs/README.md`](jobs/README.md) for the full DAG-task-to-script map
and what each script is teaching.

The techniques worth reading the jobs for:

| technique | script |
|---|---|
| deduplicating a replayed stream, then `MERGE` so reprocessing is safe | `cdr_mediation.py` |
| skew handled by **discovering** hot keys and salting only those | `interconnect_reconcile.py` |
| ratios summed as counters and divided last, never averaged | `ran_kpi_aggregate.py` |
| avoiding label leakage in a churn feature set | `subscriber_360_assemble.py` |
| copy-on-write vs merge-on-read, and why a `DELETE` may erase nothing | `gdpr_erase.py` |
| compaction that actually reconciles delete files | `iceberg_compaction.py` |
| failing on a bad **value**, not just on an exception | `churn_scores_publish.py` |
| one job, four incompatible partner file formats | `glue/interconnect_ingest.py` |

## What each technique looks like, and where

| technique | where to look |
|---|---|
| all three EMR deployment models | DAG 01 (EC2), 02/03/04 (Serverless), 05 (EKS) |
| dynamic task mapping over a **static** list | DAG 01 markets, DAG 02 regions, DAG 04 partners |
| dynamic task mapping over a **runtime** list | DAG 05 — the PII table inventory is read from the Glue catalog, so a new table is covered with no DAG edit |
| `deferrable=True` at scale | 46 of the 98 tasks; DAG 04 alone holds 16 |
| branching on **data** | DAG 01 duplicate rate, DAG 02 anomaly count, DAG 03 drift PSI, DAG 04 leakage EUR |
| branching on the **calendar** | DAG 04 month-end, using the run's date and never `today()` |
| Assets instead of cron offsets | DAG 03 is scheduled on `[CDR_SILVER, BILLING_SILVER]` and has no cron at all |
| pools as the platform's defence | `emr_serverless`, `emr_ec2_transient`, `redshift_serving`, `athena_queries` |
| setup / teardown | DAG 05's maintenance lock, always released |
| hooks where no operator exists | DAG 01 and 02 (Athena results), 03 (S3 JSON), 05 (DynamoDB, Glue catalog) |
| audit artefacts | DAG 04 and 05 both write immutable JSON attestations to S3 |

## Five things these DAGs teach that a tutorial will not

1. **`EmrTerminateJobFlowOperator` needs `trigger_rule="all_done"`.** With the
   default, a failed step leaves the cluster running until somebody notices in
   the morning. It is the most expensive one-line mistake in the catalogue.

2. **A green `AthenaOperator` means the query ran, not that the answer was
   right.** A count of zero is a success. Every real check in these DAGs reads
   the result in a task and raises.

3. **`Variable.get()` at the top of a DAG file runs on every parse** — every
   ~30 seconds, for every DAG. `telco_config.py` reads environment variables
   instead and explains why in its docstring.

4. **XCom carries the number, never the data.** Every task here returns an id,
   a count or a path. The billions of rows never leave S3.

5. **Order can be a legal requirement.** In DAG 05, deleting rows from an
   Iceberg table does not destroy anything until the snapshots referencing the
   old files have expired and the files are gone. Ship that in the wrong order
   and you have told a regulator you erased something you did not.

## Verified

Parsed against the real Airflow 3.3.1 image from `../airflow_local` with
`apache-airflow-providers-amazon` 9.34.0 loaded, using `DagBag` plus
`dag.validate()` on each DAG:

The Spark jobs were checked the same way — every `F.*` name they use was
verified to exist in the installed PySpark 4.1.3, not assumed:

| check | result |
|---|---|
| 19 job files compile | clean |
| `install_asn1.sh` | `bash -n` clean |
| 39 distinct PySpark functions used | all exist in 4.1.3 |
| `job_common.py` reaches the cluster | `--py-files` in every submit path |

```
=== IMPORT ERRORS: 0
=== DAGS: 5
  telco_01_cdr_mediation_hourly          tasks= 24 mapped=2 deferrable=12 groups=1
  telco_02_ran_kpi_micro_batch           tasks= 10 mapped=1 deferrable= 2 groups=0
  telco_03_subscriber_360_churn          tasks= 19 mapped=0 deferrable=12 groups=2
  telco_04_revenue_assurance             tasks= 29 mapped=1 deferrable=16 groups=3
  telco_05_gdpr_erasure_and_maintenance  tasks= 16 mapped=1 deferrable= 4 groups=2
=== TOTAL TASKS: 98 ; all dag.validate() passed
```

To repeat it yourself, with the `airflow_local` stack up:

```bash
cd ../airflow_local
CID=$(docker compose ps -q airflow-scheduler)
docker cp ../airflow_enterprise/dags "$CID:/tmp/ent"
docker compose exec -T airflow-scheduler python -c "
import sys; sys.path.insert(0, '/tmp/ent')
from airflow.dag_processing.dagbag import DagBag
db = DagBag(dag_folder='/tmp/ent', safe_mode=False)
print('import errors:', len(db.import_errors))
for n, d in sorted(db.dags.items()): d.validate(); print(n, len(d.tasks))
"
```

Two Airflow 3 traps were found by doing exactly that, and both are documented
in the code:

- `airflow.sdk.Variable.get()` takes **`default=`**, not the Airflow 2
  `default_var=`. The old form raises `TypeError` at parse time and removes the
  whole DAG from the UI.
- A **manually triggered run has no logical date**, so `{{ ds }}` is undefined
  and a task declaring `ds` fails. `telco_config.ds_of()` and `telco_config.DS`
  fall back to `dag_run.run_after`.

## Where to go next

- The operator reference: [Airflow Integration Catalog](../../2_atlases/10_Airflow_Integration_Catalog.html)
  (source: [`4_reference/airflow_integration_catalog.md`](../../4_reference/airflow_integration_catalog.md))
- The concepts: [atlas 8](../../2_atlases/8_Airflow_Orchestration_Atlas.html)
- The daily vocabulary: [atlas 9](../../2_atlases/9_Airflow_The_30_Building_Blocks.html)
- The runnable lab: [`../airflow_local`](../airflow_local/)
