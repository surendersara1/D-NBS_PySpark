# The jobs the DAGs submit

Every script the five enterprise DAGs reference. **This is where the actual
data work lives.** The DAGs decide *when, where and in what order*; these files
decide *what happens to the rows*. Atlas 8 chapter 1 draws that boundary; this
folder is the other half of it.

## Why these are not in `dags/`

Airflow's dag-processor **imports every `.py` file under the dags folder**,
every ~30 seconds, looking for DAG objects. A PySpark job placed there would be
imported by the scheduler on every parse — wasting time at best, and at worst
constructing a SparkSession inside the dag-processor.

```
dags/    parsed by Airflow every 30s.  Never runs Spark.
jobs/    uploaded to S3.  Only ever runs on EMR / Glue / SageMaker.
```

```bash
aws s3 sync jobs/       s3://telco-prod-emr-code/jobs/       --exclude "__pycache__/*" --exclude "README.md"
aws s3 cp   jobs/bootstrap/install_asn1.sh s3://telco-prod-emr-code/bootstrap/
```

`job_common.py` is shipped to the cluster with `--py-files` on every submit
(see `telco_config.spark_submit`). Without that line every job dies on the
cluster with `ModuleNotFoundError` while working perfectly on a laptop — a
classic first-deployment failure.

## DAG task → script

| DAG | task | script | what it really does |
|---|---|---|---|
| 01 | `add_mediation_step` (mapped, per market) | [`cdr_mediation.py`](cdr_mediation.py) | dedup by `row_number()`, rate against a broadcast tariff, salt the hot keys, `MERGE` into silver |
| 01 | cluster bootstrap | [`bootstrap/install_asn1.sh`](bootstrap/install_asn1.sh) | installs the native ASN.1 decoder on every node — **the reason DAG 01 must use EMR on EC2** |
| 02 | `aggregate_cell_counters` (mapped, per region) | [`ran_kpi_aggregate.py`](ran_kpi_aggregate.py) | normalises four vendors' counter names, pivots, computes ratios **from summed counters** |
| 02 / 05 | `compact_partitions`, `rewrite_data_files` | [`iceberg_compaction.py`](iceberg_compaction.py) | `rewrite_data_files` **plus** `rewrite_position_delete_files` — the part everyone omits |
| 03 | `features_usage` | [`features_usage.py`](features_usage.py) | three time windows in one shuffle; trend ratios; zero vs null kept distinct |
| 03 | `features_billing` | [`features_billing.py`](features_billing.py) | bill shock against a trailing **median**, payment lateness |
| 03 | `features_topups` | [`features_topups.py`](features_topups.py) | inter-event gaps with `lag()`, overdue relative to the subscriber's own rhythm |
| 03 | `features_network_experience` | [`features_network_experience.py`](features_network_experience.py) | skewed subscriber×cell join, **usage-weighted** quality exposure, home-cell detection |
| 03 | `features_care` | [`features_care.py`](features_care.py) | escalation detection, fixed indicator columns so the model schema is stable |
| 03 | `features_device` | [`features_device.py`](features_device.py) | SCD2 read **as of the run date** — the line that keeps a backfill honest |
| 03 | `assemble_wide_table` | [`subscriber_360_assemble.py`](subscriber_360_assemble.py) | left-joins six domains onto the subscriber base, builds the label, writes the ML splits |
| 03 | `population_drift_check` | [`sagemaker/drift.py`](sagemaker/drift.py) | Population Stability Index with **baseline-frozen bin edges** |
| 03 | `scores_to_iceberg` | [`churn_scores_publish.py`](churn_scores_publish.py) | per-market deciles, and a **score-distribution gate** that blocks a broken model |
| 04 | `normalise_billing_events` (Glue) | [`glue/billing_normalise.py`](glue/billing_normalise.py) | collapses a DMS CDC stream, one `MERGE` for insert+update+delete |
| 04 | `ingest_partner_file` (Glue, mapped) | [`glue/interconnect_ingest.py`](glue/interconnect_ingest.py) | four file dialects behind one common schema |
| 04 | `reconcile_ledgers` | [`interconnect_reconcile.py`](interconnect_reconcile.py) | fuzzy call matching with **discovered** hot keys and selective salting |
| 04 | `settlement_statements` | [`settlement_statements.py`](settlement_statements.py) | monthly roll-up with a snapshot id and a SHA-256 checksum |
| 05 | `delete_rows` (mapped, per PII table) | [`gdpr_erase.py`](gdpr_erase.py) | `DELETE`, then reports whether the rows are *actually* gone or merely hidden |
| 05 | `expire_snapshots`, `remove_orphan_files`, `rewrite_manifests` | [`iceberg_maintenance.py`](iceberg_maintenance.py) | three Iceberg procedures, with a guard on the 24-hour orphan rule |

## What each script is teaching

Read them for the technique, not the business domain:

| technique | best example |
|---|---|
| deduplicating a replayed stream | `cdr_mediation.py` — `row_number()` over `(cdr_id, source_ts desc)` |
| making reprocessing safe | `cdr_mediation.py` — a trailing window plus `MERGE`, never `append` |
| handling skew explicitly | `interconnect_reconcile.py` — hot keys **discovered from the data**, salted, small side replicated |
| ratios that survive aggregation | `ran_kpi_aggregate.py` — sum numerator and denominator, divide last |
| many time windows, one shuffle | `features_usage.py` — `F.sum(F.when(days_ago < N, x))` |
| zero vs null | `features_usage.py` — a dormant subscriber is 0, a new one is NULL |
| avoiding label leakage | `subscriber_360_assemble.py` — forward-window label, `label_is_observable`, raw churn date dropped |
| reading an SCD2 correctly | `features_device.py` — a range predicate, not a `max()` |
| CoW vs MoR in practice | `gdpr_erase.py` — same `DELETE`, completely different physical result |
| compaction that reconciles deletes | `iceberg_compaction.py` — `delete-file-threshold` **and** `rewrite_position_delete_files` |
| an ordering that is a legal requirement | `iceberg_maintenance.py` — delete, rewrite, expire, remove orphans |
| reproducible financial output | `settlement_statements.py` — snapshot id plus checksum |
| failing on a *value*, not just an error | `churn_scores_publish.py` — mean and stddev gates before publishing |
| one job, many input dialects | `glue/interconnect_ingest.py` — variation isolated to one function |
| Glue vs EMR at the code level | `glue/billing_normalise.py` — `getResolvedOptions`, `GlueContext`, `Job.init/commit` |

## Verified

These are reference implementations: they need real AWS data to run, so they
are not executed here. What **was** checked, against the running Airflow 3.3.1
image with PySpark 4.1.3:

| check | result |
|---|---|
| every file compiles | 19 Python files, `compileall` clean |
| `install_asn1.sh` syntax | `bash -n` clean |
| every `F.*` and `Window.*` used | 39 distinct functions, **all exist** in PySpark 4.1.3 |
| the DAGs that call them still parse | 5 DAGs, 98 tasks, 0 import errors |
| `job_common.py` reaches the cluster | `--py-files` present in every submit path |

```bash
# re-run the API check yourself, with the airflow_local stack up
cd ../../airflow_local
CID=$(docker compose ps -q airflow-scheduler)
docker cp ../airflow_enterprise/jobs "$CID:/tmp/jobs"
docker compose exec -T airflow-scheduler python -c "
import re, glob
from pyspark.sql import functions as F
bad = [(f, m) for f in glob.glob('/tmp/jobs/**/*.py', recursive=True)
       for m in re.findall(r'\bF\.([a-zA-Z_]\w*)', open(f).read()) if m not in dir(F)]
print('unknown pyspark functions:', bad or 'none')
"
```

## Deliberately not included

The scripts read tables such as `telco_prod_silver.cdr_events` that do not
exist — there is no DDL here and no synthetic data generator. That is the
boundary of this lab: it shows **how a pipeline is written at scale**, not a
dataset you can run. For code you can actually execute, use
[`../../iceberg_deep`](../../iceberg_deep/) and
[`../../airflow_local`](../../airflow_local/), where every line is verified by
a passing assertion.
