# Capstone — EMR × Iceberg × Redshift, 3 days

**Instructor notes. Do not distribute this file.**

Five project briefs, one per engineer. Each gets **only their own file**. All
five follow the same shape: three business questions → dataset and harness →
ten implementation steps with working code → acceptance criteria → evidence
pack → 45-minute technical review.

```
1_STUDENT_FinTech_CDC_Ledger.md          PaySim (Kaggle)
2_STUDENT_Ecommerce_Clickstream.md       eCommerce Events, Electronics/Cosmetics (Kaggle)
3_STUDENT_Healthcare_Telemetry.md        MIMIC-III Clinical Demo v1.4 (PhysioNet, open)
4_STUDENT_IoT_SmartGrid.md               Smart Meters in London (Kaggle)
5_STUDENT_SupplyChain_ColdChain.md       Logistics & Supply Chain Dataset (Kaggle)
```

The cohort has just finished three weeks on EMR and Apache Iceberg and has
ten years of engineering behind them. The briefs are written for that reader:
no hand-holding, real code, ambiguity left in on purpose.

---

## Why the five are not interchangeable

A generic medallion pipeline scores about a third on any of them. The value is
in each project's **technical spine** — a different Iceberg mechanism per
engineer — and in a **trap** that only surfaces when the pipeline actually runs.

| # | Domain | Technical spine | The trap they must hit and document |
|---|---|---|---|
| 1 | FinTech CDC | `MERGE INTO`, idempotent reruns, out-of-order events, two-version point-in-time balance | MERGE aborts on duplicate source keys; plain compaction reconciles zero delete files on a MoR table |
| 2 | Clickstream | Temporal range join, partition **evolution** `hours()`→`+bucket`, tags for audit | Naive range join degrades to nested-loop; a tag without `RETAIN` gets expired; on S3 Tables any tag disables snapshot management |
| 3 | Healthcare | Long→wide pivot, gap-bounded resample, **merge-on-read** measured against CoW, erasure attestation | Plain compaction leaves delete files untouched; time travel still returns erased patients until snapshots expire — and expiry can be silently disabled |
| 4 | IoT grid | Manufacturing the small-file problem, **compaction strategy** comparison, degradation cadence | `sort`/`z-order` strategies do nothing without a declared sort order |
| 5 | Supply chain | **Bucketing** on both sides for a shuffle-free join, plan reading | Mismatched bucket counts silently reintroduce the shuffle — no error, no warning |

Cross-copying fails on three axes at once: different dataset, different Iceberg
feature, different numeric thresholds. Every trap is a documented, reproducible
behaviour — not a trick.

---

## The harness pattern — every project starts by building its own input

None of the five Kaggle/PhysioNet files arrives in the shape a real platform
receives. Each brief therefore opens with a **harness** the engineer must write
and which is assessed as production code:

| # | What the raw file is | What the harness must produce |
|---|---|---|
| 1 | Flat transaction log | Debezium-envelope CDC, daily files, with 3% late / 1% duplicate-key / 0.5% delete / 2% out-of-order injected |
| 2 | Flat event log with price per event | 5-minute micro-batches **plus** a derived price-interval catalog with ≥200 products changing >15% |
| 3 | Long-format chart events | Erasure request log with 5 patients, one arriving after gold exists |
| 4 | 112 block files, half-hourly | One file per half-hour per block — ~75k files, ~670 commits — the small-file problem itself |
| 5 | ~30k flat rows, no keys | Three relational tables synthesised and scaled to ≥50M rows, with spikes, excursions, dropouts and orphans injected at known rates |

Project 5 is the most demanding harness. Because the engineer injects the
anomalies, they hold ground truth — the brief requires precision and recall for
both the spike filter and breach detection, which is only possible with a
harness.

**Skipping the harness forfeits the project.** Projects 2 and 4 say so
explicitly; the compaction and partition-evolution measurements are meaningless
without the before-state the harness creates.

---

## How this resists outsourced work

LLM use is stated as expected in every brief. The design assumes it and makes it
insufficient on its own.

**Evidence pack.** Each brief ends with 18–20 values obtainable only from the
engineer's own account: snapshot IDs, `committed_at` to the millisecond, Athena
query execution IDs, EMR application IDs, file counts before and after
compaction, shuffle bytes from the Spark UI. An LLM cannot produce an internally
consistent set, and two engineers cannot share one — the values are checked
against CloudTrail.

**Break log.** Three genuine failures with real error text, one of which must be
the project's designated trap. LLM output is happy-path. A submission with no
error text in it did not run.

**Technical review.** Twelve questions per project, answered live at the
keyboard, keyed to the engineer's own measurements — *"your first compaction
reconciled zero delete files; why, and what fixed it?"* An engineer who built it
answers in seconds.

---

## Shared environment

| Resource | Convention |
|---|---|
| Raw landing | `s3://<class-bucket>/raw/<engineer_id>/…` |
| Iceberg warehouse | Glue database per engineer: `<domain>_db` — either a general-purpose bucket or an S3 Tables bucket |
| Compute | EMR Serverless application per engineer, or a shared EMR-on-EC2 cluster with YARN queues |
| Catalog | AWS Glue Data Catalog |
| Redshift | Serverless workgroup; external schema per engineer via `CREATE EXTERNAL SCHEMA … FROM DATA CATALOG` |

**Decide up front whether the class is on general-purpose S3 or S3 Tables and
tell them.** Several traps (tags disabling maintenance, managed retention
defaults, `DROP TABLE` unsupported on EMR > 7.5) exist only on S3 Tables, and
every brief asks the engineer to state which storage they are on.

Minimum versions: **EMR 7.5+** (7.12+ for Iceberg V3), **Glue 5.0+**,
Spark 3.5+. Put a budget alarm on the account before day 1. Every brief tells
them to stop EMR at the end of each day; enforce it.

---

## Time budget

Each brief includes a suggested 3-day split. Roughly: day 1 = harness, bronze,
baseline measurement; day 2 = silver and the technical spine; day 3 = gold,
Redshift, evidence pack. The spine measurement — the thing that actually
differentiates the projects — lands on day 2 in every brief, so if someone is
behind on the morning of day 2, that is when to intervene.

---

## Reviewing

No marks-per-section rubric. Each brief has an **acceptance criteria** table
with pass conditions; use it as the checklist. Then the review.

The single most diagnostic question is the same in every project: **"show me
the snapshot before and after, and tell me what changed and why."** It cannot
be answered from a report, only from a cluster.

Second most diagnostic: the **honest-limitations paragraph** each brief requires
— fraud recall, elasticity confounders, outage-rule blind spots, ETA predictors
absent from the data. Every brief states that a confident answer with no stated
limitations is wrong, not incomplete. Hold that line; it is the difference
between an engineer and a tool operator.
