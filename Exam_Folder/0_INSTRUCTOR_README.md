# Capstone Exam — EMR × Iceberg × Redshift (3 days)

**Instructor notes. Do not distribute this file to students.**

Five briefs, one per student. Each student gets **only their own file**.

```
1_STUDENT_FinTech_CDC_Ledger.md          Student 1
2_STUDENT_Ecommerce_Clickstream.md       Student 2
3_STUDENT_Healthcare_Telemetry.md        Student 3
4_STUDENT_IoT_SmartGrid.md               Student 4
5_STUDENT_SupplyChain_ColdChain.md       Student 5
```

---

## Why these five are not interchangeable

An LLM will happily write a medallion pipeline for any of them. The briefs are
built so that a *generic* medallion answer scores about 35%. The marks live in
the **specialised technical spine** that is different for every student, and in
**evidence that only exists if the pipeline actually ran**.

| # | Domain | Technical spine — the thing being examined | The trap they must hit |
|---|---|---|---|
| 1 | FinTech CDC | `MERGE INTO`, idempotent reruns, out-of-order CDC, point-in-time balance via time travel | Duplicate keys on the source side of a MERGE; a rerun that double-applies |
| 2 | Clickstream | Hidden partitioning, partition **evolution** `day()`→`hour()` mid-project, sessionisation windows | Skew on one `category_code`; two partition specs in one table |
| 3 | Healthcare | **Merge-on-read**, deletion vectors, GDPR/HIPAA erasure, irregular timestamps | Branches silently disable snapshot expiry; V3 removes the table from Athena |
| 4 | IoT smart grid | The small-file problem, compaction strategy selection, `$files`/`$manifests` monitoring | A plain compaction reconciles no delete files; `sort` needs a declared sort order |
| 5 | Supply chain | **Bucketing** for zero-shuffle joins, broadcast vs sort-merge, join-plan reading | Mismatched bucket counts silently reintroduce the shuffle |

Cross-copying between students fails on all three axes: different dataset,
different Iceberg feature, different numeric SLA thresholds.

---

## The anti-plagiarism mechanism

Nothing here relies on trusting the student. Three layers:

**1 · The Evidence Ledger.** Every brief requires a table of values that can only
come from their own AWS account: snapshot IDs, EMR application IDs, Athena query
execution IDs, `committed_at` timestamps, file counts before and after
compaction, bytes scanned. These are 64-bit values and wall-clock times. An LLM
cannot invent a consistent set, and two students cannot share one — the
timestamps and IDs will not line up with their own CloudTrail.

**2 · The Break Log.** Each brief *requires* the student to break something on
purpose, capture the real error text, and fix it. LLM output is happy-path.
A submission with no error text in it did not run.

**3 · The Defence.** Ten questions per student, answerable only from their own
numbers. Marked live, 15 minutes. A student who outsourced the build cannot
explain why *their* file count went from 1,847 to 12.

State this to the class up front: **using an LLM is expected and allowed.**
Marks come from judgement, evidence and defence, not from typing.

---

## Shared environment

Everyone uses the same account and Region. Give each student their own prefix.

| Resource | Convention |
|---|---|
| S3 raw landing | `s3://<class-bucket>/raw/<student_id>/` |
| Iceberg warehouse | S3 Tables bucket, one namespace per student: `<student_id>_db` |
| EMR | EMR Serverless application per student, or one shared cluster with YARN queues |
| Redshift | Serverless workgroup, one schema per student |
| Catalog | Glue Data Catalog (`s3tablescatalog/<bucket>`) |

Minimum versions — these are hard requirements, not suggestions:
**EMR 7.5+** for S3 Tables (**7.12+** if the student needs Iceberg V3,
**8.0+** for the `variant` type), **Glue 5.0+**, PySpark 3.5+.

Budget guard: tell them to use `--conf spark.sql.shuffle.partitions` sensibly and
to stop EMR Serverless applications at the end of each day. Put a budget alarm on
the account before day 1.

---

## Marking

Each brief carries its own rubric totalling 100. Suggested weighting across the
cohort: Day 1 = 25, Day 2 = 30, Day 3 = 25, Defence = 20.

The single most diagnostic question in every defence is the same in spirit:
*"Show me the snapshot before and after, and tell me what changed and why."*
A student who built it answers in ten seconds.
