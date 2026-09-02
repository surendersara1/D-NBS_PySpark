# CAPSTONE — Student 3
## ICU telemetry, merge-on-read, and provable patient erasure

**Duration:** 3 days · **Stack:** EMR (PySpark) → Apache Iceberg (3 layers) → Redshift
**Total marks:** 100

---

## 0 · The situation

You are building the clinical analytics platform for a hospital group. Bedside
monitors stream vital signs into S3. Two very different people depend on you, and
their needs pull in opposite directions.

**Dr Aduba, ICU Clinical Lead**, wants early-warning scores. She wants to know
which patients deteriorated in the six hours before a critical event, so the
ward can intervene sooner. She needs the data complete and the timeline honest.

**Rita, Data Protection Officer**, has a legal obligation. When a patient
exercises their right to erasure, the hospital has **30 days** to remove their
identifiable data — and must be able to **demonstrate** it is gone. Not
"filtered out of the dashboard". Gone. She will audit you.

These requirements conflict. Erasure destroys history; clinical analysis depends
on it. **Resolving that conflict, technically and defensibly, is this exam.**

---

## 1 · Dataset

**MIMIC-III Demo** (PhysioNet, open-access demo subset) — or an **ICU Vitals
Time-Series** dataset from Kaggle. State which you used.

Core fields: `patient_id` (or `subject_id`), `charttime`, `heart_rate`, `spo2`,
`systolic_bp`, `diastolic_bp`, `temperature`, `respiratory_rate`.

> **If you use MIMIC-III:** the demo subset is open-access, but you must still
> read and follow its data-use terms. State in your report that you did. Do not
> use the full credentialed dataset for this exam.

### Known problems — handle all of them

1. **Irregular timestamps.** Readings are not on a fixed cadence. Gaps range from
   seconds to hours. Never assume a regular interval.
2. **Physiologically impossible values.** Heart rate of 0 or 300, SpO2 above 100,
   temperature of 0. These are sensor artefacts, not patients. Define your
   plausible ranges, cite a source or state your reasoning, and handle them —
   but **do not silently delete them**; a disconnected lead is clinically
   meaningful information.
3. **Missing vitals.** Not every reading has every measure.
4. **Duplicate readings** at the same `charttime` for the same patient.
5. Units may be inconsistent (Fahrenheit vs Celsius). Check before you average.

### Synthetic erasure requests — you must create these

Real erasure requests do not come with the dataset. Generate a request log:

- pick **5 patients** who have substantial data
- assign each a `request_ts` spread across your project timeline
- store as `raw/<your_id>/erasure_requests/`

At least one request must arrive **after** you have already built gold tables
from that patient's data. That is the hard case and it is deliberate.

---

## 2 · What you are building

```
raw/            vitals as landed + the erasure request log
  |
bronze.vitals_raw       every reading, as received
  |
silver.vitals           cleaned, deduplicated, plausibility-flagged
silver.patient_dim      patient attributes, with PHI handled
  |
gold.deterioration      early-warning features per patient per hour
gold.ward_summary       de-identified, aggregate only
  |
Redshift                Dr Aduba's clinical marts (de-identified)
```

---

# DAY 1 — Bronze, and choosing your write mode deliberately

**Goal: land the telemetry, and make a row-level-update decision you can defend.**

### 1.1 Bronze table
Create `bronze.vitals_raw` partitioned on `days(charttime)`.

Then answer, before writing any more code: **is this table copy-on-write or
merge-on-read, and why?** Set the three relevant table properties explicitly
rather than relying on defaults, and state your reasoning in the report.

Hint at the tension: bronze is append-only, so the write mode barely matters
here. It will matter enormously for silver. Say so, and say why.

### 1.2 The deletion-mode decision — the marked task
`silver.vitals` will need row-level deletes, potentially thousands at a time,
for erasure. You must choose between:

- **copy-on-write** — the whole data file is rewritten without the deleted rows
- **merge-on-read** — a small delete file records what to skip, reconciled at
  read time

Write a **one-page decision memo** covering: the read cost, the write cost, what
each means for a 30-day erasure SLA, and what each means for Dr Aduba's query
latency. Then choose, set the properties, and live with it.

There is no single right answer. There is a wrong answer, which is choosing
without measuring — so day 2 requires you to measure both.

### 1.3 Profile the dirt
Report, over the full dataset:

| Check | Count | % |
|---|---|---|
| Readings with HR outside your plausible range | | |
| SpO2 > 100 | | |
| Duplicate (patient_id, charttime) pairs | | |
| Readings missing at least one vital | | |
| Median gap between consecutive readings per patient | | |
| 95th percentile gap | | |

That last pair matters for day 2. A median gap of minutes and a p95 of hours
means a naive resample will invent data.

### Day 1 deliverables
- bronze DDL with write-mode properties set explicitly
- the one-page decision memo
- the data-quality table above
- your documented plausible ranges, with reasoning

---

# DAY 2 — Silver, and measuring what your choice cost

**Goal: clean, gap-aware time series — and hard numbers on CoW vs MoR.**

### 2.1 `silver.vitals`
Clean, deduplicate, flag. Requirements:

- one row per `(patient_id, charttime)` — deduplicate deterministically and say
  how you chose the winner
- a `plausible` boolean per vital, not a deletion
- units normalised, with the conversion documented
- gaps **preserved**, not filled. Add a `gap_minutes_since_prev` column.

### 2.2 Do not invent data
Dr Aduba needs hourly features. Your readings are irregular. You must decide
between last-observation-carried-forward, interpolation, or leaving gaps null.

**The rule: never carry an observation forward across a gap longer than your
p95.** State your threshold, implement it, and report how many hourly buckets
end up genuinely null as a result. A pipeline that reports a heart rate for an
hour where the monitor was disconnected is clinically dangerous. Say that in
your report.

### 2.3 The measurement — CoW vs MoR side by side
Build **two copies** of a representative subset of `silver.vitals`: one
copy-on-write, one merge-on-read. Identical data, identical partitioning.

On each, run the same **10 single-patient deletes**, one at a time. Record:

| | CoW | MoR |
|---|---|---|
| Data files before | | |
| Data files after | | |
| Position delete files after | | |
| Equality delete files after | | |
| Wall-clock time for the 10 deletes | | |
| `SELECT count(*)` runtime after | | |
| Bytes scanned on a full-table read after | | |

Use the `files` metadata table and its `content` column to separate data files
from delete files. State what `content` values you saw and what each means.

Then run compaction on the MoR copy and re-measure the delete-file count.

> **You will probably find that a plain compaction changes nothing.** That is a
> real, documented behaviour — a data file is not automatically eligible for
> rewriting just because delete files point at it. Investigate, find the option
> that makes it eligible, and report both the failed attempt and the working one.
> **Both are marked.** This is the single most valuable finding in this exam.

### 2.4 PHI handling in `silver.patient_dim`
Separate identifying attributes from clinical ones. Implement at least one of:
column-level masking, a separate restricted table, or tokenised patient IDs with
the mapping held apart. Justify the design against Rita's requirement.

### Day 2 deliverables
- silver DDL and cleaning job
- the gap-handling rule + the count of genuinely-null hourly buckets
- **the CoW vs MoR measurement table, fully populated**
- the compaction finding: what did not work, what did, and why
- the PHI design with justification

---

# DAY 3 — Gold, erasure, and Redshift

**Goal: clinical value, and proof to Rita that erasure is real.**

### 3.1 `gold.deterioration`
Per patient per hour: mean/min/max of each vital, a deterioration flag of your
own design, and the six-hour trend leading into it.

Define your early-warning rule explicitly. A simple threshold-based score is
fine — an undocumented one is not. State what it would miss.

### 3.2 `gold.ward_summary`
De-identified aggregates only. No patient can be identifiable. State your
minimum group size and what you do with groups below it.

### 3.3 Execute erasure — the audit trail
Process your 5 erasure requests. For **each one**, produce evidence:

1. Row count for that patient **before** erasure, per table
2. The delete statement, per table
3. Row count **after** — must be zero in every table
4. The snapshot ID before and after
5. **The hard part:** show whether the patient's data is still reachable via
   time travel to a pre-erasure snapshot — and state what you did about it

Point 5 is the crux. Iceberg keeps history; that is its purpose. Rita does not
accept "it is in an old snapshot but nobody looks". Explain precisely what makes
the data actually unrecoverable, which settings control the timing, and how long
it takes after your delete.

> **Warning, and it is worth marks:** on a managed Iceberg table, certain
> table-level settings and certain table state can **silently disable automatic
> snapshot expiry entirely** — meaning your deleted data never ages out. Find out
> what conditions cause that, check whether your table is in one of them, and
> report the check you ran. A student who claims erasure works without verifying
> this has not finished the task.

### 3.4 Redshift
Expose **de-identified** gold to Redshift. Build:

- `mart_ward_deterioration` — deterioration rates by ward and shift
- `mart_vitals_hourly` — aggregate hourly vitals, no patient identifiers

State explicitly how you prevent identifiable data reaching Redshift, and how an
erasure request propagates to these marts. If a patient is erased on Monday, when
does the Redshift mart reflect it? Give a number.

---

## 4 · Deliverables

```
/pipeline/        bronze, silver, gold PySpark jobs
/erasure/         the erasure runner + the audit trail
/sql/             Athena / Redshift SQL
/evidence/        the ledger below, plus pasted outputs
/REPORT.md        8-12 pages, including the CoW/MoR memo and the erasure attestation
```

The **erasure attestation** is a one-page document Rita could actually file:
what was deleted, when, from where, verified how, and when it became
unrecoverable.

---

## 5 · Evidence Ledger — mandatory

| # | Item | Value |
|---|---|---|
| 1 | EMR Serverless application ID | |
| 2 | Dataset used + confirmation you read its terms | |
| 3 | Your plausible HR / SpO2 / temp ranges | |
| 4 | Median and p95 gap between readings | |
| 5 | Hourly buckets left genuinely null by your gap rule | |
| 6 | CoW copy: data files before / after 10 deletes | |
| 7 | MoR copy: data files before / after 10 deletes | |
| 8 | MoR delete-file count after 10 deletes | |
| 9 | `content` values observed in the `files` metadata table | |
| 10 | Delete-file count after the **plain** compaction attempt | |
| 11 | Delete-file count after the **working** compaction | |
| 12 | The option that made compaction effective | |
| 13 | Erasure: patient IDs, before/after row counts (5 rows) | |
| 14 | Snapshot IDs before and after each erasure | |
| 15 | Result of your snapshot-expiry health check | |
| 16 | Time from delete to genuine unrecoverability, with the settings that determine it | |
| 17 | Athena query execution ID for the erasure verification | |

### Break Log — mandatory
Three genuine failures with real error text. At least one must be the
compaction-does-not-reconcile-deletes finding.

---

## 6 · Defence (15 minutes, live)

1. Show me the `files` metadata table for your MoR copy. Explain every distinct
   `content` value on screen.
2. You chose CoW or MoR for silver. Show me the numbers that justified it.
3. Your first compaction did nothing. Why not? What actually made it work?
4. Patient P was erased. Prove it. Now time-travel to before the erasure — what
   do you see, and why is that acceptable or not?
5. What exactly makes the erased data unrecoverable, and how long does it take?
6. You ran a check for whether automatic snapshot expiry was disabled. What was
   the check, and what did it return?
7. Your p95 gap is N minutes. Show me an hourly bucket you left null and explain
   why filling it would have been wrong.
8. A heart rate of 0 — sensor fault or cardiac arrest? What does your pipeline do,
   and is that the right call?
9. Rita asks: if a patient is erased today, when does the Redshift mart reflect it?
10. Dr Aduba wants six months of history. Rita wants 30-day erasure. Where exactly
    do those conflict in your design, and how did you resolve it?

---

## 7 · Rubric

| Area | Marks |
|---|---|
| Bronze design + explicit write-mode properties | 7 |
| CoW vs MoR decision memo, before measuring | 8 |
| Data-quality profiling incl. gap distribution | 8 |
| Gap handling — no invented data, threshold defended | 12 |
| **CoW vs MoR measurement table, fully populated** | 16 |
| **The compaction finding: failed attempt + working fix** | 12 |
| PHI design in silver | 7 |
| Deterioration feature + stated limitations | 8 |
| **Erasure audit trail, all 5 patients, all 5 evidence points** | 14 |
| Snapshot-expiry health check performed and reported | 6 |
| Redshift de-identified marts + propagation answer | 7 |
| Evidence Ledger complete and internally consistent | 5 |
| Break Log — three real failures | 5 |
| **Defence** | 20 |
| | **/135, scaled to 100** |

---

## 8 · Rules

- **Using an LLM is allowed and expected.** You are marked on judgement,
  evidence and defence — not on typing.
- Use only the open-access demo dataset. Follow its data-use terms and say so.
- Every number must be reproducible from your own account.
- Do not share tables, snapshots or query IDs with another student.
- The CoW/MoR measurement must be your own. Two students producing the same file
  counts on different data is not possible.
- Stop your EMR Serverless application at the end of each day.
- Ambiguity is deliberate. Decide, document the assumption, defend it.
