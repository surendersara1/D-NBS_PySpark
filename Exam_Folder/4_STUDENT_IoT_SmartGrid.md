# CAPSTONE — Student 4
## Smart-meter ingestion, the small-file problem, and an outage engine

**Duration:** 3 days · **Stack:** EMR (PySpark) → Apache Iceberg (3 layers) → Redshift
**Total marks:** 100

---

## 0 · The situation

You run data engineering for a regional electricity distributor. Half a million
smart meters report consumption every 15 minutes. The volume is not the problem —
the **shape** is. Readings arrive continuously in small batches, and the previous
pipeline wrote every batch straight through.

The table now has **hundreds of thousands of tiny files**. Query planning takes
longer than reading the data. The nightly report that used to take four minutes
takes fifty-one. Storage costs are climbing for reasons nobody can explain.

**Ade, Head of Network Operations**, needs outage detection: which meters went
quiet, when, and which substation they cluster around. Every minute of detection
delay is a minute of customers sitting in the dark before anyone knows.

**Fola, the CFO**, has seen the S3 bill and wants an explanation and a plan.

Your job is to build the pipeline properly and, crucially, to **measure** the
difference so Fola gets a real answer rather than an assurance.

---

## 1 · Dataset

**Smart Meter Electricity Consumption** (Kaggle). Any variant that provides
sub-hourly readings with weather is acceptable; state which you used.

Fields: meter or household identifier, timestamp, consumption in kWh, and
external metrics — temperature, humidity — plus anomaly flags where present.

### Known problems — handle all of them

1. **Missing intervals.** Meters go offline. A missing reading is not a zero —
   conflating those two ruins both the billing view and the outage engine.
2. **Negative consumption**, where solar export exists. Do not clip it blindly;
   decide and defend.
3. **Duplicate readings** for the same meter and timestamp.
4. **Clock drift** — timestamps not exactly on the 15-minute boundary.
5. **Weather is at a coarser grain** than consumption. Joining them is a decision,
   not a given.

### You must manufacture the ingestion pattern

The raw file is one large export. Your problem is the opposite. Write a
**micro-batch simulator** that replays the data as it would actually arrive:

- one small file per **15-minute interval**, per region
- at least **7 simulated days**
- written to `raw/<your_id>/meter_readings/`

This gives you the several-thousand-tiny-files starting condition the exam needs.
**Do not skip this and load the file in one shot** — the entire day-2 measurement
depends on you having created the problem first.

---

## 2 · What you are building

```
raw/            micro-batch files, as they arrive
  |
bronze.readings         every reading, as received, append-only
  |
silver.readings         cleaned, deduplicated, gap-aware
silver.meter_dim        meter to substation to region mapping
  |
gold.consumption_hourly aggregated load per meter and per substation
gold.outage_events      detected outages with duration and blast radius
  |
Redshift                Ade's operations marts, Fola's cost view
```

---

# DAY 1 — Bronze, and manufacturing the problem

**Goal: land the micro-batches and document exactly how bad the file layout is.**

### 1.1 Bronze table
Create `bronze.readings` as an Iceberg table over the micro-batch feed.
Partition on `days(reading_ts)`.

Load all 7 simulated days **through the micro-batch path** — one commit per
batch, as a streaming pipeline would.

### 1.2 The baseline — measure the damage
This is the most important measurement in the exam. Record all of it:

| Metric | Value |
|---|---|
| Total data files | |
| Total rows | |
| Average file size (bytes) | |
| Median file size | |
| Smallest / largest file | |
| Files in the largest partition | |
| Total snapshots | |
| Number of manifest files | |
| Total metadata size vs total data size | |

Get these from the `files`, `manifests` and `snapshots` metadata tables.
Paste the queries you used.

### 1.3 Quantify the query cost
Run a representative query — one day of consumption for one substation — and
record **runtime and bytes scanned**. This is your "before" number and Fola will
see it.

Then explain, in your own words, **why many small files is slow.** Name the
per-file costs. "It is slow" is not an answer; there are at least three distinct
costs and you should name them.

### Day 1 deliverables
- the micro-batch simulator
- bronze DDL and loader
- the full baseline measurement table
- the before-query runtime and bytes scanned
- your explanation of the per-file cost

---

# DAY 2 — Silver, and fixing the layout with evidence

**Goal: clean the data, then compact it and prove the improvement.**

### 2.1 `silver.readings`
Clean and type. Requirements:

- deduplicate on `(meter_id, reading_ts)` deterministically; state the rule
- snap drifted timestamps to the 15-minute grid, and record the original
- distinguish **missing** from **zero** explicitly — a nullable reading plus a
  status column, not a magic number
- decide on negative consumption and defend it
- join weather at the grain you justified

### 2.2 The compaction experiment — the marked task
Do not just compact once. **Compare strategies with measurements.**

Run at least two of the available compaction strategies on comparable copies of
your data — `binpack` versus a sort-based strategy — and record for each:

| | Before | binpack | sort-based |
|---|---|---|---|
| Data files | | | |
| Average file size | | | |
| Compaction wall-clock time | | | |
| Query runtime (the day-1 query) | | | |
| Bytes scanned (same query) | | | |

> **The sort-based strategies have a prerequisite.** They will not do what you
> expect unless something is declared on the table first. Find out what, set it,
> and report what happened on your first attempt before you did. **The failed
> attempt is worth marks** — it is the difference between reading the manual and
> reciting it.

Also report: what target file size did you use, what is the default, and what is
the permitted range? Justify your choice against your query pattern.

### 2.3 Automated maintenance vs doing it yourself
Your platform can run compaction, snapshot expiry and orphan-file removal
automatically. Investigate and report:

- which maintenance operations are automatic, and which are configured **per
  table** versus **per bucket**
- their **default values** — target file size, snapshot retention, orphan
  thresholds — with the source you read them from
- what the defaults mean for **how far back you can time travel**
- what they mean for **when storage is actually released** after data is deleted

Then write Fola's answer: a short section explaining where the storage cost came
from and what the retention settings cost per month in principle. You do not need
exact dollars; you need the correct **mechanism** and the right levers.

### 2.4 Manifest health
Compaction fixes data files. Metadata can still be fragmented. Query the
`manifests` metadata table and report the count and size distribution before and
after. If manifest rewriting is available to you, run it and measure. If it is
not, say so and explain what would need to change.

### Day 2 deliverables
- silver DDL and cleaning job with the missing-vs-zero design
- **the compaction comparison table, fully populated**
- the failed sort attempt and what fixed it
- the maintenance defaults, with source
- manifest measurements before and after
- Fola's cost explanation

---

# DAY 3 — Gold, the outage engine, and Redshift

**Goal: detect outages, and prove the pipeline holds under continued ingestion.**

### 3.1 `gold.consumption_hourly`
Hourly consumption per meter and per substation, with a data-completeness
percentage per bucket. Ade must be able to tell a low-consumption hour from an
incomplete one.

### 3.2 `gold.outage_events` — the analytical core
Define an outage. A reasonable starting rule:

> A meter is **out** when it reports no reading for **3 consecutive intervals**
> (45 minutes) having previously been reporting.

Produce outage events with: meter, start, end, duration, substation, and the
number of other meters out at the same substation in an overlapping window.

Then go further, because Ade's real question is scale:

- classify each event as **isolated** (a single meter — likely a meter fault) or
  **clustered** (multiple meters at one substation — likely a network fault)
- state your clustering threshold and defend it
- report the distribution: how many isolated, how many clustered, largest cluster

Now the honest paragraph: **what does your rule get wrong?** Name at least two
failure modes. A meter that was already offline before your window starts. A
scheduled maintenance outage indistinguishable from a fault. Communication
failure versus power failure — your data cannot tell them apart, and you should
say so.

### 3.3 Continued ingestion — does it stay fixed?
Simulate **one more day** of micro-batches into the compacted table. Then
re-measure:

- data files immediately after the new day
- data files after the automatic or manual maintenance has run

Report whether the table degrades again and how quickly. State what compaction
cadence you would schedule in production, and justify it against your measured
degradation rate rather than a round number.

### 3.4 Redshift
Expose gold to Redshift. Build:

- `mart_outage_ops` — Ade's live view: open outages, duration, substation,
  meters affected
- `mart_load_profile` — consumption by substation and hour, with weather

Document the access path and why. Then measure: **the same outage query from
Redshift versus straight from Iceberg** — runtime and data scanned. Ade needs
this to refresh every 60 seconds. State which path can support that and why.

---

## 4 · Deliverables

```
/simulator/       the micro-batch generator
/pipeline/        bronze, silver, gold PySpark jobs
/maintenance/     compaction and maintenance scripts and configs
/sql/             Athena / Redshift SQL
/evidence/        the ledger below, plus pasted outputs
/REPORT.md        8-12 pages, including Fola's cost section
```

---

## 5 · Evidence Ledger — mandatory

| # | Item | Value |
|---|---|---|
| 1 | EMR Serverless application ID | |
| 2 | Dataset variant used | |
| 3 | Micro-batches written (count) and simulated days | |
| 4 | **Baseline**: data files, avg size, median size | |
| 5 | Baseline: snapshots, manifests, metadata:data size ratio | |
| 6 | Baseline query: runtime and bytes scanned | |
| 7 | After **binpack**: files, avg size, compaction time | |
| 8 | After **sort-based**: files, avg size, compaction time | |
| 9 | Post-compaction query: runtime and bytes scanned | |
| 10 | Improvement factor (files, runtime, bytes) | |
| 11 | Target file size used / default / permitted range | |
| 12 | The prerequisite that made the sort strategy work | |
| 13 | Maintenance defaults: retention, orphan thresholds, with source | |
| 14 | Manifest count before / after | |
| 15 | Files after one more ingestion day, before and after maintenance | |
| 16 | Outage events: total, isolated, clustered, largest cluster | |
| 17 | Athena query execution ID for the outage query | |
| 18 | Redshift vs Iceberg: runtime and bytes for the outage query | |

### Break Log — mandatory
Three genuine failures with real error text. At least one must be the failed
sort-strategy attempt.

---

## 6 · Defence (15 minutes, live)

1. Show me the `files` metadata table before and after compaction. Read me the
   numbers and tell me what changed.
2. Name the three distinct costs of having many small files.
3. Your sort compaction failed first. What was missing, and how did you find out?
4. What target file size did you choose and why that one?
5. Your table degraded again after one more day of ingestion. At what rate?
   What cadence would you schedule, and how did you derive it?
6. Which maintenance operations are configured per table, and which per bucket?
   Why does that distinction matter operationally?
7. What are the default snapshot retention settings, and how far back can you
   time travel right now?
8. Fola asks why the bill went up. Answer in two sentences, with the mechanism.
9. Meter M has no readings for two hours. Outage, or comms failure? What does
   your pipeline say, and is it right?
10. Ade wants 60-second refresh. Which path supports that, and what breaks first
    if you push it to 10 seconds?

---

## 7 · Rubric

| Area | Marks |
|---|---|
| Micro-batch simulator producing a realistic file problem | 10 |
| **Baseline measurement, complete and from metadata tables** | 12 |
| Explanation of per-file cost — all three named | 6 |
| Silver cleaning, missing-vs-zero design, drift handling | 10 |
| **Compaction comparison table, fully populated** | 16 |
| **The failed sort attempt + the prerequisite that fixed it** | 8 |
| Maintenance defaults researched and correctly reported | 8 |
| Manifest health measured | 5 |
| Outage engine + isolated/clustered classification | 10 |
| Honest failure modes of the outage rule | 6 |
| Degradation re-measurement + justified cadence | 8 |
| Redshift marts + measured comparison + 60s verdict | 7 |
| Evidence Ledger complete and internally consistent | 5 |
| Break Log — three real failures | 5 |
| **Defence** | 20 |
| | **/136, scaled to 100** |

---

## 8 · Rules

- **Using an LLM is allowed and expected.** You are marked on judgement,
  evidence and defence — not on typing.
- **You must create the small-file problem before solving it.** Loading the raw
  file in one shot forfeits the baseline and compaction marks, because there is
  nothing to measure.
- Every number must be reproducible from your own account.
- Do not share tables, snapshots or query IDs with another student. Your file
  counts are unique to your ingestion pattern.
- Stop your EMR Serverless application at the end of each day.
- Ambiguity is deliberate. Decide, document the assumption, defend it.
