# CAPSTONE — Student 5
## Cold-chain integrity, and joins that refuse to shuffle

**Duration:** 3 days · **Stack:** EMR (PySpark) → Apache Iceberg (3 layers) → Redshift
**Total marks:** 100

---

## 0 · The situation

You are the data engineer for a pharmaceutical logistics operator. Refrigerated
trucks carry temperature-sensitive cargo — vaccines, biologics — and every
vehicle streams GPS position and cargo temperature continuously.

**Nadia, Head of Quality**, has a regulatory obligation. If cargo goes outside
its temperature range for longer than a permitted window, that shipment is
**compromised and must be quarantined on arrival**. She needs the breach
detected, evidenced, and attributable to a specific shipment, vehicle, driver and
route segment. Auditors will ask her to reproduce it months later.

**Tom, Head of Operations**, wants ETA accuracy. He is tired of telling customers
a delivery lands at 14:00 when it lands at 19:00.

There is an engineering constraint that dominates everything. The telemetry table
is large and you must **join it to shipment and vehicle reference data on every
single query**. The previous engineer's pipeline shuffles hundreds of gigabytes
on every run and takes forty minutes. **Your mandate is to make those joins stop
shuffling.** That is the technical spine of this exam.

---

## 1 · Dataset

**Logistics and Supply Chain Operations** dataset (Kaggle). Any variant
providing vehicle telemetry plus relational shipment data is acceptable; state
which you used.

Expected fields across the files: shipment identifier, vehicle identifier,
timestamp, GPS latitude and longitude, IoT temperature reading, cargo condition
status, ETA and ETA variation, origin and destination, and route or waypoint
information.

### You must build a genuinely relational model

This is not a single flat file exam. You need **at least three tables that join**:

- **telemetry** — the high-volume time series, keyed by `shipment_id` and
  `vehicle_id`
- **shipments** — one row per shipment: origin, destination, cargo type,
  required temperature range, promised ETA
- **vehicles** — one row per vehicle: type, refrigeration unit model, capacity

If the source dataset does not cleanly provide all three, **derive and document
them**. Manufacture the cargo temperature ranges if absent — for example a 2–8°C
range for vaccines versus −20°C for frozen product — and state your assumptions.
Realistic derivation is marked; a single denormalised table is not acceptable
and forfeits the day-2 marks entirely.

### Known problems — handle all of them

1. **GPS dropouts** — null or obviously wrong coordinates (0,0 in the Gulf of
   Guinea is the classic).
2. **Temperature sensor spikes** — a single reading of 40°C between two of 4°C is
   a sensor artefact, not a breach. Distinguishing a spike from a genuine
   excursion is a real part of the analysis.
3. **Irregular telemetry intervals**, varying by vehicle.
4. **Shipments spanning midnight** and, potentially, time zones.
5. **Orphan telemetry** — rows whose `shipment_id` has no matching shipment row.
   Quantify them. Do not silently drop them.

---

## 2 · What you are building

```
raw/            telemetry + shipment + vehicle files
  |
bronze.telemetry_raw    every ping, as received
bronze.shipments_raw    bronze.vehicles_raw
  |
silver.telemetry        cleaned, spike-flagged, BUCKETED
silver.shipments        cleaned, BUCKETED on the same key, same bucket count
silver.vehicles         small dimension
  |
gold.cold_chain_breaches   evidenced breach events per shipment
gold.eta_performance       promised vs actual, by route and vehicle
  |
Redshift                Nadia's compliance marts, Tom's ops marts
```

---

# DAY 1 — Bronze, and the join baseline you will beat

**Goal: land all three tables, then measure how bad the naive join is.**

### 1.1 Bronze tables
Create all three bronze tables as Iceberg. Partition telemetry on
`days(ping_ts)`. The dimension tables are small — think about whether they need
partitioning at all, and say why in your report.

### 1.2 The baseline join — measure the pain
Write the query Nadia needs most: **telemetry joined to shipments, filtered to a
date range, aggregated per shipment.** Run it against bronze with no
optimisation at all.

Record, from the Spark UI and the query plan:

| Metric | Value |
|---|---|
| Runtime | |
| Join strategy in the physical plan | |
| Number of stages | |
| Shuffle read / shuffle write (bytes) | |
| Max vs median task duration on the join stage | |

Then paste the relevant part of your physical plan and **name the join strategy
you see.** You must be able to read a plan and identify the join type; this is
tested again in the defence.

### 1.3 Profile the dirt
Report: null/invalid GPS percentage, temperature readings outside physical
plausibility, orphan telemetry rows as a count and percentage, and the
distribution of telemetry intervals per vehicle (median and p95).

### Day 1 deliverables
- three bronze DDLs with partitioning decisions justified
- the baseline join measurement table
- the physical plan excerpt with the join strategy named
- the data-quality report

---

# DAY 2 — Silver, and eliminating the shuffle

**Goal: three different join strategies, measured against each other.**

### 2.1 Clean silver
Build `silver.telemetry`, `silver.shipments`, `silver.vehicles`.

For telemetry specifically:
- flag GPS dropouts rather than deleting them
- implement **spike detection**: define the rule that separates a one-reading
  artefact from a genuine excursion. State the rule and the window it uses.
- preserve the raw reading alongside the cleaned one

### 2.2 Strategy A — broadcast the small side
`silver.vehicles` is tiny. Join telemetry to it with an explicit broadcast.
Confirm from the physical plan that the strategy changed. Record runtime and
shuffle bytes.

Then answer with a number: **at what size would broadcasting this table become a
bad idea, and what happens at that point?** Name the mechanism, not just "it gets
slow", and name the configuration property that governs the threshold.

### 2.3 Strategy B — bucketing for a zero-shuffle join — the marked task
`silver.shipments` is not small enough to broadcast, and telemetry joins to it on
every query. This is the case bucketing exists for.

Bucket **both** `silver.telemetry` and `silver.shipments` on `shipment_id`,
using **the same bucket count**, declared in the table specification. Choose the
count deliberately and justify it against your data volume and cluster size.

Then re-run the day-1 baseline query and record:

| | Baseline | Broadcast | Bucketed |
|---|---|---|---|
| Runtime | | | |
| Join strategy in the plan | | | |
| Number of stages | | | |
| Shuffle read bytes | | | |
| Shuffle write bytes | | | |

> **The trap, and it is silent.** If the two tables do not share the exact same
> bucket column *and* the same bucket count, the engine will simply shuffle
> anyway — no error, no warning, and you will have paid the storage rigidity for
> nothing. **Prove your join is genuinely shuffle-free by reading the physical
> plan**, not by assuming. Then deliberately break it: rebuild one side with a
> different bucket count, re-run, and show the shuffle returning. **Both the
> working and the broken plan are marked.**

### 2.4 The trade-off, honestly
Bucketing is not free. In your report, state:

- what it cost at write time
- what happens when you need to change the bucket count later
- when you would **not** bucket a table

### Day 2 deliverables
- silver DDLs including the bucket specification on both sides
- the spike-detection rule and how many spikes it caught
- **the three-way comparison table, fully populated**
- both physical plans: shuffle-free, and the deliberately broken one
- the trade-off discussion

---

# DAY 3 — Gold, breach evidence, and Redshift

**Goal: defensible compliance output and honest ETA analysis.**

### 3.1 `gold.cold_chain_breaches`
A breach is cargo outside its required range for longer than the permitted
window. Use a defensible rule; state it explicitly. For example:

> Temperature outside the shipment's required range continuously for more than
> **30 minutes**, after spike removal.

For each breach produce: shipment, vehicle, start, end, duration, minimum and
maximum temperature during the breach, the required range, the GPS bounding box
of the breach window, and a severity classification you define.

**Nadia's evidence requirement** — a breach record is worthless if it cannot be
reproduced. For **one** breach, produce a full evidence pack:

1. the raw telemetry rows underlying it
2. the cleaned rows, showing what your spike filter removed and why
3. the snapshot ID of the silver table at the moment the breach was computed
4. a **time-travel query** that reproduces the breach from that exact snapshot
5. the shipment and vehicle context joined in

Point 4 is the one auditors care about. Months later, after the table has been
updated many times, Nadia must reproduce this exact finding. Show that she can —
then state what determines **how long** she will still be able to.

### 3.2 `gold.eta_performance` for Tom
Per shipment: promised ETA, actual arrival, variance in minutes. Aggregate by
route, vehicle type and time of day.

Then the honest analysis. Report the distribution — median, p90, worst case —
and identify the two strongest predictors of lateness you can support **from
this data**. Then name at least two you **cannot** measure here (traffic,
weather at the destination, loading-dock queueing) and say what data would be
needed. A confident ETA model with no stated blind spots scores zero on this
section.

### 3.3 Redshift
Expose gold to Redshift and build:

- `mart_compliance_breaches` — Nadia's audit view, one row per breach with full
  context
- `mart_eta_performance` — Tom's route scorecard

Document the access path and why.

Then answer Nadia's real question: **when an auditor asks about a shipment from
eight months ago, can you still produce the evidence pack from section 3.1?**
Give the answer, the mechanism that determines it, and if the answer is no, what
you would change.

---

## 4 · Deliverables

```
/pipeline/        bronze, silver, gold PySpark jobs
/joins/           the three join experiments, runnable independently
/sql/             Athena / Redshift SQL
/evidence/        the ledger below, plans, and the breach evidence pack
/REPORT.md        8-12 pages
```

---

## 5 · Evidence Ledger — mandatory

| # | Item | Value |
|---|---|---|
| 1 | EMR Serverless application ID | |
| 2 | Dataset variant used | |
| 3 | Row counts: telemetry / shipments / vehicles | |
| 4 | Orphan telemetry rows: count and % | |
| 5 | Telemetry interval: median and p95 | |
| 6 | **Baseline join**: runtime, strategy, stages, shuffle read/write | |
| 7 | **Broadcast join**: runtime, strategy, shuffle read/write | |
| 8 | Broadcast threshold property and the value you used | |
| 9 | Bucket count chosen, and your justification in one line | |
| 10 | **Bucketed join**: runtime, strategy, stages, shuffle read/write | |
| 11 | Plan excerpt proving the shuffle-free join | |
| 12 | Plan excerpt after the deliberate bucket-count mismatch | |
| 13 | Spikes detected and removed (count) | |
| 14 | Breaches detected: total, by severity | |
| 15 | Snapshot ID used for the breach evidence pack | |
| 16 | `committed_at` of that snapshot (UTC, to the millisecond) | |
| 17 | ETA variance: median, p90, worst | |
| 18 | Athena query execution ID for the breach query | |
| 19 | Redshift vs Iceberg: runtime and bytes for the compliance query | |

### Break Log — mandatory
Three genuine failures with real error text. At least one must be the
bucket-mismatch shuffle you induced deliberately.

---

## 6 · Defence (15 minutes, live)

1. Show me the baseline physical plan. Name the join strategy and point at the
   exchange.
2. Show me the bucketed plan. Point at what is **missing** compared to the
   baseline, and explain why it is missing.
3. You chose N buckets. Why N and not 4N?
4. You broke the bucketing on purpose. Show me that plan. Why was there no error?
5. At what table size does broadcasting become dangerous? What exactly fails?
6. Show me a spike your filter removed. Convince me it was a sensor artefact and
   not a real excursion.
7. Reproduce breach B from the snapshot you recorded. Now tell me how long that
   will keep working.
8. What did bucketing cost you at write time?
9. When would you not bucket a table?
10. An auditor asks about a shipment from eight months ago. Walk me through
    exactly what you would run.

---

## 7 · Rubric

| Area | Marks |
|---|---|
| Three-table relational model, derived and documented | 10 |
| Bronze design with justified partitioning | 6 |
| **Baseline join measured + plan read correctly** | 10 |
| Data-quality profiling incl. orphan quantification | 7 |
| Spike detection rule + evidence it works | 8 |
| Broadcast strategy + threshold reasoning | 8 |
| **Bucketing implemented on both sides, same key and count** | 14 |
| **Three-way comparison table, fully populated** | 10 |
| **Deliberate bucket mismatch, with both plans shown** | 8 |
| Bucketing trade-off discussion | 5 |
| Breach detection + severity design | 8 |
| **Breach evidence pack, all five points incl. time travel** | 12 |
| ETA analysis with stated blind spots | 8 |
| Redshift marts + the eight-month answer | 7 |
| Evidence Ledger complete and internally consistent | 5 |
| Break Log — three real failures | 5 |
| **Defence** | 20 |
| | **/151, scaled to 100** |

---

## 8 · Rules

- **Using an LLM is allowed and expected.** You are marked on judgement,
  evidence and defence — not on typing.
- A single denormalised table forfeits the day-2 marks. The join is the exam.
- You must **read physical plans**, not assume outcomes. Claims about shuffle
  behaviour that are not backed by a pasted plan score zero.
- Every number must be reproducible from your own account.
- Do not share tables, snapshots or query IDs with another student.
- Stop your EMR Serverless application at the end of each day.
- Ambiguity is deliberate. Decide, document the assumption, defend it.
