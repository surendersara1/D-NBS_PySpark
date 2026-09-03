# PROJECT 5 — Cold-Chain Integrity & Joins That Refuse to Shuffle

**Stack:** AWS EMR (PySpark) · Apache Iceberg · AWS Glue Data Catalog · Amazon Redshift
**Duration:** 3 days · **Level:** senior data engineer

You are the data engineer for a pharmaceutical logistics operator. Refrigerated
vehicles carry temperature-sensitive cargo and stream position and cargo
temperature continuously. A regulator requires that any temperature excursion
beyond the permitted window be detected, evidenced, attributed to a shipment and
vehicle, and reproducible months later.

Three engineering problems dominate this build:

1. **Every query joins telemetry to reference data.** The previous pipeline
   shuffles hundreds of gigabytes on every run. Making that join stop shuffling
   — and proving it from the physical plan — is the technical spine.
2. **A sensor spike is not an excursion.** One reading of 40 °C between two of
   4 °C is an artefact. Distinguishing that from a genuine 35-minute breach is
   the analytical core.
3. **Evidence must survive the table changing.** An auditor in eight months must
   reproduce today's breach finding exactly. That is a snapshot-retention
   decision made now.

---

# THE THREE QUESTIONS

### Q1 · Cold-chain breach, evidenced
> Which shipments experienced cargo temperature outside their required range for
> **more than 30 continuous minutes**, after sensor-spike removal — and for each
> breach, can you reproduce the exact finding from the **snapshot it was computed
> against**, with the raw and cleaned rows, the vehicle, the driver segment and
> the GPS bounding box?

### Q2 · The cost of the join
> For the standard compliance query — telemetry joined to shipments and vehicles,
> filtered to a week, aggregated per shipment — what are the runtime and shuffle
> bytes under a naive sort-merge join, a broadcast join, and a bucketed join —
> and what happens to the bucketed plan if the two sides disagree on bucket count?

### Q3 · ETA variance and its blind spots
> Per route and vehicle type, what is the distribution of arrival variance
> against promised ETA — which two factors in the data best predict lateness —
> and which factors that *actually* drive lateness are **absent** from this data?

---

# DATASET

**Logistics and Supply Chain Dataset** (Kaggle) — hourly telemetry with
`Timestamp`, `Vehicle_GPS_Latitude`, `Vehicle_GPS_Longitude`, `IoT_Temperature`,
`Cargo_Condition_Status`, `ETA_Variation_Hours`, `Traffic_Congestion_Level`,
`Weather_Condition_Severity`, `Route_Risk_Level`, `Driver_Behavior_Score`,
`Fatigue_Monitoring_Score`, `Delivery_Time_Deviation`, `Delay_Probability` and
similar. Confirm column names on download.

Two facts about this dataset shape the whole project:

1. **It is small** — on the order of 30k rows. That is not enough for a join
   measurement to mean anything.
2. **It has no shipment or vehicle keys.** It is a flat telemetry stream with no
   relational structure.

So you must **build the relational model and scale it**. That harness is graded
as production code.

### Harness requirements

**Synthesise three tables** from the flat file:

```
shipments   shipment_id, vehicle_id, origin, destination, cargo_class,
            required_min_c, required_max_c, promised_eta_ts, actual_arrival_ts
vehicles    vehicle_id, vehicle_type, reefer_model, capacity_kg
telemetry   ping_id, shipment_id, vehicle_id, ping_ts, lat, lon, cargo_temp_c,
            cargo_status, traffic_level, weather_severity, driver_score, fatigue_score
```

Rules:

| Rule | Why |
|---|---|
| Assign every telemetry row to a `shipment_id`; a shipment is a contiguous run of pings for one vehicle between an origin and a destination | Gives Q1 something to attribute to |
| `cargo_class` drawn from {`vaccine` 2–8 °C, `frozen` −25 to −15 °C, `ambient` 15–25 °C}; set `required_min_c` / `required_max_c` accordingly | Q1 needs a range to breach |
| Scale telemetry to **≥ 50 million rows** by replicating shipments across time with jitter on timestamps and temperatures | Q2 is meaningless on 30k rows |
| Inject **temperature spikes**: ~0.5% of pings get a single-reading artefact of ±20 °C | Q1's spike filter must have something to remove |
| Inject **genuine excursions**: ~2% of shipments get a 20–90 minute contiguous drift outside range | Q1 must detect these and only these |
| Inject **GPS dropouts**: ~1% of pings at `(0, 0)` or `NULL` | Cleaning target |
| Inject **orphan telemetry**: ~0.3% of pings with a `shipment_id` that has no shipment row | Must be quantified, not dropped silently |
| Keep `shipments` at ~500k rows and `vehicles` at ~2k | One side is broadcastable; the other is not — that contrast is the exam |

Land as `s3://<bucket>/raw/{telemetry,shipments,vehicles}/`. State the seed so
your data is reproducible.

---

# ARCHITECTURE

```
s3://raw/telemetry/  s3://raw/shipments/  s3://raw/vehicles/
        │  Step 1
bronze_telemetry · bronze_shipments · bronze_vehicles
        │  Step 3  baseline join measured here — the "before"
        │  Step 4  spike detection, GPS cleaning
silver_telemetry            days(ping_ts), bucket(64, shipment_id), sorted
silver_shipments            bucket(64, shipment_id)  ← same key, same N
silver_vehicles             tiny, unpartitioned
        │  Step 5/6  broadcast · bucketed · deliberately-broken joins
        │  Step 7
gold_cold_chain_breaches    evidenced, with snapshot ID
gold_eta_performance        variance by route and vehicle type
        │  Step 9
Redshift Spectrum           compliance + ops marts
```

---

# THE TEN STEPS

## Step 1 · Bronze ingestion

```python
telemetry = (spark.read.parquet(f"s3://{BUCKET}/raw/telemetry/")
    .withColumn("_src_file",    F.input_file_name())
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_batch_id",    F.lit(BATCH_ID))
    .withColumn("ping_date",    F.to_date("ping_ts")))
telemetry.writeTo("glue_catalog.coldchain_db.bronze_telemetry").overwritePartitions()
```

```sql
CREATE TABLE glue_catalog.coldchain_db.bronze_telemetry (
    ping_id          STRING,
    shipment_id      STRING,
    vehicle_id       STRING,
    ping_ts          TIMESTAMP,
    lat              DOUBLE,
    lon              DOUBLE,
    cargo_temp_c     DOUBLE,
    cargo_status     STRING,
    traffic_level    STRING,
    weather_severity STRING,
    driver_score     DOUBLE,
    fatigue_score    DOUBLE,
    _src_file        STRING,
    _ingested_at     TIMESTAMP,
    _batch_id        STRING,
    ping_date        DATE
) USING iceberg
PARTITIONED BY (ping_date)
TBLPROPERTIES ('format-version' = '2');
```

Load `shipments` and `vehicles` unpartitioned — they are dimension tables and
partitioning them buys nothing. Say so in the report.

**Profile:** rows per table; GPS null/(0,0) count and %; temperature readings
outside physical plausibility (−40 to +60 °C); **orphan telemetry** — pings whose
`shipment_id` has no shipment — as count and %; telemetry interval median and p95
per vehicle.

---

## Step 2 · Silver schema — bucketed on both sides

```sql
CREATE TABLE glue_catalog.coldchain_db.silver_telemetry (
    ping_id          STRING,
    shipment_id      STRING,
    vehicle_id       STRING,
    ping_ts          TIMESTAMP,
    lat              DOUBLE,
    lon              DOUBLE,
    gps_valid        BOOLEAN,
    cargo_temp_raw   DOUBLE,
    cargo_temp_c     DOUBLE,
    is_spike         BOOLEAN,
    cargo_status     STRING,
    traffic_level    STRING,
    weather_severity STRING,
    driver_score     DOUBLE,
    fatigue_score    DOUBLE,
    _updated_at      TIMESTAMP
) USING iceberg
PARTITIONED BY (days(ping_ts), bucket(64, shipment_id))
TBLPROPERTIES (
    'format-version'               = '2',
    'write.target-file-size-bytes' = '134217728',
    'write.distribution-mode'      = 'hash'
);
ALTER TABLE glue_catalog.coldchain_db.silver_telemetry
  WRITE ORDERED BY shipment_id, ping_ts;

CREATE TABLE glue_catalog.coldchain_db.silver_shipments (
    shipment_id        STRING,
    vehicle_id         STRING,
    origin             STRING,
    destination        STRING,
    cargo_class        STRING,
    required_min_c     DOUBLE,
    required_max_c     DOUBLE,
    promised_eta_ts    TIMESTAMP,
    actual_arrival_ts  TIMESTAMP
) USING iceberg
PARTITIONED BY (bucket(64, shipment_id))          -- SAME key, SAME count
TBLPROPERTIES ('format-version' = '2');

CREATE TABLE glue_catalog.coldchain_db.silver_vehicles (
    vehicle_id    STRING,
    vehicle_type  STRING,
    reefer_model  STRING,
    capacity_kg   DOUBLE
) USING iceberg;                                    -- tiny; no partitioning
```

**`bucket(64, shipment_id)` on both telemetry and shipments, identical.** That
is the entire step-6 mechanism. Justify 64 against 50M pings, 500k shipments,
and your executor count.

---

## Step 3 · The baseline join — measure the pain first

Before optimising anything, run the compliance query naively against
**bronze** and capture the plan.

```python
baseline = spark.sql("""
    SELECT s.shipment_id, s.cargo_class, v.vehicle_type,
           count(*)             AS pings,
           avg(t.cargo_temp_c)  AS mean_temp,
           max(t.cargo_temp_c)  AS max_temp
    FROM glue_catalog.coldchain_db.bronze_telemetry t
    JOIN glue_catalog.coldchain_db.bronze_shipments s ON t.shipment_id = s.shipment_id
    JOIN glue_catalog.coldchain_db.bronze_vehicles  v ON s.vehicle_id  = v.vehicle_id
    WHERE t.ping_ts >= TIMESTAMP '2026-06-01' AND t.ping_ts < TIMESTAMP '2026-06-08'
    GROUP BY 1, 2, 3""")

print(baseline._jdf.queryExecution().executedPlan().toString())
baseline.write.mode("overwrite").parquet(f"s3://{BUCKET}/tmp/baseline/")   # force execution
```

From the Spark UI, record:

| Metric | Baseline |
|---|---|
| Runtime | |
| Join strategy, telemetry ⋈ shipments | |
| Join strategy, shipments ⋈ vehicles | |
| Stages | |
| Shuffle read bytes / shuffle write bytes | |
| Max vs median task duration on the join stage | |

**Name the join strategies from the plan.** You will be asked to point at the
`Exchange` nodes in review.

---

## Step 4 · Silver cleaning — spikes and GPS

```python
w = Window.partitionBy("shipment_id").orderBy("ping_ts")

clean = (spark.table("glue_catalog.coldchain_db.bronze_telemetry")
    .withColumn("gps_valid",
        F.col("lat").isNotNull() & F.col("lon").isNotNull() &
        ~((F.abs("lat") < 0.001) & (F.abs("lon") < 0.001)) &
        F.col("lat").between(-90, 90) & F.col("lon").between(-180, 180))
    .withColumn("prev_t", F.lag("cargo_temp_c", 1).over(w))
    .withColumn("next_t", F.lead("cargo_temp_c", 1).over(w))
    # a spike: one reading far from BOTH neighbours, which agree with each other
    .withColumn("is_spike",
        (F.abs(F.col("cargo_temp_c") - F.col("prev_t")) > 8) &
        (F.abs(F.col("cargo_temp_c") - F.col("next_t")) > 8) &
        (F.abs(F.col("prev_t") - F.col("next_t")) < 2))
    .withColumn("cargo_temp_raw", F.col("cargo_temp_c"))
    .withColumn("cargo_temp_c",
        F.when(F.col("is_spike"), (F.col("prev_t") + F.col("next_t")) / 2)
         .otherwise(F.col("cargo_temp_c"))))
```

**The rule in words:** a reading is a spike if it disagrees with both
neighbours by more than 8 °C while the neighbours agree with each other within
2 °C. A genuine excursion has neighbours that *also* drift. State your
thresholds and how you chose them. Report spikes detected vs spikes injected —
your harness knows the truth, so **you can compute the filter's precision and
recall**. Do it.

Keep `cargo_temp_raw`. The auditor will want to see what you changed.

---

## Step 5 · Strategy A — broadcast the small side

`silver_vehicles` is ~2k rows. Force it to every executor.

```python
from pyspark.sql.functions import broadcast
q = (silver_telemetry.join(silver_shipments, "shipment_id")
                     .join(broadcast(silver_vehicles), "vehicle_id"))
```

Confirm from the plan that `shipments ⋈ vehicles` became a `BroadcastHashJoin`.
Record runtime and shuffle bytes. The telemetry ⋈ shipments join is still a
shuffle at this point — that is what step 6 removes.

Then answer: **at what size does broadcasting `vehicles` become a bad idea, and
what fails first?** Name the mechanism (it is collected to the driver) and the
property that governs the automatic threshold.

---

## Step 6 · Strategy B — bucketed, shuffle-free — the marked step

Both sides are bucketed on `shipment_id` into 64. Re-run the compliance query
on silver.

```python
spark.conf.set("spark.sql.sources.bucketing.enabled", "true")
spark.conf.set("spark.sql.iceberg.planning.preserve-data-grouping", "true")

q = spark.sql("""
    SELECT s.shipment_id, s.cargo_class, v.vehicle_type,
           count(*) AS pings, avg(t.cargo_temp_c) AS mean_temp, max(t.cargo_temp_c) AS max_temp
    FROM glue_catalog.coldchain_db.silver_telemetry t
    JOIN glue_catalog.coldchain_db.silver_shipments s ON t.shipment_id = s.shipment_id
    JOIN glue_catalog.coldchain_db.silver_vehicles  v ON s.vehicle_id  = v.vehicle_id
    WHERE t.ping_ts >= TIMESTAMP '2026-06-01' AND t.ping_ts < TIMESTAMP '2026-06-08'
    GROUP BY 1, 2, 3""")
print(q._jdf.queryExecution().executedPlan().toString())
```

| | Baseline (bronze) | Broadcast only | Bucketed + broadcast |
|---|---|---|---|
| Runtime | | | |
| telemetry ⋈ shipments strategy | | | |
| shipments ⋈ vehicles strategy | | | |
| Stages | | | |
| Shuffle read bytes | | | |
| Shuffle write bytes | | | |

**Prove it from the plan.** The `Exchange` node before the telemetry ⋈ shipments
join must be **absent**. If it is present, bucketing did not engage — check the
two config properties above, the Spark/Iceberg versions, and that both tables
report the same spec in their `partitions` metadata.

### The deliberate break

Rebuild `silver_shipments` with `bucket(32, shipment_id)`. Re-run. Paste the
plan.

> **There is no error.** The engine notices the specs disagree and simply
> shuffles — you pay the storage rigidity of bucketing and get none of the
> benefit, silently. This is why bucket counts are a contract between tables, not
> a per-table tuning knob. Both the shuffle-free plan and the broken plan are
> deliverables.

Restore 64 afterwards.

### The trade-off

State: what bucketing cost at write time (the hash exchange on ingest); what
changing N later means (full rewrite of both sides); when you would **not**
bucket (small tables, ad-hoc join keys, tables joined on several different keys).

---

## Step 7 · Gold — breaches with evidence

### 7a · `gold_cold_chain_breaches` — Q1

```python
w = Window.partitionBy("shipment_id").orderBy("ping_ts")

flagged = (silver_telemetry.join(silver_shipments, "shipment_id")
    .withColumn("out_of_range",
        (F.col("cargo_temp_c") < F.col("required_min_c")) |
        (F.col("cargo_temp_c") > F.col("required_max_c")))
    .withColumn("prev_oor", F.lag("out_of_range").over(w))
    .withColumn("run_start", F.when(F.col("out_of_range") & ~F.coalesce("prev_oor", F.lit(False)), 1).otherwise(0))
    .withColumn("run_id", F.sum("run_start").over(w.rowsBetween(Window.unboundedPreceding, 0))))

breaches = (flagged.where("out_of_range")
    .groupBy("shipment_id", "vehicle_id", "cargo_class", "required_min_c", "required_max_c", "run_id")
    .agg(F.min("ping_ts").alias("breach_start"), F.max("ping_ts").alias("breach_end"),
         F.min("cargo_temp_c").alias("min_temp"), F.max("cargo_temp_c").alias("max_temp"),
         F.min("lat").alias("bbox_lat_min"), F.max("lat").alias("bbox_lat_max"),
         F.min("lon").alias("bbox_lon_min"), F.max("lon").alias("bbox_lon_max"),
         F.avg("driver_score").alias("driver_score_mean"),
         F.count("*").alias("pings"))
    .withColumn("duration_min",
        (F.col("breach_end").cast("long") - F.col("breach_start").cast("long")) / 60)
    .where("duration_min > 30")
    .withColumn("severity",
        F.when(F.col("duration_min") > 120, "CRITICAL")
         .when(F.col("duration_min") > 60,  "HIGH")
         .otherwise("MEDIUM"))
    .withColumn("computed_at",  F.current_timestamp())
    .withColumn("silver_snapshot_id", F.lit(CURRENT_SILVER_SNAPSHOT)))   # capture it
```

**Capture the silver snapshot ID into every breach row.** That is the evidence
link. Get it before you compute:

```sql
SELECT snapshot_id FROM glue_catalog.coldchain_db.silver_telemetry.snapshots
ORDER BY committed_at DESC LIMIT 1;
```

Your harness injected excursions — compute **detection precision and recall**
against the injected truth. Report both. Then the honest paragraph: what your
rule cannot see (a 29-minute breach; two 20-minute breaches 5 minutes apart;
a reefer that fails while parked with the sensor disconnected).

### 7b · `gold_eta_performance` — Q3

Per shipment: `promised_eta_ts`, `actual_arrival_ts`, variance in minutes.
Aggregate by route (`origin`→`destination`), `vehicle_type`, hour of departure.
Report median, p90, worst case.

Then find the **two strongest predictors of lateness in the data** — candidates
are `traffic_level`, `weather_severity`, `driver_score`, `fatigue_score`,
`route_risk`. Use correlation or a simple grouped comparison; you are not
building a model. Then name at least two drivers that are **absent**: destination
dock queueing, load/unload time, customs, actual road closures. A confident
predictor list with no stated blind spots scores zero.

---

## Step 8 · The evidence pack — one breach, fully reproducible

For **one** breach from step 7a, produce:

```sql
-- 1. the raw pings, from the exact snapshot the breach was computed against
SELECT ping_ts, cargo_temp_raw, cargo_temp_c, is_spike, lat, lon
FROM glue_catalog.coldchain_db.silver_telemetry VERSION AS OF <silver_snapshot_id>
WHERE shipment_id = '<id>' AND ping_ts BETWEEN <breach_start> - INTERVAL 30 MINUTES
                                           AND <breach_end>   + INTERVAL 30 MINUTES
ORDER BY ping_ts;

-- 2. what the spike filter changed
SELECT ping_ts, cargo_temp_raw, cargo_temp_c
FROM glue_catalog.coldchain_db.silver_telemetry VERSION AS OF <silver_snapshot_id>
WHERE shipment_id = '<id>' AND is_spike;

-- 3. the shipment and vehicle context
SELECT s.*, v.vehicle_type, v.reefer_model
FROM glue_catalog.coldchain_db.silver_shipments s JOIN glue_catalog.coldchain_db.silver_vehicles v USING (vehicle_id)
WHERE s.shipment_id = '<id>';

-- 4. the breach row itself
SELECT * FROM glue_catalog.coldchain_db.gold_cold_chain_breaches WHERE shipment_id = '<id>';
```

Then **change the live table** — run an `UPDATE` touching that shipment's rows —
and re-run query 1. It must return the same rows. That is the reproducibility
guarantee.

### Retention — the decision that makes or breaks the audit

```sql
CALL glue_catalog.system.expire_snapshots(
    table       => 'coldchain_db.silver_telemetry',
    older_than  => TIMESTAMP '2026-01-01 00:00:00',   -- 8+ months back
    retain_last => 200);
```

State your retention policy and the resulting time-travel horizon. **An
eight-month audit requirement with a five-day retention setting is a failed
audit that nobody notices until the auditor arrives.** If you are on S3 Tables,
the managed default max snapshot age is far shorter than eight months — check
it, and either change it or tag the snapshot (and then check that tagging did
not disable automated expiry for the whole table).

---

## Step 9 · Redshift

```sql
CREATE EXTERNAL SCHEMA spectrum_coldchain
FROM DATA CATALOG DATABASE 'coldchain_db'
IAM_ROLE 'arn:aws:iam::<ACCOUNT>:role/RedshiftLakehouseRole' REGION 'us-east-1';

CREATE MATERIALIZED VIEW mv_compliance_breaches AS
SELECT b.shipment_id, b.vehicle_id, v.vehicle_type, b.cargo_class,
       b.breach_start, b.breach_end, b.duration_min, b.min_temp, b.max_temp,
       b.severity, b.silver_snapshot_id
FROM spectrum_coldchain.gold_cold_chain_breaches b
JOIN spectrum_coldchain.silver_vehicles v USING (vehicle_id);

CREATE MATERIALIZED VIEW mv_eta_route_scorecard AS
SELECT origin, destination, vehicle_type,
       count(*) AS shipments,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY variance_min) AS median_var,
       percentile_cont(0.9) WITHIN GROUP (ORDER BY variance_min) AS p90_var
FROM spectrum_coldchain.gold_eta_performance
GROUP BY 1, 2, 3;
```

Measure the compliance query three ways — Athena on Iceberg, Spectrum, MV —
runtime and bytes. Note that `silver_snapshot_id` travels into the mart: that is
how an auditor querying Redshift gets back to the exact Iceberg snapshot.

---

## Step 10 · Reporting and validation

```sql
-- Q1
SELECT cargo_class, severity, count(*) AS breaches,
       avg(duration_min) AS mean_min, max(duration_min) AS worst_min
FROM spectrum_coldchain.gold_cold_chain_breaches GROUP BY 1, 2 ORDER BY 1, 2;

-- Q3
SELECT * FROM mv_eta_route_scorecard ORDER BY p90_var DESC LIMIT 20;
```

Q2 is the three-way join table from step 6, plus the broken-bucket plan.

Then answer the auditor's question in prose: **a shipment from eight months ago
— walk through exactly what you would run, and whether it would still work.**

---

# SUGGESTED 3-DAY PLAN

| Day | Steps | Checkpoint |
|---|---|---|
| 1 | Harness, 1–3 | Three tables synthesised and scaled; bronze loaded; baseline plan captured with strategies named |
| 2 | 4–6 | Spike filter with precision/recall; broadcast plan; **bucketed plan with no Exchange**; broken plan captured |
| 3 | 7–10 | Breaches with snapshot IDs; evidence pack reproduced after a live UPDATE; retention set; Redshift three-way |

---

# ACCEPTANCE CRITERIA

| # | Criterion | Pass condition |
|---|---|---|
| A1 | Harness | Three relational tables; ≥ 50M telemetry rows; all six injections present and quantified |
| A2 | Baseline | Plan captured; both join strategies named; shuffle bytes recorded |
| A3 | Spike filter | Rule stated; precision and recall vs injected truth |
| A4 | Broadcast | `BroadcastHashJoin` in plan; threshold property named; failure mode described |
| A5 | **Bucketed join** | Plan with **no `Exchange`** before telemetry ⋈ shipments |
| A6 | **Deliberate break** | Mismatched-count plan pasted; shuffle visibly returns; no error |
| A7 | Three-way table | Fully populated |
| A8 | Q1 | Breaches with `silver_snapshot_id`; precision/recall vs injected excursions; limitations |
| A9 | **Evidence pack** | Four queries; reproduced after a live `UPDATE`; retention set to cover 8 months |
| A10 | Q3 | Distribution; two in-data predictors; two absent drivers |
| A11 | Redshift | `silver_snapshot_id` reaches the mart; three-way measurement |

---

# EVIDENCE PACK

| # | Item | Value |
|---|---|---|
| 1 | EMR cluster / Serverless application ID | |
| 2 | Storage type: general-purpose S3 or S3 Tables | |
| 3 | Harness seed; rows: telemetry / shipments / vehicles | |
| 4 | Injections: spikes, excursions, GPS dropouts, orphans — counts | |
| 5 | Orphan telemetry % ; interval median / p95 | |
| 6 | **Baseline:** runtime, both join strategies, stages, shuffle read/write | |
| 7 | **Broadcast:** runtime, strategies, shuffle bytes; threshold property + value | |
| 8 | Bucket count; justification in one line | |
| 9 | **Bucketed:** runtime, strategies, stages, shuffle read/write | |
| 10 | Plan excerpt — no Exchange before telemetry ⋈ shipments | |
| 11 | Plan excerpt — after the 32-bucket mismatch | |
| 12 | Spike filter precision / recall | |
| 13 | Breaches: total, by severity; detection precision / recall | |
| 14 | `silver_snapshot_id` on the evidence-pack breach; `committed_at` | |
| 15 | Evidence query 1 result before and after the live UPDATE (identical) | |
| 16 | Retention setting; resulting time-travel horizon | |
| 17 | ETA variance median / p90 / worst; two predictors; two absent drivers | |
| 18 | Athena / Spectrum / MV: runtime + bytes | |

**Break log.** Three genuine failures with real error text. One must be the
bucket-mismatch shuffle. One must come from the bucketing not engaging on the
first try (config, version, or spec mismatch).

---

# TECHNICAL REVIEW (45 min, live)

1. Show the baseline plan. Point at each `Exchange` and name the join strategy
   it feeds.
2. Show the bucketed plan. Point at what is **missing**. Why is it missing?
3. Why 64 buckets and not 256?
4. Show the 32-bucket plan. Why was there no error?
5. At what size does broadcasting `vehicles` become dangerous? What fails?
6. Show a spike your filter removed. Convince me it was an artefact.
7. Your spike filter's recall is X. Show me an injected spike it missed.
8. Reproduce breach B from its snapshot. Now `UPDATE` that shipment and run it
   again.
9. What is your time-travel horizon? Does it cover eight months? Which setting?
10. What did bucketing cost you at write time?
11. When would you not bucket a table?
12. Two lateness predictors in the data, and two that are not. Go.

---

# NOTES

- LLM assistance is expected. Assessment is on judgement, measurement and
  defence. Every number must be reproducible from your account.
- A single flat table forfeits steps 2–6. The join **is** the project.
- Claims about shuffle behaviour are backed by a pasted physical plan or they
  are not claims.
- Your harness knows ground truth for spikes and excursions. Use it — precision
  and recall are expected, not optional.
- Stop EMR at the end of each day. Budget alarm first.
