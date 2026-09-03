# PROJECT 4 — Smart-Meter Ingestion, the Small-File Problem & an Outage Engine

**Stack:** AWS EMR (PySpark) · Apache Iceberg · AWS Glue Data Catalog · Amazon Redshift
**Duration:** 3 days · **Level:** senior data engineer

You run data engineering for an electricity distributor. Thousands of smart
meters report every half hour. The previous pipeline wrote each arriving batch
straight through, and the table now has hundreds of thousands of tiny files.
Planning takes longer than reading. The nightly report went from four minutes to
fifty-one. The CFO has seen the S3 bill.

Three engineering problems dominate this build:

1. **The file layout, not the data volume.** You must manufacture the problem
   realistically, measure it, fix it with the right compaction strategy, and
   prove it stays fixed under continued ingestion.
2. **Missing is not zero.** A meter that went silent and a meter that used
   nothing are different facts. Conflating them breaks both billing and outage
   detection.
3. **Detecting absence at scale.** Outages are gaps in a time series, per meter,
   clustered across meters. That is a windowing problem with a skew problem
   inside it.

---

# THE THREE QUESTIONS

### Q1 · Outage detection and blast radius
> Which meters went silent for **three or more consecutive half-hour intervals**
> having previously been reporting — and for each event, how many other meters
> in the **same block** went silent in an overlapping window?

A single silent meter is probably a meter fault. Forty silent meters on one block
is a network fault. Your output must classify each event and give operations the
blast radius.

### Q2 · The cost of layout
> For the standard nightly query, what is the runtime and bytes-scanned **before
> and after** compaction — and after **one more day** of ingestion, how fast does
> the table degrade again?

This is the CFO's question. The answer is a measured degradation rate and a
compaction cadence derived from it, not a round number.

### Q3 · Weather-driven load
> How does hourly consumption respond to temperature, by customer segment — and
> at what temperature does the demand curve inflect?

Household demand has a U-shape against temperature: heating at the cold end,
cooling at the warm end. Find the inflection per ACORN group and quantify the
slope on each side.

---

# DATASET

**Smart Meters in London** (Kaggle; UK Power Networks / London Datastore).
~5,500 households, half-hourly readings, November 2011 – February 2014.

| File | Grain | Columns you need |
|---|---|---|
| `halfhourly_dataset/block_*.csv` (112 files) | one row per meter per half hour | `LCLid`, `tstp`, `energy(kWh/hh)` |
| `informations_households.csv` | one row per meter | `LCLid`, `stdorToU`, `Acorn`, `Acorn_grouped`, `file` (block) |
| `weather_hourly_darksky.csv` | one row per hour | `time`, `temperature`, `humidity`, `windSpeed`, `precipType`, `summary` |
| `uk_bank_holidays.csv` | one row per holiday | `Bank holidays`, `Type` |

Confirm column names on download. **`energy(kWh/hh)` is a string in the source
CSVs** and contains the literal `Null` for missing readings. That is not a
parsing bug to hide; it is the missing-vs-zero problem made concrete.

The full half-hourly dataset is roughly **167 million rows**. Use it. This
project needs volume for the compaction measurement to mean anything.

### Known dirt — handle all of it

| Problem | Reality |
|---|---|
| `Null` string in `energy(kWh/hh)` | Missing reading. Must become a real `NULL` plus a status, never `0.0` |
| Timestamps not on the half-hour boundary | Small drift. Snap to the grid; record the original |
| Duplicate `(LCLid, tstp)` | Same interval reported twice |
| Meters that start late / stop early | Their absence before install is not an outage |
| `stdorToU` — some meters on time-of-use tariff | Different consumption pattern; segment on it |
| Weather is hourly; readings are half-hourly | Join grain is a decision |
| Timezone | Source is UK local time with DST. Decide UTC or local and be consistent |

### Micro-batch simulator — you must build this

The Kaggle export is one file per block. The real pipeline receives **one small
file per half hour per block**. Replay it that way:

```
s3://<bucket>/raw/readings/block=<block>/dt=<date>/hh=<0000..2330>/part.json
```

Minimum **14 simulated days across all 112 blocks** — that is
14 × 48 × 112 ≈ **75,000 files** before you even start. Each Iceberg commit
should correspond to one simulated half-hour across all blocks, giving ~670
snapshots. **This is the problem you are then paid to fix.**

Loading the 112 block files directly forfeits the entire measurement — there is
nothing to compact.

---

# ARCHITECTURE

```
s3://raw/readings/block=*/dt=*/hh=*/     ~75k micro-batch files
s3://raw/households/  s3://raw/weather/
        │  Step 1  — one commit per simulated half-hour
bronze_readings                         append-only; ~670 snapshots; tiny files
bronze_households · bronze_weather
        │  Step 2  dedup, drift snap, Null → NULL + status
silver_readings                         days(reading_ts), bucket(32, meter_id), sorted
silver_meter_dim                        meter → block → acorn → tariff
        │  Step 4  weather join
        │  Step 6  compaction experiment  ← the measurement
        │  Step 8
gold_load_hourly                        per meter, per block, completeness %
gold_outage_events                      isolated vs clustered, blast radius
gold_weather_response                   demand vs temperature by acorn group
        │  Step 9
Redshift Spectrum                       ops marts, CFO cost view
```

---

# THE TEN STEPS

## Step 1 · Micro-batch ingestion — manufacture the problem

```python
from pyspark.sql import functions as F, types as T

schema = T.StructType([
    T.StructField("LCLid", T.StringType()),
    T.StructField("tstp",  T.StringType()),
    T.StructField("energy(kWh/hh)", T.StringType()),   # deliberately STRING
])

def load_half_hour(date, hh):
    path = f"s3://{BUCKET}/raw/readings/block=*/dt={date}/hh={hh}/"
    batch = (spark.read.schema(schema).json(path)
        .withColumnRenamed("energy(kWh/hh)", "energy_raw")
        .withColumn("meter_id",     F.col("LCLid"))
        .withColumn("reading_ts",   F.to_timestamp("tstp"))
        .withColumn("_src_file",    F.input_file_name())
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_batch_id",    F.lit(f"{date}-{hh}"))
        .withColumn("reading_date", F.to_date("reading_ts")))
    batch.writeTo("glue_catalog.grid_db.bronze_readings").append()   # ONE COMMIT
```

Yes, `append()` — this step is *supposed* to produce the small-file problem.
One commit per half-hour across all blocks is exactly what a streaming sink does.

```sql
CREATE TABLE glue_catalog.grid_db.bronze_readings (
    meter_id      STRING,
    reading_ts    TIMESTAMP,
    energy_raw    STRING,
    _src_file     STRING,
    _ingested_at  TIMESTAMP,
    _batch_id     STRING,
    reading_date  DATE
) USING iceberg
PARTITIONED BY (reading_date)
TBLPROPERTIES ('format-version' = '2');
```

### The baseline — the most important measurement in the project

```sql
SELECT count(*)                                   AS data_files,
       sum(record_count)                          AS rows,
       cast(avg(file_size_in_bytes) AS BIGINT)    AS avg_bytes,
       percentile_approx(file_size_in_bytes, 0.5) AS median_bytes,
       min(file_size_in_bytes)                    AS min_bytes,
       max(file_size_in_bytes)                    AS max_bytes
FROM glue_catalog.grid_db.bronze_readings.files WHERE content = 0;

SELECT partition, count(*) AS files
FROM glue_catalog.grid_db.bronze_readings.files
GROUP BY partition ORDER BY files DESC LIMIT 5;

SELECT count(*) FROM glue_catalog.grid_db.bronze_readings.snapshots;
SELECT count(*), sum(length) FROM glue_catalog.grid_db.bronze_readings.manifests;
```

Record all of it. Then run the **standard nightly query** — one day, one block,
total consumption per meter — and record runtime and bytes scanned. That is the
"before" number for Q2.

**Explain the per-file cost.** There are at least three distinct costs to
opening many small files. Name them. "It is slow" is not an answer.

---

## Step 2 · Silver cleaning — missing is not zero

```python
from pyspark.sql.window import Window

w_dup = (Window.partitionBy("meter_id", "reading_ts_snapped")
               .orderBy(F.col("_ingested_at").desc(), F.col("_src_file").desc()))

clean = (spark.table("glue_catalog.grid_db.bronze_readings")
    # snap to the half-hour grid, keep the original
    .withColumn("reading_ts_snapped",
        F.from_unixtime((F.col("reading_ts").cast("long") / 1800).cast("long") * 1800)
         .cast("timestamp"))
    .withColumn("drift_seconds",
        F.col("reading_ts").cast("long") - F.col("reading_ts_snapped").cast("long"))
    # Null string -> real NULL + status
    .withColumn("energy_kwh",
        F.when(F.trim("energy_raw").isin("Null", ""), F.lit(None))
         .otherwise(F.col("energy_raw").cast("double")))
    .withColumn("reading_status",
        F.when(F.col("energy_kwh").isNull(), "MISSING")
         .when(F.col("energy_kwh") < 0,       "NEGATIVE")
         .when(F.col("energy_kwh") == 0,      "ZERO")
         .otherwise("OK"))
    .withColumn("_rn", F.row_number().over(w_dup)).where("_rn = 1").drop("_rn"))
```

`MISSING` and `ZERO` are **different states** and must stay different through
every layer. A `ZERO` is a reading; a `MISSING` is the absence of one. Q1 is
built entirely on that distinction.

Report: drift distribution (how many readings were off-grid, by how much);
`MISSING` count and %; `ZERO` count and %; duplicates removed; negative readings
and your decision on them.

---

## Step 3 · Silver schema

```sql
CREATE TABLE glue_catalog.grid_db.silver_readings (
    meter_id         STRING,
    reading_ts       TIMESTAMP,
    reading_ts_raw   TIMESTAMP,
    drift_seconds    INT,
    energy_kwh       DOUBLE,
    reading_status   STRING,
    block            STRING,
    acorn_group      STRING,
    tariff           STRING,
    _updated_at      TIMESTAMP
) USING iceberg
PARTITIONED BY (days(reading_ts), bucket(32, meter_id))
TBLPROPERTIES (
    'format-version'               = '2',
    'write.target-file-size-bytes' = '268435456',
    'write.distribution-mode'      = 'hash'
);

ALTER TABLE glue_catalog.grid_db.silver_readings
  WRITE ORDERED BY block, meter_id, reading_ts;
```

**The sort order is the prerequisite for step 6.** Set it now; you will see why
when the `sort` strategy runs. Justify 256 MB target against the nightly query
(one block, one day) versus the default.

`bucket(32, meter_id)` co-locates a meter's series — the outage window in step 8
needs that. Justify 32 against 5,500 meters now and 500,000 in production.

---

## Step 4 · Weather join — grain is a decision

Weather is hourly; readings are half-hourly. Two half-hour readings share one
weather row.

```python
weather = (spark.table("glue_catalog.grid_db.bronze_weather")
    .withColumn("weather_hour", F.date_trunc("hour", F.to_timestamp("time")))
    .select("weather_hour", "temperature", "humidity", "windSpeed", "precipType"))

joined = (clean
    .withColumn("reading_hour", F.date_trunc("hour", "reading_ts"))
    .join(F.broadcast(weather), F.col("reading_hour") == F.col("weather_hour"), "left"))
```

Weather is ~20k rows — broadcast it and delete the shuffle. Confirm from the
physical plan that you got a `BroadcastHashJoin`, and paste it. State the
threshold property that governs automatic broadcast and at what size this table
would exceed it.

Report how many readings have no weather match (gaps in the weather file exist)
and what you did with them.

---

## Step 5 · Baseline query cost — the "before"

Run these against **silver, before any compaction**, and record runtime and bytes
scanned for each:

```sql
-- nightly: one block, one day
SELECT meter_id, sum(energy_kwh) AS kwh, count(*) AS intervals,
       sum(CASE WHEN reading_status = 'MISSING' THEN 1 ELSE 0 END) AS missing
FROM glue_catalog.grid_db.silver_readings
WHERE block = 'block_7' AND reading_ts >= TIMESTAMP '2013-06-01' AND reading_ts < TIMESTAMP '2013-06-02'
GROUP BY meter_id;

-- one meter, one month
SELECT reading_ts, energy_kwh FROM glue_catalog.grid_db.silver_readings
WHERE meter_id = 'MAC000002' AND reading_ts >= TIMESTAMP '2013-06-01' AND reading_ts < TIMESTAMP '2013-07-01';

-- all meters, one hour
SELECT block, sum(energy_kwh) FROM glue_catalog.grid_db.silver_readings
WHERE reading_ts >= TIMESTAMP '2013-06-15 18:00:00' AND reading_ts < TIMESTAMP '2013-06-15 19:00:00'
GROUP BY block;
```

Three query shapes, because compaction strategies favour different ones. The
comparison in step 6 runs all three.

---

## Step 6 · The compaction experiment — measured, not assumed

Make **three copies** of silver from the same source data. Compact each with a
different strategy. Run all three step-5 queries against each.

```sql
-- copy A: binpack
CALL glue_catalog.system.rewrite_data_files(
    table    => 'grid_db.silver_readings_binpack',
    strategy => 'binpack',
    options  => map('target-file-size-bytes', '268435456',
                    'partial-progress-enabled', 'true'));

-- copy B: sort
CALL glue_catalog.system.rewrite_data_files(
    table      => 'grid_db.silver_readings_sort',
    strategy   => 'sort',
    sort_order => 'block ASC, meter_id ASC, reading_ts ASC',
    options    => map('target-file-size-bytes', '268435456'));

-- copy C: z-order — for the multi-dimensional query shape
CALL glue_catalog.system.rewrite_data_files(
    table      => 'grid_db.silver_readings_zorder',
    strategy   => 'sort',
    sort_order => 'zorder(meter_id, reading_ts)',
    options    => map('target-file-size-bytes', '268435456'));
```

| | Before | binpack | sort | z-order |
|---|---|---|---|---|
| Data files | | | | |
| Avg file size | | | | |
| Compaction wall-clock | | | | |
| Q-nightly runtime / bytes | | | | |
| Q-meter-month runtime / bytes | | | | |
| Q-all-meters-hour runtime / bytes | | | | |

> **The sort strategy has a prerequisite you may have skipped.** If the table
> has no declared sort order and you did not pass `sort_order`, it does not do
> what you expect. Try it on a copy without step 3's `WRITE ORDERED BY`, record
> what happened, then fix it. The failed attempt is a deliverable.

Then argue, from the table, which strategy wins **for which query shape** —
and why no single one wins all three.

Also verify manifests:

```sql
CALL glue_catalog.system.rewrite_manifests('grid_db.silver_readings_sort');
SELECT count(*), sum(length) FROM glue_catalog.grid_db.silver_readings_sort.manifests;
```

---

## Step 7 · Maintenance defaults and the CFO's answer

Research and report, with the source you read:

| Operation | Scope (table / bucket) | Default | Range |
|---|---|---|---|
| Compaction target file size | | | |
| Minimum snapshots retained | | | |
| Maximum snapshot age | | | |
| Unreferenced file grace period | | | |
| Noncurrent-object deletion delay | | | |

If you are on **S3 Tables**, these run automatically and the values above govern
them; if you are on a general-purpose bucket, you schedule them yourself:

```sql
CALL glue_catalog.system.expire_snapshots(
    table => 'grid_db.silver_readings', older_than => TIMESTAMP '2026-08-01', retain_last => 20);
CALL glue_catalog.system.remove_orphan_files(
    table => 'grid_db.silver_readings', dry_run => true);
```

Then answer, with numbers:

- **How far back can you time travel right now**, and which setting determines it?
- **When is storage actually released** after a snapshot expires? Under managed
  defaults there is a second wait after expiry. Name it.
- **Fola's answer:** in one paragraph, where the storage cost came from (670
  snapshots, each pinning its own tiny files; expired-but-not-yet-deleted
  objects; metadata growth) and which levers reduce it.

---

## Step 8 · Gold — load, and the outage engine

### 8a · `gold_load_hourly`

```python
hourly = (silver
    .withColumn("hour", F.date_trunc("hour", "reading_ts"))
    .groupBy("meter_id", "block", "acorn_group", "tariff", "hour")
    .agg(F.sum("energy_kwh").alias("kwh"),
         F.count(F.when(F.col("reading_status") == "OK", 1)).alias("ok_intervals"),
         F.count("*").alias("total_intervals"))
    .withColumn("completeness_pct", F.col("ok_intervals") / 2.0))   # 2 half-hours per hour
```

`completeness_pct` is what lets operations tell a low-consumption hour from an
incomplete one. It must be on every row.

### 8b · `gold_outage_events` — Q1

```python
GRID = 1800   # seconds

w_m = Window.partitionBy("meter_id").orderBy("reading_ts")

# expected grid per meter, from first to last reading it ever gave
grid = (silver.groupBy("meter_id", "block")
    .agg(F.min("reading_ts").alias("first_ts"), F.max("reading_ts").alias("last_ts"))
    .withColumn("slot", F.explode(F.sequence("first_ts", "last_ts", F.expr("INTERVAL 30 MINUTES")))))

present = silver.where("reading_status <> 'MISSING'").select("meter_id", F.col("reading_ts").alias("slot"))

silent = (grid.join(present, ["meter_id", "slot"], "left_anti")   # slots with no reading
    .withColumn("prev_slot", F.lag("slot").over(Window.partitionBy("meter_id").orderBy("slot")))
    .withColumn("new_run",
        F.when(F.col("prev_slot").isNull() |
               (F.col("slot").cast("long") - F.col("prev_slot").cast("long") > GRID), 1).otherwise(0))
    .withColumn("run_id", F.sum("new_run").over(
        Window.partitionBy("meter_id").orderBy("slot").rowsBetween(Window.unboundedPreceding, 0))))

outages = (silent.groupBy("meter_id", "block", "run_id")
    .agg(F.min("slot").alias("outage_start"), F.max("slot").alias("outage_end"),
         F.count("*").alias("silent_intervals"))
    .where("silent_intervals >= 3")
    .withColumn("duration_min", F.col("silent_intervals") * 30))
```

**Bounding by `first_ts`/`last_ts` per meter** is what stops pre-install and
post-decommission silence being counted as outages. Say so.

Then the blast radius:

```python
w_blk = Window.partitionBy("block")
clustered = (outages.alias("a").join(outages.alias("b"),
        (F.col("a.block") == F.col("b.block")) & (F.col("a.meter_id") != F.col("b.meter_id")) &
        (F.col("b.outage_start") <= F.col("a.outage_end")) &
        (F.col("b.outage_end")   >= F.col("a.outage_start")))
    .groupBy("a.meter_id", "a.run_id")
    .agg(F.countDistinct("b.meter_id").alias("concurrent_meters")))

events = (outages.join(clustered, ["meter_id", "run_id"], "left")
    .fillna({"concurrent_meters": 0})
    .withColumn("classification",
        F.when(F.col("concurrent_meters") >= 5, "CLUSTERED")   # justify 5
         .otherwise("ISOLATED")))
```

Report: events total; isolated vs clustered; largest cluster and its block;
duration distribution. **Then the honest paragraph** — what your rule cannot
distinguish: comms failure from power failure, scheduled maintenance from a
fault, a meter already dark before your window opened.

### 8c · `gold_weather_response` — Q3

Per `acorn_group`, per temperature bin (1 °C), mean hourly kWh. Fit the two
sides of the U separately — find the minimum-demand temperature per group, then
slope below and above it. Report the inflection temperature per group and
whether ToU-tariff households differ.

---

## Step 9 · Continued ingestion — does it stay fixed?

Simulate **one more day** of micro-batches (48 commits) into the
**sort-compacted** table. Then:

```sql
SELECT count(*) AS files, cast(avg(file_size_in_bytes) AS BIGINT) AS avg_bytes
FROM glue_catalog.grid_db.silver_readings_sort.files WHERE content = 0;
```

Run immediately after the 48 commits, then after maintenance. Record the
degradation: files added per day, average size drop, and re-run the nightly
query for runtime and bytes.

**Derive a compaction cadence from your measured rate.** If the nightly query
crosses 2× its post-compaction runtime after N hours of ingestion, your cadence
is less than N. Say what N is for your data.

### Redshift

```sql
CREATE EXTERNAL SCHEMA spectrum_grid
FROM DATA CATALOG DATABASE 'grid_db'
IAM_ROLE 'arn:aws:iam::<ACCOUNT>:role/RedshiftLakehouseRole' REGION 'us-east-1';

CREATE MATERIALIZED VIEW mv_outage_ops AS
SELECT block, classification, count(*) AS events,
       sum(duration_min) AS total_minutes, max(concurrent_meters) AS largest_cluster
FROM spectrum_grid.gold_outage_events
WHERE outage_end >= dateadd(day, -7, current_date)
GROUP BY 1, 2;
```

Operations wants `mv_outage_ops` fresh every **60 seconds**. Measure the same
outage query three ways — Athena on Iceberg, Spectrum, MV — and state which
path can support 60-second refresh and what breaks first at 10 seconds.

---

## Step 10 · Reporting and validation

```sql
-- Q1
SELECT classification, count(*) AS events,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY duration_min) AS median_min,
       max(concurrent_meters) AS worst_cluster
FROM spectrum_grid.gold_outage_events GROUP BY 1;

-- Q3
SELECT acorn_group, inflection_temp_c, slope_below, slope_above
FROM spectrum_grid.gold_weather_response ORDER BY inflection_temp_c;
```

Q2 is the before/after/after-one-more-day table from steps 5, 6 and 9, presented
with the derived cadence.

---

# SUGGESTED 3-DAY PLAN

| Day | Steps | Checkpoint |
|---|---|---|
| 1 | 1–3 | ~75k micro-batch files landed via ~670 commits; baseline table filled; silver DDL with sort order |
| 2 | 4–7 | Weather broadcast plan pasted; three-strategy comparison filled; failed sort attempt recorded; maintenance defaults sourced |
| 3 | 8–10 | Outage events classified; degradation re-measured; cadence derived; Redshift 60-second verdict |

---

# ACCEPTANCE CRITERIA

| # | Criterion | Pass condition |
|---|---|---|
| A1 | Micro-batch simulator | ≥ 14 days × 48 × 112 blocks, one commit per half hour |
| A2 | **Baseline** | All metadata-table measurements recorded; nightly query before-number captured |
| A3 | Per-file cost | Three distinct costs named |
| A4 | Missing vs zero | Separate `reading_status` values, preserved through gold |
| A5 | Weather join | `BroadcastHashJoin` shown in plan; threshold property named |
| A6 | **Compaction comparison** | Three strategies, three query shapes, table fully populated |
| A7 | **Failed sort attempt** | Recorded with cause and fix |
| A8 | Maintenance defaults | Table filled with source; time-travel horizon and storage-release timing answered |
| A9 | Q1 | Isolated/clustered classification; blast radius; limitations paragraph |
| A10 | **Degradation** | Files and runtime after one more day; cadence derived from measurement |
| A11 | Q3 | Inflection temperature per acorn group |
| A12 | Redshift | Three-way measurement; 60-second verdict with reasoning |

---

# EVIDENCE PACK

| # | Item | Value |
|---|---|---|
| 1 | EMR cluster / Serverless application ID | |
| 2 | Storage type: general-purpose S3 or S3 Tables | |
| 3 | Micro-batch files written; commits; simulated days | |
| 4 | **Baseline:** data files, avg / median / min size, snapshots, manifests | |
| 5 | Nightly query before: runtime, bytes | |
| 6 | Drift: readings off-grid, max drift seconds | |
| 7 | `MISSING` count and %; `ZERO` count and %; duplicates removed | |
| 8 | Weather join: plan excerpt showing broadcast; unmatched readings | |
| 9 | **Compaction table — all 4 columns × 6 rows** | |
| 10 | Failed sort attempt: what happened, what fixed it | |
| 11 | Target file size used / default / range | |
| 12 | Manifests before / after rewrite | |
| 13 | Maintenance defaults table with source | |
| 14 | Time-travel horizon now; storage-release timing | |
| 15 | Outages: total, isolated, clustered, largest cluster + block | |
| 16 | After +1 day: files, avg size, nightly runtime; derived cadence | |
| 17 | Q3: inflection temp per acorn group | |
| 18 | Athena / Spectrum / MV: runtime + bytes, outage query | |

**Break log.** Three genuine failures with real error text. One must be the
failed sort-strategy attempt.

---

# TECHNICAL REVIEW (45 min, live)

1. Show `files` for bronze before compaction. Read me the median file size and
   tell me what wrote it.
2. Name the three costs of a small file.
3. Show the weather join plan. Which node proves it was broadcast?
4. Your sort compaction failed first. What was missing?
5. binpack vs sort vs z-order — which won the meter-month query, and why?
6. Which maintenance settings are per table, which per bucket, and why does that
   matter operationally?
7. How far back can you time travel right now? Which number sets that?
8. After one more day of ingestion, at what rate did the table degrade? What
   cadence did you derive?
9. Meter M has no readings for two hours. Outage, comms failure, or
   decommissioned? What does your pipeline say?
10. Fola asks why the bill went up. Two sentences, with the mechanism.
11. Operations wants 60-second refresh. Which path, and what breaks at 10?
12. At 500,000 meters, which of your windows breaks first?

---

# NOTES

- LLM assistance is expected. Assessment is on judgement, measurement and
  defence. Every number must be reproducible from your account.
- **Create the small-file problem first.** Loading the 112 block files directly
  forfeits the baseline, the compaction comparison, and the degradation
  measurement — most of the project.
- `energy(kWh/hh)` is a string containing `Null`. Handle it as the
  missing-vs-zero problem, not as a parsing nuisance.
- Stop EMR at the end of each day. Budget alarm first.
