# PROJECT 3 — ICU Patient Telemetry, Merge-on-Read & Provable Erasure

**Stack:** AWS EMR (PySpark) · Apache Iceberg · AWS Glue Data Catalog · Amazon Redshift
**Duration:** 3 days · **Level:** senior data engineer

You are building the clinical analytics layer for a hospital group. Bedside
monitors write vital signs continuously. Two obligations pull against each
other: the ICU wants early-warning scores that depend on complete history, and
the Data Protection Officer must prove — not assert — that an erased patient is
gone within 30 days.

Three engineering problems dominate this build:

1. **The data is long-format and irregular.** One row per measurement, not per
   timestamp. Pivoting and resampling without inventing readings is the core
   transformation.
2. **Row-level deletes at scale.** Erasure means deleting one patient's rows from
   every layer. Copy-on-write rewrites whole files for that; merge-on-read
   writes delete files that accumulate read cost. You must measure both.
3. **History is the feature and the liability.** Iceberg keeps every snapshot.
   Proving erasure means proving the snapshots are gone too — and knowing what
   silently stops that from happening.

---

# THE THREE QUESTIONS

### Q1 · Six-hour deterioration lead
> For every ICU stay that ended in death or an escalation event, what did the
> vitals look like in the **6 hours before** it, hour by hour — and which
> single vital crossed its threshold **first**?

The answer is a table the ward can act on: at what lead time, and on which
signal, could this have been flagged. The hard part is that readings are
irregular, so "hour by hour" requires a resample that does not fabricate values
across monitor disconnections.

### Q2 · Erasure attestation
> After patient `X` exercises the right to erasure, produce a document the DPO
> can file: what was deleted, from which tables, verified how, and **the exact
> point in time at which the data became unrecoverable** — including through
> time travel.

"Deleted from the current snapshot" is not erasure. Iceberg retains history by
design. You must show the pre-erasure snapshots are expired and their files
released, and you must know the conditions under which that expiry silently
stops running.

### Q3 · Completeness and fabrication risk
> For every ICU stay, what fraction of hourly buckets are **genuinely
> observed**, what fraction would a naive forward-fill have **invented**, and
> what fraction of raw readings are **sensor artefacts** rather than physiology?

This is the question that decides whether Q1 can be trusted. A model trained on
forward-filled data across a two-hour monitor gap has learned nothing about the
patient.

---

# DATASET

**MIMIC-III Clinical Database Demo v1.4** (PhysioNet, open access, ~100
patients). Read and accept its data-use terms; state in your report that you did.
**Do not use the full credentialed dataset for this project.**

You will use five tables. Confirm exact column names on download — the demo
uses uppercase `SUBJECT_ID`, `HADM_ID`, `ICUSTAY_ID`.

| Table | Grain | Columns you need |
|---|---|---|
| `CHARTEVENTS` | one row per measurement | `SUBJECT_ID`, `HADM_ID`, `ICUSTAY_ID`, `ITEMID`, `CHARTTIME`, `VALUE`, `VALUENUM`, `VALUEUOM`, `ERROR` |
| `D_ITEMS` | one row per `ITEMID` | `ITEMID`, `LABEL`, `CATEGORY`, `UNITNAME` |
| `ICUSTAYS` | one row per ICU stay | `ICUSTAY_ID`, `INTIME`, `OUTTIME`, `FIRST_CAREUNIT` |
| `ADMISSIONS` | one row per admission | `HADM_ID`, `ADMITTIME`, `DISCHTIME`, `DEATHTIME`, `HOSPITAL_EXPIRE_FLAG` |
| `PATIENTS` | one row per patient | `SUBJECT_ID`, `GENDER`, `DOB`, `DOD` |

`CHARTEVENTS` in the demo is roughly 750k rows in **long format** — one row per
reading, identified by `ITEMID`. The vitals you need, by MetaVision `ITEMID`:

| Vital | ITEMID(s) | Unit note |
|---|---|---|
| Heart rate | 220045 | bpm |
| SpO2 | 220277 | % |
| Systolic BP (non-invasive) | 220179 | mmHg |
| Diastolic BP (non-invasive) | 220180 | mmHg |
| Respiratory rate | 220210 | breaths/min |
| Temperature | 223761 (°F), 223762 (°C) | **two ITEMIDs, two units** |

Some demo patients are on the older CareVue system with different `ITEMID`s
(e.g. 211 for heart rate). Use `D_ITEMS.LABEL` to find them, and document the
mapping you built. **Missing a system means silently dropping patients.**

### Known dirt — handle all of it

| Problem | Reality |
|---|---|
| Long format | Pivot to one row per `(ICUSTAY_ID, CHARTTIME)` — but readings for different vitals rarely share an exact timestamp |
| Irregular cadence | Gaps from seconds to hours. Compute median and p95 gap per stay; never assume a fixed interval |
| `ERROR = 1` rows | The source system flagged them. Decide: exclude, or keep with a flag |
| Physiologically impossible values | HR 0 or 300, SpO2 > 100, temp 0. Sensor artefacts — but a disconnected lead is itself clinical information. Flag, do not delete |
| Temperature in two units | 223761 is °F, 223762 is °C. Normalise before you aggregate anything |
| Duplicate `(ICUSTAY_ID, ITEMID, CHARTTIME)` | Same reading charted twice. Deterministic dedup |
| Dates are shifted | MIMIC shifts all dates into 2100–2200 for de-identification. Do not "fix" this; it is intentional |

### Erasure request log — you must create it

Real erasure requests do not ship with the dataset. Generate
`s3://<bucket>/raw/erasure_requests/` containing **5 `SUBJECT_ID`s** with a
`request_ts` each. At least one request must arrive **after** the gold tables
for that patient have already been computed — that is the hard case.

---

# ARCHITECTURE

```
s3://raw/mimic/                    CHARTEVENTS, D_ITEMS, ICUSTAYS, ADMISSIONS, PATIENTS
s3://raw/erasure_requests/
        │  Step 1
bronze_chartevents                 long format, as received, ERROR flag kept
bronze_icustays · bronze_admissions · bronze_patients
        │  Step 2  dedup, unit normalisation, plausibility flags
        │  Step 4  pivot long → wide
silver_vitals                      one row per (icustay_id, charttime), MoR, bucket(16, subject_id)
silver_patient_dim                 PHI separated, tokenised
        │  Step 5  gap-bounded hourly resample
silver_vitals_hourly
        │  Step 6
gold_deterioration                 6h lead features per stay, per hour
gold_ward_summary                  de-identified aggregates, k ≥ 5
        │  Step 8  erasure runner + attestation
        │  Step 9
Redshift Spectrum                  de-identified marts only
```

---

# THE TEN STEPS

## Step 1 · Bronze ingestion — EMR + PySpark

```python
from pyspark.sql import functions as F, types as T

schema = T.StructType([
    T.StructField("ROW_ID",     T.LongType()),
    T.StructField("SUBJECT_ID", T.LongType()),
    T.StructField("HADM_ID",    T.LongType()),
    T.StructField("ICUSTAY_ID", T.LongType()),
    T.StructField("ITEMID",     T.LongType()),
    T.StructField("CHARTTIME",  T.TimestampType()),
    T.StructField("STORETIME",  T.TimestampType()),
    T.StructField("CGID",       T.LongType()),
    T.StructField("VALUE",      T.StringType()),
    T.StructField("VALUENUM",   T.DoubleType()),
    T.StructField("VALUEUOM",   T.StringType()),
    T.StructField("WARNING",    T.IntegerType()),
    T.StructField("ERROR",      T.IntegerType()),
    T.StructField("RESULTSTATUS", T.StringType()),
    T.StructField("STOPPED",    T.StringType()),
])

chart = (spark.read.schema(schema).option("header", True)
    .csv(f"s3://{BUCKET}/raw/mimic/CHARTEVENTS.csv")
    .withColumn("_src_file",    F.input_file_name())
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_batch_id",    F.lit(BATCH_ID))
    .withColumn("chart_date",   F.to_date("CHARTTIME")))

chart.writeTo("glue_catalog.clinical_db.bronze_chartevents").overwritePartitions()
```

Declare the schema. Inference on 750k mixed-type rows will type `VALUE` wrong
and cost you the step.

```sql
CREATE TABLE glue_catalog.clinical_db.bronze_chartevents (
    row_id        BIGINT,
    subject_id    BIGINT,
    hadm_id       BIGINT,
    icustay_id    BIGINT,
    itemid        BIGINT,
    charttime     TIMESTAMP,
    storetime     TIMESTAMP,
    value         STRING,
    valuenum      DOUBLE,
    valueuom      STRING,
    error         INT,
    _src_file     STRING,
    _ingested_at  TIMESTAMP,
    _batch_id     STRING,
    chart_date    DATE
) USING iceberg
PARTITIONED BY (chart_date)
TBLPROPERTIES (
    'format-version'                = '2',
    'write.parquet.compression-codec' = 'zstd'
);
```

Load the four small reference tables the same way, unpartitioned.

**Deliverable:** row counts per table; distinct `ITEMID` count; the `D_ITEMS`
join that maps every vital `ITEMID` you will use, including CareVue-era ones.

---

## Step 2 · Deduplication, units, and plausibility — EMR + PySpark

```python
from pyspark.sql.window import Window

VITALS = {  # itemid -> (name, unit_fix)
    220045: ("heart_rate", None), 211: ("heart_rate", None),
    220277: ("spo2", None),       646: ("spo2", None),
    220179: ("sbp", None),        455: ("sbp", None),
    220180: ("dbp", None),        8441: ("dbp", None),
    220210: ("resp_rate", None),  618: ("resp_rate", None),
    223762: ("temp_c", None),     676: ("temp_c", None),
    223761: ("temp_c", "F"),      678: ("temp_c", "F"),
}
mapping = spark.createDataFrame(
    [(k, v[0], v[1]) for k, v in VITALS.items()], ["itemid", "vital", "unit_fix"])

w_dup = (Window.partitionBy("icustay_id", "itemid", "charttime")
               .orderBy(F.col("storetime").desc(), F.col("row_id").desc()))

clean = (spark.table("glue_catalog.clinical_db.bronze_chartevents")
    .join(F.broadcast(mapping), "itemid")
    .withColumn("_rn", F.row_number().over(w_dup)).where("_rn = 1").drop("_rn")
    .withColumn("value_norm",
        F.when(F.col("unit_fix") == "F", (F.col("valuenum") - 32) * 5 / 9)
         .otherwise(F.col("valuenum")))
    .withColumn("plausible",
        F.when(F.col("vital") == "heart_rate", F.col("value_norm").between(20, 250))
         .when(F.col("vital") == "spo2",       F.col("value_norm").between(50, 100))
         .when(F.col("vital") == "sbp",        F.col("value_norm").between(40, 260))
         .when(F.col("vital") == "dbp",        F.col("value_norm").between(20, 160))
         .when(F.col("vital") == "resp_rate",  F.col("value_norm").between(4, 60))
         .when(F.col("vital") == "temp_c",     F.col("value_norm").between(30, 43))
         .otherwise(F.lit(None)))
    .withColumn("source_error", F.col("error") == 1))
```

Deduplicate on `storetime` then `row_id` — the most recently stored charting of
the same reading wins, and `row_id` breaks ties deterministically.

**Plausibility ranges are yours to justify.** Cite a clinical source or state
your reasoning. **Flag, never delete.** A heart rate of 0 is either a lead
disconnection or a cardiac arrest; your pipeline cannot know which, and dropping
it destroys the evidence either way.

**Deliverable:** rows in → rows out; implausible count per vital; `ERROR = 1`
count and your decision on it; how many °F readings were converted.

---

## Step 3 · Silver schema — the write-mode decision

```sql
CREATE TABLE glue_catalog.clinical_db.silver_vitals (
    subject_id     BIGINT,
    hadm_id        BIGINT,
    icustay_id     BIGINT,
    charttime      TIMESTAMP,
    heart_rate     DOUBLE,
    spo2           DOUBLE,
    sbp            DOUBLE,
    dbp            DOUBLE,
    resp_rate      DOUBLE,
    temp_c         DOUBLE,
    hr_plausible   BOOLEAN,
    spo2_plausible BOOLEAN,
    sbp_plausible  BOOLEAN,
    gap_minutes    DOUBLE,
    _updated_at    TIMESTAMP
) USING iceberg
PARTITIONED BY (days(charttime), bucket(16, subject_id))
TBLPROPERTIES (
    'format-version'               = '2',
    'write.delete.mode'            = 'merge-on-read',
    'write.update.mode'            = 'merge-on-read',
    'write.merge.mode'             = 'merge-on-read',
    'write.target-file-size-bytes' = '134217728',
    'write.distribution-mode'      = 'hash'
);

ALTER TABLE glue_catalog.clinical_db.silver_vitals
  WRITE ORDERED BY subject_id, charttime;
```

**Set the write modes explicitly.** Do not rely on defaults. Then write the
**one-page decision memo** — before step 7 measures anything — covering:

- read cost and write cost under each mode
- what each means for a 30-day erasure SLA when a patient has 40,000 rows across
  200 files
- what each means for Dr Aduba's query latency at 07:00 on the ward round

`bucket(16, subject_id)` co-locates a patient's rows, which is what both the
resample and the erasure need. Justify 16 against ~100 patients now and 10,000
in production.

---

## Step 4 · Pivot long → wide — the shape transformation

The raw data has one row per reading. Q1 needs one row per timestamp with all
vitals as columns. Different vitals rarely share an exact `charttime`, so a naive
pivot produces a sparse matrix.

```python
wide = (clean
    .groupBy("subject_id", "hadm_id", "icustay_id", "charttime")
    .pivot("vital", ["heart_rate", "spo2", "sbp", "dbp", "resp_rate", "temp_c"])
    .agg(F.first("value_norm"))
    .withColumn("gap_minutes",
        (F.col("charttime").cast("long")
         - F.lag("charttime").over(
             Window.partitionBy("icustay_id").orderBy("charttime")).cast("long")) / 60.0))
```

Report the sparsity: what fraction of wide rows have all six vitals? Three?
One? This number tells you how much step 5's resample has to do.

Also compute and record, per stay: **median gap and p95 gap** between
consecutive rows. Step 5's threshold comes from this.

---

## Step 5 · Gap-bounded hourly resample — do not invent data

Dr Aduba needs hourly features. The data is irregular. You have three options:
last-observation-carried-forward, interpolation, or leave the bucket null.

> **Rule: never carry a value forward across a gap longer than your p95.**
> State the threshold, implement it, and count the buckets left null.

```python
P95_MIN = 240   # from step 4 — replace with your measured value

hourly_grid = (icustays
    .withColumn("hour", F.explode(F.sequence(
        F.date_trunc("hour", "intime"), F.date_trunc("hour", "outtime"),
        F.expr("INTERVAL 1 HOUR"))))
    .select("icustay_id", "subject_id", "hour"))

w_locf = (Window.partitionBy("icustay_id").orderBy("charttime")
                .rowsBetween(Window.unboundedPreceding, Window.currentRow))

carried = (wide
    .withColumn("hr_last", F.last("heart_rate", ignorenulls=True).over(w_locf))
    .withColumn("hr_last_ts",
        F.last(F.when(F.col("heart_rate").isNotNull(), F.col("charttime")),
               ignorenulls=True).over(w_locf))
    # ... repeat per vital
)

hourly = (hourly_grid.alias("g")
    .join(carried.alias("c"),
          (F.col("g.icustay_id") == F.col("c.icustay_id")) &
          (F.col("c.charttime") <= F.col("g.hour") + F.expr("INTERVAL 1 HOUR")) &
          (F.col("c.charttime") >  F.col("g.hour")), "left")
    .groupBy("g.icustay_id", "g.subject_id", "g.hour")
    .agg(F.avg("heart_rate").alias("hr_mean"),
         F.min("heart_rate").alias("hr_min"),
         F.max("heart_rate").alias("hr_max"),
         F.count("heart_rate").alias("hr_obs"),
         F.max("hr_last").alias("hr_carried"),
         F.max("hr_last_ts").alias("hr_carried_ts"))
    .withColumn("hr_final",
        F.when(F.col("hr_obs") > 0, F.col("hr_mean"))
         .when((F.col("hour").cast("long") - F.col("hr_carried_ts").cast("long")) / 60
               <= P95_MIN, F.col("hr_carried"))
         .otherwise(F.lit(None)))
    .withColumn("hr_source",
        F.when(F.col("hr_obs") > 0, "observed")
         .when(F.col("hr_final").isNotNull(), "carried")
         .otherwise("gap")))
```

Every hourly value carries a `*_source` column: `observed`, `carried`, or `gap`.
That column **is** the answer to Q3.

> The join above is a range join on time and will be slow at scale. For 100
> demo patients it is fine. Note in your report what you would change at 10,000
> patients — the grid-expansion pattern from Project 2 applies here too.

---

## Step 6 · Gold — deterioration features and de-identified summary

### 6a · `gold_deterioration` — Q1

Define an escalation event per stay: `DEATHTIME` within the stay, or
`HOSPITAL_EXPIRE_FLAG = 1`, or ICU readmission within 48h (derive from
`ICUSTAYS`). For each such stay, take the **6 hours before** the event.

```python
NEWS_LIKE = {  # threshold rule — document your source
    "hr":   lambda c: (c < 40) | (c > 130),
    "spo2": lambda c: c < 92,
    "sbp":  lambda c: c < 90,
    "rr":   lambda c: (c < 8) | (c > 24),
    "temp": lambda c: (c < 35.0) | (c > 39.0),
}

lead = (hourly.join(events, "icustay_id")
    .where(F.col("hour").between(
        F.col("event_ts") - F.expr("INTERVAL 6 HOURS"), F.col("event_ts")))
    .withColumn("lead_hours",
        (F.col("event_ts").cast("long") - F.col("hour").cast("long")) / 3600)
    .withColumn("hr_breach",   NEWS_LIKE["hr"](F.col("hr_final")))
    .withColumn("spo2_breach", NEWS_LIKE["spo2"](F.col("spo2_final")))
    # ...
    .withColumn("any_breach", F.col("hr_breach") | F.col("spo2_breach") | ...)
)

first_breach = (lead.where("any_breach")
    .groupBy("icustay_id")
    .agg(F.max("lead_hours").alias("earliest_lead_h"),
         F.first("first_breaching_vital").alias("first_signal")))
```

**Only count a breach on an `observed` or `carried` value, never a `gap`.**
Report the distribution of `earliest_lead_h` and which vital breaches first most
often. State what your rule structurally misses.

### 6b · `gold_ward_summary` — de-identified

Aggregate by `FIRST_CAREUNIT` and hour of day. **Minimum group size k = 5**;
suppress any cell below it. No `subject_id`, no dates finer than hour-of-day.
State how you verified no cell is re-identifiable.

---

## Step 7 · CoW vs MoR, measured — and the compaction finding

Build **two copies** of `silver_vitals` from the same data: one with
`copy-on-write` on all three modes, one `merge-on-read`. Same partitioning.

Run the **same 10 single-patient deletes** on each, one at a time:

```sql
DELETE FROM glue_catalog.clinical_db.silver_vitals_cow WHERE subject_id = 10006;
DELETE FROM glue_catalog.clinical_db.silver_vitals_mor WHERE subject_id = 10006;
```

Record after each:

```sql
SELECT content, count(*) AS files, cast(avg(file_size_in_bytes) AS BIGINT) AS avg_bytes
FROM glue_catalog.clinical_db.silver_vitals_mor.files
GROUP BY content;
-- content 0 = data, 1 = position delete, 2 = equality delete
```

| | CoW | MoR |
|---|---|---|
| Data files before | | |
| Data files after 10 deletes | | |
| Position delete files after | | |
| Wall-clock for 10 deletes | | |
| `count(*)` runtime after | | |
| Bytes scanned, full read, after | | |

Then compact the MoR copy:

```sql
CALL glue_catalog.system.rewrite_data_files(
    table => 'clinical_db.silver_vitals_mor');
```

Re-query `files`. **Record the delete-file count.**

> **It will almost certainly not have changed.** A data file is not eligible for
> rewrite just because delete files point at it — binpack only rewrites files it
> considers badly sized. Find the option that makes those files eligible, run it,
> re-measure. **Report both the no-op and the fix.** This is the most important
> finding in the project, because a production MoR table with a "scheduled
> compaction" that reconciles nothing has read cost growing forever.

---

## Step 8 · Erasure — execution and attestation

For each of the 5 requests:

```python
for req in erasure_requests.collect():
    sid = req.subject_id
    before = {t: spark.sql(f"SELECT count(*) FROM {t} WHERE subject_id = {sid}").first()[0]
              for t in ALL_TABLES}
    snap_before = spark.sql(f"SELECT max(snapshot_id) FROM {SILVER}.snapshots").first()[0]

    for t in ALL_TABLES:
        spark.sql(f"DELETE FROM {t} WHERE subject_id = {sid}")

    after = {t: spark.sql(f"SELECT count(*) FROM {t} WHERE subject_id = {sid}").first()[0]
             for t in ALL_TABLES}
    snap_after = spark.sql(f"SELECT max(snapshot_id) FROM {SILVER}.snapshots").first()[0]

    # THE CRUX: is the patient still reachable via time travel?
    still_there = spark.sql(f"""
        SELECT count(*) FROM {SILVER} VERSION AS OF {snap_before}
        WHERE subject_id = {sid}""").first()[0]
```

`still_there` will be **non-zero**. That is Iceberg working correctly — and it
is exactly what the DPO will not accept. Now make it zero:

```sql
CALL glue_catalog.system.expire_snapshots(
    table       => 'clinical_db.silver_vitals',
    older_than  => current_timestamp(),
    retain_last => 1);

CALL glue_catalog.system.remove_orphan_files(
    table => 'clinical_db.silver_vitals', dry_run => false);
```

Re-run the `VERSION AS OF` query. It must now **fail**, not return zero rows.
Paste the error.

> ### The trap — snapshot expiry can be silently disabled
>
> If your tables are in an **S3 Tables bucket**, automated snapshot management
> **fails for the entire table** when either of these is true:
> - any user-defined **branch or tag** exists on the table
> - `history.expire.max-snapshot-age-ms` or `history.expire.min-snapshots-to-keep`
>   is set as a table property
>
> Nothing surfaces in Spark or Athena. Check the maintenance job status explicitly
> and paste the result. On a general-purpose bucket with manual `expire_snapshots`,
> tags still pin their snapshots — check `refs` for anything but `main`.
>
> **An erasure attestation that does not include this check is incomplete.**

Then answer the timing question for the DPO: after `DELETE`, how long until the
data is genuinely unrecoverable, and which settings determine it? Under managed
maintenance defaults the answer involves a snapshot age *plus* a noncurrent-days
wait; under manual expiry it is whenever you run it. Give a number and the
mechanism.

**Deliverable — the attestation**, one page per patient, filed as
`ATTESTATION_<subject_id>.md`: tables touched, rows before/after, snapshot IDs
before/after, the time-travel query and its failure, the expiry health check,
and the unrecoverability timestamp.

---

## Step 9 · Redshift — de-identified marts only

```sql
CREATE EXTERNAL SCHEMA spectrum_clinical
FROM DATA CATALOG
DATABASE 'clinical_db'
IAM_ROLE 'arn:aws:iam::<ACCOUNT>:role/RedshiftLakehouseRole'
REGION 'us-east-1';
```

**Expose only `gold_ward_summary` and `gold_deterioration` with identifiers
stripped.** Enforce it, do not just intend it: either build the external schema
over a Glue database containing only the de-identified tables, or use Lake
Formation column-level permissions to deny `subject_id` to the Redshift role.
Show the permission and show a query that fails because of it.

```sql
CREATE MATERIALIZED VIEW mv_ward_deterioration AS
SELECT care_unit, hour_of_day,
       count(*) AS stays, avg(earliest_lead_h) AS mean_lead_h,
       sum(CASE WHEN first_signal = 'spo2' THEN 1 ELSE 0 END) AS spo2_first
FROM spectrum_clinical.gold_deterioration
GROUP BY 1, 2;
```

Answer: **when patient X is erased on Monday, when does this mart stop
reflecting them?** Give the refresh cadence and the resulting lag in hours.

---

## Step 10 · Reporting and audit validation

```sql
-- Q1: lead time distribution and first-breaching signal
SELECT first_signal, count(*) AS stays,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY earliest_lead_h) AS median_lead_h
FROM spectrum_clinical.gold_deterioration GROUP BY 1 ORDER BY 2 DESC;

-- Q3: completeness — observed vs carried vs gap, per stay
SELECT hr_source, count(*) AS hourly_buckets,
       count(*)::float / sum(count(*)) OVER () AS share
FROM spectrum_clinical.silver_vitals_hourly_deid GROUP BY 1;
```

For Q2, present the five attestations. Then, live in review, pick one and re-run
its time-travel query to show it still fails.

---

# SUGGESTED 3-DAY PLAN

| Day | Steps | Checkpoint |
|---|---|---|
| 1 | 1–4 | Bronze loaded, ITEMID mapping complete incl. CareVue, wide table built, gap stats recorded |
| 2 | 5–7 | Hourly resample with `*_source` columns; CoW/MoR table populated; compaction finding captured |
| 3 | 8–10 | Five attestations filed; Redshift marts with enforced column restriction; review prep |

---

# ACCEPTANCE CRITERIA

| # | Criterion | Pass condition |
|---|---|---|
| A1 | ITEMID mapping | Covers both MetaVision and CareVue; documented with `D_ITEMS` evidence |
| A2 | Units | °F readings converted; count reported |
| A3 | Plausibility | Ranges justified; artefacts flagged not deleted |
| A4 | Pivot | Sparsity reported; gap median and p95 per stay recorded |
| A5 | Resample | No value carried past p95; `*_source` column present; null-bucket count reported |
| A6 | Q1 | Lead-time distribution + first-signal breakdown; breaches only on observed/carried |
| A7 | **CoW vs MoR** | Table fully populated from two real copies |
| A8 | **Compaction finding** | No-op run and working run both reported with delete-file counts |
| A9 | **Erasure** | 5 attestations; time-travel query fails after expiry; health check pasted |
| A10 | De-identification | k ≥ 5 enforced; Redshift cannot see `subject_id`, proven by a failing query |
| A11 | Q3 | Observed / carried / gap shares reported per stay |

---

# EVIDENCE PACK

| # | Item | Value |
|---|---|---|
| 1 | EMR cluster / Serverless application ID | |
| 2 | Storage type: general-purpose S3 or S3 Tables | |
| 3 | `CHARTEVENTS` rows loaded; distinct ITEMIDs; vitals ITEMIDs mapped | |
| 4 | Dedup: rows in → out; `ERROR = 1` count and decision | |
| 5 | Implausible readings per vital | |
| 6 | °F readings converted | |
| 7 | Wide-row sparsity: % with 6 / 3 / 1 vitals | |
| 8 | Gap median and p95 (minutes), overall and worst stay | |
| 9 | LOCF threshold used; hourly buckets left `gap` | |
| 10 | Q1: stays with an event; median `earliest_lead_h`; first-signal counts | |
| 11 | **CoW vs MoR table — all 7 rows** | |
| 12 | `content` values seen in `files` | |
| 13 | Delete files after plain compaction / after fix; the option used | |
| 14 | Erasure: 5 × (subject_id, rows before/after per table, snapshot before/after) | |
| 15 | Time-travel query result before expiry (rows) and after (error text) | |
| 16 | Snapshot-expiry health check: command and result | |
| 17 | Time-to-unrecoverable and the settings that determine it | |
| 18 | Redshift: the query that fails on `subject_id`, with error text | |
| 19 | Mart refresh cadence and erasure propagation lag | |

**Break log.** Three genuine failures with real error text. One must be the
compaction no-op. One must be the post-expiry time-travel failure.

---

# TECHNICAL REVIEW (45 min, live)

1. Show `D_ITEMS` for heart rate. How many ITEMIDs did you map, and what happens
   to a patient if you missed one?
2. Show me a wide row with three vitals and three nulls. Why are they null?
3. Your p95 gap is N minutes. Show me an hourly bucket you left `gap` and
   explain why filling it would have been clinically wrong.
4. Show the `files` metadata for the MoR copy. Read me every `content` value.
5. Your first compaction changed nothing. Why? What made it work?
6. Pick patient P from your attestations. Run the time-travel query now.
7. What exactly makes P's data unrecoverable, and how long did it take?
8. What did your snapshot-expiry health check return? What would have made it
   fail?
9. Run a query against Redshift selecting `subject_id`. Show me the error.
10. Heart rate 0 — lead disconnection or arrest? What does your pipeline do, and
    why is that the right call?
11. Dr Aduba wants six months of history; the DPO wants 30-day erasure. Where
    exactly do they conflict in your design?
12. At 10,000 patients, which step breaks first?

---

# NOTES

- LLM assistance is expected. Assessment is on judgement, measurement and
  defence. Every number must be reproducible from your account.
- Demo dataset only. Read and follow its terms; say so in the report.
- Dates are intentionally shifted to 2100+. Do not correct them.
- The CoW/MoR measurement must come from two real table copies in your account.
- Stop EMR at the end of each day. Budget alarm first.
