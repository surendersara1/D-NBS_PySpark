# PROJECT 1 — FinTech Transaction Ledger & CDC Engine

**Stack:** AWS EMR (PySpark) · Apache Iceberg · AWS Glue Data Catalog · Amazon Redshift
**Duration:** 3 days · **Level:** senior data engineer

You are building the ledger layer for a payments platform. The source is an OLTP
core banking system you cannot query. It emits Debezium-style CDC onto S3. Your
job is a lakehouse that survives late-arriving adjustments, re-runs, and an
auditor asking about a balance from eight months ago.

---

# THE THREE QUESTIONS

Everything you build exists to answer these. They are the acceptance test.

### Q1 · Historical point-in-time audit
> What was the exact settled balance for account `X` at timestamp `T` — say
> 23:59:59 last Friday — **accounting for chargebacks and updates that arrived
> days after the transactions occurred?**

The hard part is not time travel. It is that the answer changes depending on
whether you mean *"the balance as we understood it on Friday"* or *"the balance
as we now know Friday's to have been."* Both are legitimate; they differ by the
late adjustments. **You must produce both numbers and label them.**

### Q2 · Rapid outflow / fraud velocity
> Which accounts moved cumulative withdrawals plus transfers exceeding **80% of
> their starting daily balance** inside any **rolling 15-minute window**?

Rolling, not tumbling. An account that drains 80% between 10:07 and 10:22 must
be caught even though no clock-aligned bucket contains it.

### Q3 · Settlement drift from late adjustments
> What is the net daily balance shift **per currency**, and how much financial
> drift was introduced into **already-published** close reports by CDC `U` and
> `D` events that arrived after those reports were signed?

This is the reconciliation question that gets platforms audited. You are
quantifying how wrong yesterday's published number turned out to be.

---

# DATASET & CDC HARNESS

**PaySim — Synthetic Financial Dataset for Fraud Detection** (Kaggle).
`step` is an hour counter from 1. Anchor **step 1 = 2026-01-01 00:00:00 UTC**.

PaySim is a flat transaction log. Debezium output is not. **Build the harness
first** — everything downstream is meaningless without a realistic CDC stream.

### Harness requirements

Emit newline-delimited JSON to `s3://<bucket>/raw/cdc/dt=YYYY-MM-DD/`, one file
per hour, minimum **20 consecutive days**. Envelope shape:

```json
{
  "op": "u",
  "ts_ms": 1767225600000,
  "source": {"table": "transactions", "lsn": 84412093, "ts_ms": 1767225598000},
  "before": {"transaction_id": "T-88213", "status": "PENDING",  "amount": 4200.00},
  "after":  {"transaction_id": "T-88213", "status": "SETTLED",  "amount": 4200.00}
}
```

Inject these, because the real source does:

| Behaviour | Rate | Why it matters |
|---|---|---|
| Late arrivals — `source.ts_ms` belongs to a prior day | **3%** | Q1 and Q3 exist because of these |
| Same PK twice in one file, different `ts_ms` | **1%** | Breaks a naive `MERGE` (step 4) |
| `op: "d"` — chargeback reversal / account closure | **0.5%** | Must vanish from current, persist in history |
| Out-of-order LSN within a file | **2%** | Ordering by arrival is wrong; you need `ts_ms` |
| Schema drift — a `risk_score` field appears at day 12 | once | Bronze must not break |

Commit the harness. It is graded as production code, not a fixture.

---

# ARCHITECTURE

```
s3://raw/cdc/dt=*/                      Debezium JSON envelopes
        │  Step 1
bronze.cdc_events                       append-only, schema-on-read, audit cols
        │  Step 2  dedup + event ordering
        │  Step 4  MERGE INTO
silver.ledger                           one row per transaction, current state
silver.accounts                         one row per account, running balance
        │  Step 6
gold.daily_account_summary              open/close, credit/debit volume
gold.velocity_alerts                    rolling 15-min breach events
gold.settlement_drift                   restated vs originally-published
        │  Step 8
Redshift Spectrum → materialized views  executive dashboards
```

---

# THE TEN STEPS

## Step 1 · Bronze ingestion — EMR + PySpark

Land the envelopes without judging them. Bronze is the record of *what arrived*,
so it is append-only and never updated.

```python
from pyspark.sql import functions as F

raw = (spark.read
       .option("mode", "PERMISSIVE")
       .option("columnNameOfCorruptRecord", "_corrupt")
       .json(f"s3://{BUCKET}/raw/cdc/dt={run_date}/"))

bronze = (raw
    .withColumn("cdc_operation",   F.col("op"))
    .withColumn("source_ts",       (F.col("source.ts_ms") / 1000).cast("timestamp"))
    .withColumn("arrival_ts",      (F.col("ts_ms")        / 1000).cast("timestamp"))
    .withColumn("source_lsn",      F.col("source.lsn"))
    .withColumn("_src_file",       F.input_file_name())
    .withColumn("_ingested_at",    F.current_timestamp())
    .withColumn("_batch_id",       F.lit(BATCH_ID))
    .withColumn("extract_date",    F.lit(run_date).cast("date")))

bronze.writeTo("glue_catalog.fintech_db.bronze_cdc_events").append()
```

```sql
CREATE TABLE glue_catalog.fintech_db.bronze_cdc_events (
    cdc_operation   STRING,
    source_ts       TIMESTAMP,
    arrival_ts      TIMESTAMP,
    source_lsn      BIGINT,
    before          STRING,
    after           STRING,
    _src_file       STRING,
    _ingested_at    TIMESTAMP,
    _batch_id       STRING,
    extract_date    DATE
) USING iceberg
PARTITIONED BY (extract_date)
TBLPROPERTIES ('write.parquet.compression-codec' = 'zstd');
```

**Partition on `extract_date`, not `source_ts`.** Ingestion writes one partition
per run; partitioning on source date would scatter every batch across 20+
partitions and produce small files immediately. Document this trade-off —
it costs you pruning on `source_ts`, which you recover in silver.

**Idempotency contract.** Re-running a batch must not duplicate rows. Use
`_batch_id` and `overwritePartitions()` rather than blind `append()`:

```python
bronze.writeTo("glue_catalog.fintech_db.bronze_cdc_events").overwritePartitions()
```

**Deliverable:** load day 5 twice. `count(*)` identical, two snapshots, same row
count. Record both snapshot IDs.

---

## Step 2 · Deduplication & event ordering — EMR + PySpark

One transaction can appear many times across batches, and more than once inside
a single batch. Collapse to the latest state per key.

```python
from pyspark.sql.window import Window

w = (Window.partitionBy("transaction_id")
           .orderBy(F.col("source_ts").desc(), F.col("source_lsn").desc()))

latest = (spark.table("glue_catalog.fintech_db.bronze_cdc_events")
    .where(F.col("_batch_id") == BATCH_ID)
    .select(
        F.get_json_object("after", "$.transaction_id").alias("transaction_id"),
        F.get_json_object("after", "$.account_id").alias("account_id"),
        F.get_json_object("after", "$.amount").cast("decimal(18,2)").alias("amount"),
        F.upper(F.get_json_object("after", "$.currency")).alias("currency"),
        F.upper(F.get_json_object("after", "$.status")).alias("status"),
        F.get_json_object("after", "$.tx_type").alias("tx_type"),
        "cdc_operation", "source_ts", "source_lsn")
    .withColumn("_rn", F.row_number().over(w))
    .where(F.col("_rn") == 1)
    .drop("_rn"))
```

Order on `source_ts` **then** `source_lsn`. Two events can share a millisecond;
LSN is the tiebreaker and it is monotonic in the source. Ordering on
`arrival_ts` is wrong — that is when the file landed, not when the change
happened.

`dropDuplicates()` is not acceptable here. It keeps an arbitrary row, so your
refund report is right on Monday and wrong on Tuesday with no code change.

**Deliverable:** rows in, rows out, collapse ratio. Count of keys that appeared
more than once within one batch.

---

## Step 3 · Silver schema — EMR + Iceberg DDL

```sql
CREATE TABLE glue_catalog.fintech_db.silver_ledger (
    transaction_id  STRING,
    account_id      STRING,
    amount          DECIMAL(18,2),
    currency        STRING,
    tx_type         STRING,
    status          STRING,
    event_timestamp TIMESTAMP,
    source_lsn      BIGINT,
    is_deleted      BOOLEAN,
    _updated_at     TIMESTAMP
) USING iceberg
PARTITIONED BY (day(event_timestamp), bucket(16, account_id))
TBLPROPERTIES (
    'format-version'                  = '2',
    'write.delete.mode'               = 'merge-on-read',
    'write.update.mode'               = 'merge-on-read',
    'write.merge.mode'                = 'merge-on-read',
    'write.target-file-size-bytes'    = '134217728',
    'write.distribution-mode'         = 'hash'
);

ALTER TABLE glue_catalog.fintech_db.silver_ledger
  WRITE ORDERED BY account_id, event_timestamp DESC;
```

Four decisions to defend in your report:

- **`day(event_timestamp)`** — hidden transform. Analysts filter the raw
  timestamp and still prune; no derived column, no `WHERE day=` ritual.
- **`bucket(16, account_id)`** — `account_id` is high cardinality. Partitioning
  on it directly would produce millions of directories. 16 buckets caps that and
  co-locates an account's history. Justify 16 against your volume and core count.
- **`merge-on-read`** — CDC means constant small updates. Copy-on-write would
  rewrite a whole 128 MB file to change one row. You pay for this at read time
  and in step 7.
- **`write.distribution-mode = hash`** — without it, ten rows for one partition
  spread across ten tasks become ten files.

---

## Step 4 · CDC upserts — MERGE INTO

```sql
MERGE INTO glue_catalog.fintech_db.silver_ledger t
USING staged_changes s
   ON t.transaction_id = s.transaction_id

WHEN MATCHED AND s.cdc_operation = 'd'
   THEN UPDATE SET t.is_deleted = true, t._updated_at = current_timestamp()

WHEN MATCHED AND s.cdc_operation IN ('u','c','r')
              AND s.source_ts > t.event_timestamp
   THEN UPDATE SET
        t.amount          = s.amount,
        t.status          = s.status,
        t.currency        = s.currency,
        t.event_timestamp = s.source_ts,
        t.source_lsn      = s.source_lsn,
        t._updated_at     = current_timestamp()

WHEN NOT MATCHED AND s.cdc_operation <> 'd'
   THEN INSERT (transaction_id, account_id, amount, currency, tx_type,
                status, event_timestamp, source_lsn, is_deleted, _updated_at)
        VALUES (s.transaction_id, s.account_id, s.amount, s.currency, s.tx_type,
                s.status, s.source_ts, s.source_lsn, false, current_timestamp());
```

### The two things that will break

**Duplicate keys on the source side.** If `staged_changes` contains two rows for
one `transaction_id`, the MERGE aborts. That is the correctness guarantee
working, not a bug — the engine cannot know which row you meant. Step 2 is the
fix. Capture the exact error text; it is a deliverable.

**Out-of-order overwrite.** The `s.source_ts > t.event_timestamp` guard is why a
late-arriving day-3 event cannot clobber a day-11 value. Remove it and your
ledger silently rots. Prove the guard works:

1. Record account X's balance and `event_timestamp`.
2. Inject a synthetic late row for X with an older `source_ts` and a wrong amount.
3. Show the ledger unchanged.
4. Show the row still present in bronze.

**Soft delete, not hard.** `is_deleted = true` rather than `DELETE FROM`. A
chargeback must vanish from the current ledger and remain visible to the auditor.
Hard deletes plus snapshot expiry destroy that. Defend your choice either way.

---

## Step 5 · Point-in-time audit — Iceberg time travel

**Correct syntax matters here.** Iceberg on Spark uses `TIMESTAMP AS OF` /
`VERSION AS OF`. Athena prefixes with `FOR`. `FOR SYSTEM_TIME AS OF` is Delta /
SQL Server syntax and will fail.

```sql
-- Spark (EMR)
SELECT * FROM glue_catalog.fintech_db.silver_ledger
  TIMESTAMP AS OF '2026-01-16 23:59:59';

SELECT * FROM glue_catalog.fintech_db.silver_ledger
  VERSION AS OF 8333017788700497002;

-- Athena
SELECT * FROM fintech_db.silver_ledger
  FOR TIMESTAMP AS OF (current_timestamp - INTERVAL '7' DAY);
```

Find your snapshot first:

```sql
SELECT snapshot_id, committed_at, operation, summary['added-records']
FROM glue_catalog.fintech_db.silver_ledger.snapshots
ORDER BY committed_at;
```

### Answering Q1 properly — both numbers

```python
AS_OF = "2026-01-16 23:59:59"

# (a) balance AS WE KNEW IT on Friday — the table as of Friday's snapshot
as_known = spark.sql(f"""
    SELECT account_id, sum(CASE WHEN tx_type IN ('CASH-IN')
                                THEN amount ELSE -amount END) AS balance
    FROM glue_catalog.fintech_db.silver_ledger TIMESTAMP AS OF '{AS_OF}'
    WHERE account_id = '{ACCOUNT}' AND NOT is_deleted
      AND event_timestamp <= TIMESTAMP '{AS_OF}'
    GROUP BY account_id""")

# (b) balance AS WE NOW KNOW IT — current table, same event cutoff,
#     including late adjustments that arrived after Friday
as_restated = spark.sql(f"""
    SELECT account_id, sum(CASE WHEN tx_type IN ('CASH-IN')
                                THEN amount ELSE -amount END) AS balance
    FROM glue_catalog.fintech_db.silver_ledger
    WHERE account_id = '{ACCOUNT}' AND NOT is_deleted
      AND event_timestamp <= TIMESTAMP '{AS_OF}'
    GROUP BY account_id""")
```

The delta between (a) and (b) **is** the late-adjustment impact, per account.
That is the input to Q3. Report both, and the delta, for at least 5 accounts.

**Gotcha:** `TIMESTAMP AS OF` resolves to the newest snapshot **strictly older**
than the value. Passing a snapshot's own `committed_at` finds nothing older and
raises `Cannot find a snapshot older than …`. Also: never let a Spark timestamp
become a Python `datetime` on the way into SQL — PySpark converts it to the
**driver's local zone** and Iceberg re-parses it as session time, silently
shifting it by your UTC offset. Format inside SQL with `date_format()`.

---

## Step 6 · Gold aggregations — EMR + PySpark

### 6a · `gold_daily_account_summary`

```sql
CREATE TABLE glue_catalog.fintech_db.gold_daily_account_summary (
    account_id       STRING,
    business_date    DATE,
    currency         STRING,
    opening_balance  DECIMAL(18,2),
    credit_volume    DECIMAL(18,2),
    debit_volume     DECIMAL(18,2),
    closing_balance  DECIMAL(18,2),
    txn_count        BIGINT,
    computed_at      TIMESTAMP
) USING iceberg
PARTITIONED BY (business_date)
TBLPROPERTIES ('format-version' = '2');
```

**The reconciliation invariant is absolute:**

```
opening_balance + credit_volume − debit_volume = closing_balance
```

```sql
-- must return ZERO rows
SELECT account_id, business_date,
       opening_balance + credit_volume - debit_volume AS derived,
       closing_balance,
       abs(opening_balance + credit_volume - debit_volume - closing_balance) AS drift
FROM glue_catalog.fintech_db.gold_daily_account_summary
WHERE abs(opening_balance + credit_volume - debit_volume - closing_balance) > 0.01;
```

If it returns rows, find the bug. Do not filter it away. Report how many failed
before the fix and the root cause.

### 6b · `gold_velocity_alerts` — Q2

Rolling 15 minutes means a **range** frame over an epoch-seconds column, not
`rowsBetween`, and not a tumbling `window()`.

```python
outflow = (spark.table("glue_catalog.fintech_db.silver_ledger")
    .where(~F.col("is_deleted") & F.col("tx_type").isin("CASH-OUT", "TRANSFER", "DEBIT"))
    .withColumn("ts_epoch", F.col("event_timestamp").cast("long"))
    .withColumn("business_date", F.to_date("event_timestamp")))

w15 = (Window.partitionBy("account_id", "business_date")
             .orderBy("ts_epoch")
             .rangeBetween(-900, 0))          # 900 seconds, inclusive of current row

alerts = (outflow
    .withColumn("rolling_15m_outflow", F.sum("amount").over(w15))
    .join(opening_balances, ["account_id", "business_date"])
    .withColumn("pct_of_opening",
                F.col("rolling_15m_outflow") / F.col("opening_balance"))
    .where((F.col("opening_balance") > 0) & (F.col("pct_of_opening") >= 0.80))
    .withColumn("severity",
        F.when(F.col("pct_of_opening") >= 1.00, "CRITICAL")
         .when(F.col("pct_of_opening") >= 0.90, "HIGH")
         .otherwise("MEDIUM")))
```

Cross-check against PaySim's `isFraud`. Report precision and recall. **A low
score is fine; an unexamined one is not.** Name two fraud patterns this rule
structurally cannot see.

### 6c · `gold_settlement_drift` — Q3

Per currency per business date: the originally-published net position versus the
restated one, using the same snapshot technique as step 5.

```python
published = spark.sql(f"""
    SELECT currency, to_date(event_timestamp) AS business_date,
           sum(CASE WHEN tx_type='CASH-IN' THEN amount ELSE -amount END) AS net
    FROM glue_catalog.fintech_db.silver_ledger VERSION AS OF {SNAPSHOT_AT_CLOSE}
    WHERE NOT is_deleted GROUP BY 1,2""")

restated = spark.sql("""
    SELECT currency, to_date(event_timestamp) AS business_date,
           sum(CASE WHEN tx_type='CASH-IN' THEN amount ELSE -amount END) AS net
    FROM glue_catalog.fintech_db.silver_ledger
    WHERE NOT is_deleted GROUP BY 1,2""")

drift = (published.alias("p").join(restated.alias("r"),
            ["currency", "business_date"], "full_outer")
    .withColumn("drift_amount", F.col("r.net") - F.col("p.net"))
    .withColumn("drift_pct",    F.col("drift_amount") / F.abs(F.col("p.net"))))
```

Report the worst three currency-days by absolute drift, and state how many days
of published close reports were materially wrong.

---

## Step 7 · Table maintenance — EMR + Iceberg procedures

High-frequency `MERGE` on a merge-on-read table produces small files **and**
delete files. Both degrade reads.

```sql
-- compaction
CALL glue_catalog.system.rewrite_data_files(
    table   => 'fintech_db.silver_ledger',
    strategy => 'sort',
    sort_order => 'account_id ASC, event_timestamp DESC',
    options => map(
        'target-file-size-bytes',   '134217728',
        'partial-progress-enabled', 'true',
        'max-concurrent-file-group-rewrites', '4'
    ));

-- metadata
CALL glue_catalog.system.rewrite_manifests('fintech_db.silver_ledger');

-- retention
CALL glue_catalog.system.expire_snapshots(
    table       => 'fintech_db.silver_ledger',
    older_than  => TIMESTAMP '2026-01-10 00:00:00',
    retain_last => 50,
    stream_results => true);

CALL glue_catalog.system.remove_orphan_files(
    table => 'fintech_db.silver_ledger', dry_run => true);
```

### Measure it, do not assume it

| Metric | Before | After |
|---|---|---|
| Data files (`.files` where `content=0`) | | |
| Position delete files (`content=1`) | | |
| Average file size | | |
| Q1 query runtime | | |
| Bytes scanned | | |

```sql
SELECT content, count(*) AS files,
       cast(avg(file_size_in_bytes) AS BIGINT) AS avg_bytes
FROM glue_catalog.fintech_db.silver_ledger.files
GROUP BY content;
```

> **A plain `rewrite_data_files` will very likely reconcile none of your delete
> files.** Binpack only rewrites files it considers badly sized; delete files
> pointing at a healthy 128 MB file are not, on their own, a reason to rewrite
> it. Find the option that makes them eligible, and report both the no-op run
> and the working one. This is the single most valuable finding in this project.

**Retention vs audit.** `expire_snapshots` is what frees storage *and* what ends
time travel — the same operation. Q1 requires eight months of history. State your
retention policy and what it costs. Do not set `retain_last => 1` and then claim
you can answer an audit.

---

## Step 8 · Redshift integration

Zero-copy. The Iceberg tables stay in S3; Redshift reads them through Glue.

```sql
CREATE EXTERNAL SCHEMA spectrum_fintech
FROM DATA CATALOG
DATABASE 'fintech_db'
IAM_ROLE 'arn:aws:iam::<ACCOUNT>:role/RedshiftLakehouseRole'
REGION 'us-east-1';

SELECT count(*) FROM spectrum_fintech.gold_daily_account_summary;
```

The role needs `glue:GetTable*`, `glue:GetPartition*`, `glue:GetDatabase*` and
`s3:GetObject`/`s3:ListBucket` on the warehouse prefix. If the tables live in an
**S3 Tables bucket** rather than a general-purpose bucket, you need a Glue
**resource link** first and the catalog path changes — document which you used.

---

## Step 9 · Query optimisation in Redshift

```sql
-- materialize the executive aggregate
CREATE MATERIALIZED VIEW mv_exec_daily_close
AUTO REFRESH NO
AS
SELECT business_date,
       currency,
       count(DISTINCT account_id) AS active_accounts,
       sum(closing_balance)       AS total_closing_balance,
       sum(credit_volume)         AS total_credits,
       sum(debit_volume)          AS total_debits,
       sum(txn_count)             AS total_transactions
FROM spectrum_fintech.gold_daily_account_summary
GROUP BY business_date, currency;

REFRESH MATERIALIZED VIEW mv_exec_daily_close;
```

Then measure, three ways, same question:

| Path | Runtime | Data scanned |
|---|---|---|
| Athena direct on Iceberg | | |
| Redshift Spectrum external schema | | |
| Redshift materialized view | | |

Explain the differences. A materialized view that is 50× faster is not free —
state the refresh cost and the staleness window, and say which one you would put
behind a dashboard refreshing every five minutes.

---

## Step 10 · Executive reporting & final validation

### 10a · Answer the three questions in Redshift SQL

```sql
-- Q1: point-in-time balance, both versions, for one account
SELECT * FROM spectrum_fintech.gold_pit_balance_compare
WHERE account_id = 'C1231006815' AND as_of_ts = '2026-01-16 23:59:59';

-- Q2: velocity breaches, ranked
SELECT account_id, business_date, max(pct_of_opening) AS peak_pct, severity
FROM spectrum_fintech.gold_velocity_alerts
GROUP BY account_id, business_date, severity
ORDER BY peak_pct DESC LIMIT 50;

-- Q3: drift by currency
SELECT currency,
       sum(abs(drift_amount))                    AS total_abs_drift,
       count(*) FILTER (WHERE abs(drift_pct) > 0.001) AS material_days
FROM spectrum_fintech.gold_settlement_drift
GROUP BY currency ORDER BY total_abs_drift DESC;
```

### 10b · Full-pipeline idempotency

Re-run days 1–20 end to end into the same tables. Prove nothing moved:

```sql
SELECT count(*) AS rows,
       sum(closing_balance) AS total_balance,
       count(DISTINCT account_id) AS accounts
FROM glue_catalog.fintech_db.gold_daily_account_summary;
```

Identical before and after. If not, your pipeline is not idempotent — that is
the failure that puts a bank on a regulator's list.

---

# SUGGESTED 3-DAY PLAN

| Day | Steps | Checkpoint |
|---|---|---|
| 1 | Harness, 1–3 | 20 days of Debezium-style CDC landed; bronze idempotency proven with two snapshot IDs; silver DDL with MoR modes set |
| 2 | 4–6 | MERGE running with the ordering guard; duplicate-key error captured; Q1 both balances; Q2 rolling window; reconciliation at zero rows |
| 3 | 7–10 | Compaction no-op and fix both recorded; retention set; Redshift external schema + MV; three-way measurement; full re-run identical |

---

# ACCEPTANCE CRITERIA

| # | Criterion | Pass condition |
|---|---|---|
| A1 | CDC harness realism | All five injected behaviours present and measurable |
| A2 | Bronze idempotency | Same day loaded twice ⇒ identical row count |
| A3 | Dedup correctness | Zero duplicate `transaction_id` in silver |
| A4 | MERGE ordering guard | Late row does not overwrite newer state — proven in 4 steps |
| A5 | Reconciliation | The step-6a check returns **0 rows** |
| A6 | Q1 answered | Both balances produced and labelled, for ≥5 accounts |
| A7 | Q2 answered | Rolling — not tumbling — window, with precision/recall vs `isFraud` |
| A8 | Q3 answered | Drift per currency, with worst 3 currency-days named |
| A9 | Maintenance measured | Before/after table populated; delete-file finding reported |
| A10 | Redshift | External schema + MV live; three-way performance table populated |
| A11 | Full-pipeline idempotency | Gold totals unchanged after full re-run |

---

# EVIDENCE PACK

Numbers only obtainable from your own account. Submit as `EVIDENCE.md`.

| # | Item | Value |
|---|---|---|
| 1 | EMR cluster / Serverless application ID | |
| 2 | Glue database + warehouse S3 prefix | |
| 3 | Bronze snapshot ID after day-5 load 1 / load 2 | |
| 4 | Row count at both (must match) | |
| 5 | Dedup: rows in → rows out, collapse ratio | |
| 6 | Intra-batch duplicate keys found | |
| 7 | Exact error text from the duplicate-key MERGE failure | |
| 8 | Snapshot ID + `committed_at` used for Q1 (a) | |
| 9 | Q1 delta (restated − as-known) for 5 accounts | |
| 10 | Reconciliation: rows failing before fix / after (must be 0) | |
| 11 | Velocity alerts: count by severity; precision / recall | |
| 12 | Drift: worst 3 currency-days, material-day count | |
| 13 | Files & delete-files before / after compaction | |
| 14 | Delete files after **plain** compaction vs after the fix | |
| 15 | The option that made compaction reconcile deletes | |
| 16 | Retention policy set, and resulting time-travel horizon | |
| 17 | Athena / Spectrum / MV — runtime and bytes for the same query | |
| 18 | Gold totals before and after full re-run | |

**Break log.** Three genuine failures: real error text, diagnosis, fix. One must
be the duplicate-key MERGE. One must be the compaction no-op.

---

# TECHNICAL REVIEW (45 min, live)

Bring the cluster up. These are answered by running things, not describing them.

1. Show `silver_ledger.snapshots`. Walk the history and point at the MERGE that
   applied late adjustments.
2. Why `bucket(16, account_id)` and not `bucket(256, …)` or plain partitioning?
3. Show the physical plan for Q2. Where is the shuffle, and how many stages?
4. Your MERGE aborted first run. Exact error, and why is aborting *correct*?
5. Remove the `source_ts >` guard, re-run, and show me what breaks.
6. Q1 gives two numbers. Which one goes in the regulatory filing, and why?
7. Reconciliation failed N rows before your fix. Root cause?
8. Your first compaction reconciled zero delete files. Why? What fixed it?
9. What is your time-travel horizon right now, and which setting sets it?
10. Auditor asks for account X at a date eight months back. Run it. If it fails,
    explain exactly which decision in step 7 caused that.

---

# NOTES

- LLM assistance is expected. You are assessed on judgement, measurement and
  defence — not on typing. Every number must be reproducible from your account.
- Ambiguity in this brief is deliberate. Decide, document the assumption,
  defend it in review.
- Stop EMR at the end of each day. Put a budget alarm on the account first.
