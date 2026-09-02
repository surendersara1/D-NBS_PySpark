# PROJECT 2 — E-Commerce Clickstream & Dynamic Pricing Elasticity

**Stack:** AWS EMR (PySpark) · Apache Iceberg · AWS Glue Data Catalog · Amazon Redshift
**Duration:** 3 days · **Level:** senior data engineer

You own the behavioural data platform for an online electronics retailer.
Merchandising changes prices continuously and has no measurement of the effect.
The clickstream is high-velocity and arrives as small files. Finance needs an
immutable view of Black Friday that survives live writes.

Three engineering problems dominate this build, and none of them is the
medallion structure:

1. **Binding the price the user actually saw** — a temporal range join, which is
   the single most expensive operation in Spark if you write it naively.
2. **High-velocity ingestion into hourly partitions** — guaranteed small files.
3. **Freezing an auditable state** while the table keeps changing underneath.

---

# THE THREE QUESTIONS

### Q1 · Price elasticity & conversion funnel drift
> When a product's price drops by more than **15%**, what happens to
> view→cart and cart→purchase conversion in the **2 hours after** the change,
> compared with the **7-day baseline** at the original price?

Two traps. First, a 2-hour window has small counts — a product with 3 purchases
before and 5 after is noise, not elasticity, so you need a minimum-volume floor
and you must state it. Second, if the same product changes price twice inside the
baseline window, your "original price" is undefined. Decide the rule and apply it
consistently.

### Q2 · Cart abandonment revenue impact from price increases
> Which sessions added an item to cart **within 30 minutes of a price increase**
> and then ended without purchasing — and what is the total lost revenue per
> category?

"Lost revenue" requires a definition. The price at cart-add time, or the new
higher price? Those give different numbers and different business conclusions.
Pick one, justify it, and report the other as a sensitivity.

### Q3 · Black Friday audit under live writes
> Can the executive team query exact conversion and pricing metrics **as they
> stood at midnight on Black Friday**, months later, while the underlying tables
> continue to receive updates and deletes?

This is not a reporting question, it is a retention question. Answering it
requires a named, immutable reference **and** a maintenance policy that will not
delete the data underneath it. Getting one without the other fails the audit.

---

# DATASET & INGESTION HARNESS

**eCommerce Events History in Electronics Store** (Kaggle) — or the Cosmetics
Store variant. State which.

Fields: `event_time`, `event_type` (view / cart / remove_from_cart / purchase),
`product_id`, `category_id`, `category_code`, `brand`, `price`, `user_id`,
`user_session`.

### You must build two streams, not one

The raw file is a single flat export. Your platform receives two independent
feeds, and the whole project turns on joining them correctly.

**Stream A — clickstream.** Replay the events as micro-batches: one JSON file per
**5-minute interval**, landing in `s3://<bucket>/raw/clickstream/dt=*/hh=*/`.
Minimum 14 simulated days. This deliberately creates the small-file problem.

**Stream B — price catalog CDC.** The raw file contains price *per event*. A real
platform has a price catalog that changes on its own schedule. Derive it:

```
product_id, price, effective_start, effective_end, change_reason
```

Rules:
- Detect genuine price changes from the event stream — the same `product_id`
  appearing at a different `price`. Define your noise threshold; a 0.01
  difference is rounding, not a repricing.
- Close each interval properly: `effective_end` of one row = `effective_start`
  of the next, with the current row open-ended (`NULL` or far-future sentinel —
  pick one and be consistent, it changes your join predicate).
- Ensure **at least 200 products** have a price change exceeding 15% in either
  direction, or Q1 has nothing to measure.
- Land it as its own feed under `s3://<bucket>/raw/price_catalog/`.

### Known dirt — handle all of it

| Problem | Note |
|---|---|
| `category_code` null on a large minority | Report the exact %. Drop, bucket as `unknown`, or backfill from `product_id` — decide and defend |
| `brand` null on a different, overlapping set | |
| `user_session` null, and reused after long gaps | You will define your own sessions in step 2 |
| Events not ordered by `event_time` within a file | |
| One `category_code` dominates the dataset | Measure its share. This becomes a shuffle problem in step 6 |

---

# ARCHITECTURE

```
s3://raw/clickstream/dt=*/hh=*/     5-min micro-batches, JSON
s3://raw/price_catalog/             price validity intervals
        │  Step 1
bronze_clickstream                  append-only, audit columns
bronze_price_catalog
        │  Step 2  dedup + sessionization
        │  Step 4  temporal join — bind the price actually seen
silver_clickstream                  hours(event_time), bucket(32, user_id)
silver_price_intervals
        │  Step 6
gold_product_conversion_hourly      funnel + elasticity per product per hour
gold_cart_abandonment               lost revenue per session and category
        │  Step 7   tag: black_friday_2026_final
        │  Step 8
Redshift Spectrum → materialized views
```

---

# THE TEN STEPS

## Step 1 · Bronze ingestion — EMR + PySpark

Two tables, both append-only.

```python
from pyspark.sql import functions as F

clicks = (spark.read
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt")
    .json(f"s3://{BUCKET}/raw/clickstream/dt={run_date}/")
    .withColumn("_src_file",   F.input_file_name())
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_batch_id",   F.lit(BATCH_ID))
    .withColumn("ingest_date", F.lit(run_date).cast("date")))

clicks.writeTo("glue_catalog.ecommerce_db.bronze_clickstream").overwritePartitions()
```

```sql
CREATE TABLE glue_catalog.ecommerce_db.bronze_clickstream (
    event_time    TIMESTAMP,
    event_type    STRING,
    product_id    STRING,
    category_id   STRING,
    category_code STRING,
    brand         STRING,
    price         DOUBLE,
    user_id       STRING,
    user_session  STRING,
    _src_file     STRING,
    _ingested_at  TIMESTAMP,
    _batch_id     STRING,
    ingest_date   DATE
) USING iceberg
PARTITIONED BY (ingest_date)
TBLPROPERTIES ('write.parquet.compression-codec' = 'zstd');
```

**Partition bronze on `ingest_date`, not `event_time`.** One batch writes one
partition. Partitioning on event time would scatter every micro-batch across many
partitions and multiply the small-file problem you already have.

Use `overwritePartitions()` rather than `append()` so a re-run of the same batch
is idempotent.

**Baseline measurement — record this now, you compare against it in step 7:**

```sql
SELECT count(*) AS files,
       cast(avg(file_size_in_bytes) AS BIGINT) AS avg_bytes,
       cast(min(file_size_in_bytes) AS BIGINT) AS min_bytes
FROM glue_catalog.ecommerce_db.bronze_clickstream.files
WHERE content = 0;
```

---

## Step 2 · Deduplication & sessionization — EMR + PySpark

### 2a · Deduplicate

```python
from pyspark.sql.window import Window

dedup_w = (Window
    .partitionBy("event_time", "user_id", "product_id", "event_type")
    .orderBy(F.col("_ingested_at").desc(), F.col("_src_file").desc()))

deduped = (spark.table("glue_catalog.ecommerce_db.bronze_clickstream")
    .withColumn("_rn", F.row_number().over(dedup_w))
    .where(F.col("_rn") == 1).drop("_rn"))
```

The `_src_file` tiebreaker matters: two rows can share an ingestion timestamp to
the millisecond. Without a deterministic second key, your dedup result changes
between runs and so does every downstream number.

### 2b · Sessionize — do not trust `user_session`

The raw `user_session` is unreliable: it is null on some rows and the same value
reappears after long gaps. Define sessions yourself.

> **Rule: a session ends after 30 minutes of inactivity for a `user_id`.**

```python
w_user = Window.partitionBy("user_id").orderBy("event_time")

sessionized = (deduped
    .withColumn("prev_ts", F.lag("event_time").over(w_user))
    .withColumn("gap_s",
        F.col("event_time").cast("long") - F.col("prev_ts").cast("long"))
    .withColumn("is_new_session",
        F.when(F.col("prev_ts").isNull() | (F.col("gap_s") > 1800), 1).otherwise(0))
    .withColumn("session_seq",
        F.sum("is_new_session").over(
            w_user.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
    .withColumn("session_key", F.concat_ws("#", "user_id", "session_seq")))
```

> **This is an unbounded window over `user_id` — the most expensive operation in
> step 2.** Every event for a user must land on one executor. Check the Spark UI
> for skew on the heaviest users. Report max vs median task duration on that
> stage; if the ratio exceeds ~10×, say what you would do about it.

Report: total sessions, median events per session, and how many of *your*
sessions disagree with the raw `user_session` — with one worked example.

---

## Step 3 · Silver schema — EMR + Iceberg DDL

```sql
CREATE TABLE glue_catalog.ecommerce_db.silver_clickstream (
    event_time      TIMESTAMP,
    event_type      STRING,
    product_id      STRING,
    category_id     STRING,
    category_code   STRING,
    brand           STRING,
    price_at_event  DECIMAL(12,2),
    catalog_price   DECIMAL(12,2),
    price_match     BOOLEAN,
    user_id         STRING,
    session_key     STRING,
    _updated_at     TIMESTAMP
) USING iceberg
PARTITIONED BY (hours(event_time), bucket(32, user_id))
TBLPROPERTIES (
    'format-version'               = '2',
    'write.target-file-size-bytes' = '134217728',
    'write.distribution-mode'      = 'hash'
);

ALTER TABLE glue_catalog.ecommerce_db.silver_clickstream
  WRITE ORDERED BY product_id, event_time;
```

Four decisions to defend:

- **`hours(event_time)`** — hidden transform, so analysts filter the raw
  timestamp and still prune. Justify hourly over daily against Q1's 2-hour window.
- **`bucket(32, user_id)`** — co-locates a user's events, which is what
  sessionization and Q2 need. **Note the tension:** Q1 is product-centric, so
  this bucketing does *not* help it. State the trade-off explicitly and say what
  you would do if product-side queries dominated.
- **`write.distribution-mode = hash`** — without it, a handful of rows for one
  hour spread across many tasks become many tiny files.
- **`WRITE ORDERED BY product_id`** — clusters rows so that product-level
  scans prune, and it is the prerequisite for sort-based compaction in step 7.

---

## Step 4 · Temporal join — bind the price the user actually saw

**This is the step that separates this project from a tutorial.** Everything
downstream depends on knowing the price displayed at the moment of the click.

The obvious query is a range join:

```sql
SELECT c.*, p.price AS catalog_price
FROM silver_clickstream c
JOIN silver_price_intervals p
  ON c.product_id = p.product_id
 AND c.event_time >= p.effective_start
 AND c.event_time <  COALESCE(p.effective_end, TIMESTAMP '9999-12-31')
```

### Why that is a trap

A `BETWEEN` predicate is **not an equality condition**, so Spark cannot hash the
join. Depending on statistics it degrades to a broadcast nested-loop join or a
sort-merge join with a range filter — on hundreds of millions of click rows, that
is where your afternoon goes.

**Do this: run it naively first, capture the physical plan, and record the
runtime.** You need the "before" to justify the "after".

```python
naive = spark.sql(NAIVE_RANGE_JOIN_SQL)
print(naive._jdf.queryExecution().executedPlan().toString())
```

Report the join strategy you see by name.

### Three fixes — implement at least two, measure all you implement

**A · Broadcast the price catalog.** If the interval table is small enough, force
it to every executor and delete the shuffle.

```python
from pyspark.sql.functions import broadcast
joined = clicks.join(broadcast(price_intervals), "product_id") \
               .where("event_time >= effective_start AND event_time < effective_end_x")
```

State the size of your interval table and the threshold property that governs
automatic broadcast. Say at what size this stops being safe and what fails first.

**B · Grid expansion — turn the range join into an equi-join.** Explode each
price interval into one row per hour it covers, then join on `(product_id, hour)`.

```python
price_hourly = (price_intervals
    .withColumn("effective_end_x",
        F.coalesce("effective_end", F.lit("2099-12-31").cast("timestamp")))
    .withColumn("hour_grid", F.explode(F.sequence(
        F.date_trunc("hour", F.col("effective_start")),
        F.date_trunc("hour", F.col("effective_end_x")),
        F.expr("INTERVAL 1 HOUR"))))
    .select("product_id", "hour_grid", "price"))

joined = (clicks
    .withColumn("event_hour", F.date_trunc("hour", "event_time"))
    .join(price_hourly,
          (clicks.product_id == price_hourly.product_id) &
          (F.col("event_hour") == F.col("hour_grid")), "left"))
```

Now it is a hash join. **The cost:** you lose sub-hour precision, and a price
that changed at 14:30 is attributed to the whole 14:00 hour. Quantify how many
events are misattributed and decide whether that is acceptable for Q1. If it is
not, use a 15-minute grid and report the row multiplication.

**C · Windowed as-of join.** Union clicks and price changes into one stream,
order by `(product_id, ts)`, and carry the last known price forward:

```python
w_asof = (Window.partitionBy("product_id").orderBy("ts")
                .rowsBetween(Window.unboundedPreceding, Window.currentRow))

asof = (union_stream
    .withColumn("carried_price", F.last("new_price", ignorenulls=True).over(w_asof))
    .where(F.col("record_type") == "click"))
```

Exact to the event, but another unbounded window and therefore another skew risk.

### Deliverable — the comparison table

| Approach | Runtime | Join strategy in plan | Shuffle read | Rows out | Price exact? |
|---|---|---|---|---|---|
| Naive range join | | | | | yes |
| A · broadcast | | | | | yes |
| B · hourly grid | | | | | no — hour granularity |
| C · as-of window | | | | | yes |

Then set `price_match = (price_at_event = catalog_price)` and report the
mismatch rate. A high rate means your interval derivation is wrong — go fix it
before step 6, because Q1 and Q2 both depend on it.

---

## Step 5 · Schema and partition evolution

### 5a · Schema evolution — metadata only

```sql
ALTER TABLE glue_catalog.ecommerce_db.silver_clickstream
  ADD COLUMN promotional_tag STRING;

ALTER TABLE glue_catalog.ecommerce_db.silver_clickstream
  ADD COLUMN price_change_pct DOUBLE AFTER catalog_price;
```

Prove it cost nothing:

```sql
SELECT count(*) FROM glue_catalog.ecommerce_db.silver_clickstream.files
WHERE content = 0;          -- run before and after; must be identical

SELECT count(*) FROM glue_catalog.ecommerce_db.silver_clickstream.snapshots;
```

**Both unchanged.** A schema change adds no snapshot and rewrites no Parquet,
because Iceberg tracks columns by permanent integer ID rather than by name or
position. Existing rows read `NULL` for the new column — correct, not an error.
Show that with a query.

Now demonstrate you understand *why*: rename a column and show old files still
resolve.

```sql
ALTER TABLE glue_catalog.ecommerce_db.silver_clickstream
  RENAME COLUMN category_code TO category_path;
```

### 5b · Partition evolution — the marked task

Your table is partitioned `hours(event_time)`. Suppose volume grows and you need
to add a second dimension, or the reverse — you started hourly and daily would be
cheaper for the older data.

```sql
ALTER TABLE glue_catalog.ecommerce_db.silver_clickstream
  ADD PARTITION FIELD bucket(16, product_id);
```

Prove all four:

1. Total row count unchanged.
2. **Zero data files rewritten** — file count identical immediately before and
   after the `ALTER`.
3. Two specs now coexist:

```sql
SELECT spec_id, count(*) AS partitions
FROM glue_catalog.ecommerce_db.silver_clickstream.partitions
GROUP BY spec_id ORDER BY spec_id;
```

4. A query spanning both old and new data returns correct results.

Explain what the engine does at plan time when one query covers two partition
specs. **And note the interaction with step 7:** compaction honours the *current*
spec, so a rewrite of old data will silently re-partition it. If you want history
left under the old spec, scope your compaction with a `where` filter.

> **Not every engine can do this.** `ALTER TABLE … ADD PARTITION FIELD` is a
> Spark SQL extension. Record which of your available engines supports it and
> which does not. Athena cannot.

---

## Step 6 · Gold aggregations — EMR + PySpark

### 6a · `gold_product_conversion_hourly`

```sql
CREATE TABLE glue_catalog.ecommerce_db.gold_product_conversion_hourly (
    product_id        STRING,
    category_code     STRING,
    event_hour        TIMESTAMP,
    catalog_price     DECIMAL(12,2),
    views             BIGINT,
    carts             BIGINT,
    purchases         BIGINT,
    view_to_cart      DOUBLE,
    cart_to_purchase  DOUBLE,
    overall_conv      DOUBLE,
    revenue           DECIMAL(18,2),
    computed_at       TIMESTAMP
) USING iceberg
PARTITIONED BY (days(event_hour))
TBLPROPERTIES ('format-version' = '2');
```

Guard every rate against divide-by-zero — a product with views and no carts must
give `0.0`, not `NULL`, or your averages silently drop rows.

**Skew warning.** `groupBy(category_code)` on a dataset where one category holds
a large share will produce one enormous reducer task. Confirm AQE is on, measure
max vs median task duration on that stage, and report both. If AQE does not
resolve it, salt the key and re-aggregate.

### 6b · Q1 — elasticity around a price change

```python
# 1. find qualifying changes: |ΔP| > 15%
changes = (price_intervals
    .withColumn("prev_price", F.lag("price").over(
        Window.partitionBy("product_id").orderBy("effective_start")))
    .withColumn("pct_change",
        (F.col("price") - F.col("prev_price")) / F.col("prev_price"))
    .where(F.abs("pct_change") > 0.15))

# 2. post-window: 2 hours after the change
post = (conversion_hourly.alias("c").join(changes.alias("k"), "product_id")
    .where((F.col("c.event_hour") >= F.col("k.effective_start")) &
           (F.col("c.event_hour") <  F.expr("k.effective_start + INTERVAL 2 HOURS")))
    .groupBy("product_id", "k.effective_start")
    .agg(F.sum("views").alias("post_views"),
         F.sum("carts").alias("post_carts"),
         F.sum("purchases").alias("post_purchases")))

# 3. baseline: 7 days before, at the ORIGINAL price
base = (conversion_hourly.alias("c").join(changes.alias("k"), "product_id")
    .where((F.col("c.event_hour") <  F.col("k.effective_start")) &
           (F.col("c.event_hour") >= F.expr("k.effective_start - INTERVAL 7 DAYS")) &
           (F.col("c.catalog_price") == F.col("k.prev_price")))
    .groupBy("product_id", "k.effective_start")
    .agg((F.sum("purchases") / 84.0).alias("base_purchases_2h")))   # 7d -> per 2h
```

**Normalise the windows.** A 7-day baseline compared against a 2-hour post-window
is 84 two-hour blocks; comparing raw sums is meaningless. Do the arithmetic and
say so in the report.

Then:

```
elasticity = pct_change_in_purchases / pct_change_in_price
```

Apply a **minimum-volume floor** — state it, e.g. at least 30 baseline purchases
— and report how many products qualified out of how many had a change.

**The honest paragraph is mandatory.** Report median elasticity, whether the sign
matches economic theory, and **at least two confounders you cannot rule out**:
seasonality, concurrent promotions, stock-outs, competitor pricing, day-of-week.
A confident elasticity number with no stated confounders is a wrong answer, not
an incomplete one — merchandising will act on it.

### 6c · Q2 — cart abandonment after a price increase

```python
increases = changes.where(F.col("pct_change") > 0.15)

cart_after_increase = (silver.alias("s")
    .where(F.col("s.event_type") == "cart")
    .join(increases.alias("i"), "product_id")
    .where((F.col("s.event_time") >= F.col("i.effective_start")) &
           (F.col("s.event_time") <  F.expr("i.effective_start + INTERVAL 30 MINUTES"))))

purchased = (silver.where(F.col("event_type") == "purchase")
    .select("session_key", "product_id").distinct())

abandoned = (cart_after_increase.join(purchased, ["session_key","product_id"], "left_anti")
    .withColumn("lost_revenue_at_cart", F.col("s.catalog_price"))
    .withColumn("lost_revenue_at_old",  F.col("i.prev_price")))
```

Report **both** revenue definitions — priced at cart-add versus at the old
pre-increase price — aggregated per `category_code`. State which one you would
put in front of the business and why. They differ, and the difference is the
argument for or against the price rise.

---

## Step 7 · Branching, tagging, and the maintenance interaction

### 7a · Compaction — you have a small-file problem by construction

5-minute micro-batches into hourly partitions guarantee it. Measure, compact,
measure again.

```sql
CALL glue_catalog.system.rewrite_data_files(
    table      => 'ecommerce_db.silver_clickstream',
    strategy   => 'sort',
    sort_order => 'product_id ASC, event_time ASC',
    options    => map(
        'target-file-size-bytes',   '134217728',
        'partial-progress-enabled', 'true'));

CALL glue_catalog.system.rewrite_manifests('ecommerce_db.silver_clickstream');
```

| Metric | Before | After |
|---|---|---|
| Data files | | |
| Average file size | | |
| Manifests | | |
| Q1 query runtime | | |
| Bytes scanned | | |

> The `sort` strategy needs a sort order declared on the table. You set one in
> step 3. If you skipped it, this call will not do what you expect — report what
> happened.

### 7b · Tag the Black Friday state — Q3

```sql
-- tag the current state
ALTER TABLE glue_catalog.ecommerce_db.gold_product_conversion_hourly
  CREATE TAG `black_friday_2026_final` RETAIN 365 DAYS;

-- or pin a specific historical snapshot
ALTER TABLE glue_catalog.ecommerce_db.gold_product_conversion_hourly
  CREATE TAG `black_friday_2026_final`
  AS OF VERSION 8333017788700497002
  RETAIN 365 DAYS;

-- read it back, months later, while the table keeps changing
SELECT * FROM glue_catalog.ecommerce_db.gold_product_conversion_hourly
  VERSION AS OF 'black_friday_2026_final';

SELECT * FROM glue_catalog.ecommerce_db.gold_product_conversion_hourly.refs;
```

**Prove immutability, do not assert it:**

1. Record a metric from the tag — say total purchases for one category.
2. Run heavy `UPDATE`/`DELETE`/`INSERT` against the live table.
3. Re-read through the tag. Identical.
4. Read the live table. Different.
5. Paste all four results.

> ### The trap that fails this audit silently
>
> **`RETAIN 365 DAYS` is not optional here.** A tag without retention, or a
> retention shorter than your audit horizon, lets `expire_snapshots` remove the
> snapshot the tag points at. Your tag then references nothing and Q3 fails —
> months later, in front of an auditor.
>
> **And if these tables live in an S3 Tables bucket rather than a general-purpose
> bucket, it is worse:** the presence of *any* user-defined branch or tag causes
> automated snapshot management to fail **for the entire table**. Nothing surfaces
> in your query engine. Storage then grows without bound and nothing expires.
>
> Determine which storage you are on, state it, and if it is S3 Tables, check the
> maintenance job status explicitly and report the result. A student who claims
> the audit works without verifying this has not finished the step.

### 7c · Retention policy

```sql
CALL glue_catalog.system.expire_snapshots(
    table       => 'ecommerce_db.gold_product_conversion_hourly',
    older_than  => TIMESTAMP '2026-01-01 00:00:00',
    retain_last => 100);
```

Run this **after** tagging, then re-verify the tag still reads. State your
retention policy and the time-travel horizon it produces.

---

## Step 8 · Redshift external catalog integration

```sql
CREATE EXTERNAL SCHEMA spectrum_ecommerce
FROM DATA CATALOG
DATABASE 'ecommerce_db'
IAM_ROLE 'arn:aws:iam::<ACCOUNT>:role/RedshiftLakehouseRole'
REGION 'us-east-1';

SELECT count(*) FROM spectrum_ecommerce.gold_product_conversion_hourly;
```

The role needs `glue:GetTable*`, `glue:GetPartition*`, `glue:GetDatabase*`, plus
`s3:GetObject` / `s3:ListBucket` on the warehouse prefix.

Generate column statistics so Redshift's cost-based optimiser can prune
partitions and push predicates down. Report the plan **before and after** stats
exist for one representative query, and quantify the difference.

---

## Step 9 · Redshift materialized views

```sql
CREATE MATERIALIZED VIEW mv_category_elasticity AS
SELECT category_code,
       date_trunc('day', event_hour)         AS business_date,
       count(DISTINCT product_id)            AS products,
       sum(views)                            AS views,
       sum(carts)                            AS carts,
       sum(purchases)                        AS purchases,
       sum(revenue)                          AS revenue,
       sum(carts)::float / nullif(sum(views),0)      AS view_to_cart,
       sum(purchases)::float / nullif(sum(carts),0)  AS cart_to_purchase
FROM spectrum_ecommerce.gold_product_conversion_hourly
GROUP BY 1, 2;

REFRESH MATERIALIZED VIEW mv_category_elasticity;
```

> **Verify before you design around it: `AUTO REFRESH YES` is restricted for
> materialized views built on external tables.** A materialized view over a
> Spectrum external schema generally cannot auto-refresh — you refresh it
> explicitly, on your own schedule. Confirm the behaviour in your Redshift
> version, record what you found, and if auto-refresh is unavailable, document
> the refresh cadence you chose and the staleness window it implies.
>
> `AVG(price)` across a category, as a headline metric, is close to meaningless —
> it averages a $2,000 laptop with a $9 cable. Aggregate revenue and volume, and
> derive rates. Weight any price metric by units sold.

Measure the same question three ways:

| Path | Runtime | Data scanned |
|---|---|---|
| Athena direct on Iceberg | | |
| Redshift Spectrum external schema | | |
| Redshift materialized view | | |

State which you would put behind a dashboard refreshing every five minutes, and
what the MV costs to keep current.

---

## Step 10 · Executive reporting & audit validation

```sql
-- Q1: elasticity for products with a >15% drop
SELECT product_id, category_code, pct_price_change,
       base_conv_2h, post_conv_2h,
       (post_conv_2h - base_conv_2h) / nullif(base_conv_2h,0) AS conv_lift,
       elasticity
FROM spectrum_ecommerce.gold_price_elasticity
WHERE pct_price_change < -0.15 AND base_purchases >= 30
ORDER BY elasticity ASC LIMIT 50;

-- Q2: lost revenue by category, both definitions
SELECT category_code,
       count(DISTINCT session_key)   AS abandoned_sessions,
       sum(lost_revenue_at_cart)     AS lost_at_new_price,
       sum(lost_revenue_at_old)      AS lost_at_old_price
FROM spectrum_ecommerce.gold_cart_abandonment
GROUP BY category_code ORDER BY lost_at_new_price DESC;

-- Q3: the frozen Black Friday view, queried live
SELECT category_code, sum(purchases) AS purchases, sum(revenue) AS revenue
FROM spectrum_ecommerce.gold_product_conversion_hourly_bf_tag
GROUP BY category_code;
```

For Q3, demonstrate the audit end to end: run the tagged query, then run heavy
DML against the live table, then run the tagged query again and show it is
byte-identical while the live table has moved.

---

# ACCEPTANCE CRITERIA

| # | Criterion | Pass condition |
|---|---|---|
| A1 | Two independent feeds built | Clickstream micro-batches **and** a derived price-interval catalog |
| A2 | ≥200 products with a >15% price change | Otherwise Q1 has no sample |
| A3 | Sessionization | Own 30-minute rule; disagreement with `user_session` quantified |
| A4 | **Temporal join measured** | Naive plan captured; ≥2 optimisations implemented and compared |
| A5 | Price binding accuracy | `price_match` rate reported and explained |
| A6 | Schema evolution | File count and snapshot count both unchanged |
| A7 | **Partition evolution** | Zero files rewritten; two `spec_id`s coexisting |
| A8 | Compaction | Before/after table populated; sort strategy actually applied |
| A9 | Q1 | Windows normalised, volume floor stated, ≥2 confounders named |
| A10 | Q2 | Both revenue definitions reported, with a recommendation |
| A11 | **Q3** | Tag with retention; 4-step immutability proof; expiry policy verified |
| A12 | Redshift | External schema + MV; three-way performance table populated |

---

# EVIDENCE PACK

| # | Item | Value |
|---|---|---|
| 1 | EMR cluster / Serverless application ID | |
| 2 | Dataset variant; storage type (general-purpose S3 or S3 Tables) | |
| 3 | Micro-batches written; simulated days | |
| 4 | Price intervals derived; products with >15% change | |
| 5 | `category_code` null %; largest category share % | |
| 6 | Sessions created; median events/session; disagreement vs `user_session` | |
| 7 | Sessionization stage: max vs median task duration | |
| 8 | **Naive range join**: runtime + join strategy from the plan | |
| 9 | **Optimised joins**: runtime, strategy, shuffle bytes for each approach | |
| 10 | `price_match` rate | |
| 11 | Files + snapshots before/after `ADD COLUMN` (must be identical) | |
| 12 | File count before/after `ADD PARTITION FIELD` (must be identical) | |
| 13 | `spec_id` values and partition counts | |
| 14 | Files, avg size, manifests: before/after compaction | |
| 15 | Q1: products qualifying / total; median elasticity; volume floor used | |
| 16 | Q2: abandoned sessions; lost revenue both ways, top 3 categories | |
| 17 | Tag name, snapshot ID, `committed_at`, retention set | |
| 18 | Tag immutability proof — all four readings | |
| 19 | Snapshot-expiry health check result | |
| 20 | Athena / Spectrum / MV — runtime and bytes, same query | |

**Break log.** Three genuine failures with real error text, diagnosis and fix.
One must come from the temporal join. One must come from partition evolution or
tagging.

---

# TECHNICAL REVIEW (45 min, live)

1. Show the naive range-join plan. Name the join strategy and explain why Spark
   chose it.
2. Show your optimised plan. What changed, and what did it cost you?
3. Your hourly grid loses sub-hour precision. How many events are misattributed,
   and why is that acceptable — or is it?
4. `ADD COLUMN` changed no files and no snapshots. Explain the mechanism.
5. Show `partitions` grouped by `spec_id`. What does the engine do at plan time?
6. Your compaction honours the current spec. What happens to data written under
   the old one, and how did you protect it?
7. Read me the Black Friday number from the tag. Now run a DELETE against the
   live table and read it again.
8. What stops `expire_snapshots` from destroying your tag?
9. Are you on S3 Tables? If so, show me the maintenance job status.
10. Your median elasticity is X. Name a confounder and the data that would remove it.
11. Category Y is Z% of your data. Show me the stage where that hurt.
12. Q2 gives two lost-revenue numbers. Which goes to merchandising, and why?

---

# NOTES

- LLM assistance is expected. Assessment is on judgement, measurement and
  defence — not on typing. Every number must be reproducible from your account.
- **Load in micro-batches as specified.** Loading the file in one shot forfeits
  the small-file baseline and the compaction measurement.
- Claims about join behaviour must be backed by a pasted physical plan.
- Ambiguity is deliberate. Decide, document the assumption, defend it.
- Stop EMR at the end of each day. Budget alarm on the account first.
