# CAPSTONE — Student 2
## Clickstream sessionisation and price elasticity at event scale

**Duration:** 3 days · **Stack:** EMR (PySpark) → Apache Iceberg (3 layers) → Redshift
**Total marks:** 100

---

## 0 · The situation

You are the first data engineer at a fast-growing electronics retailer. Until
now, analytics has been a nightly export to a spreadsheet. It takes forty
minutes and nobody trusts it.

**Marco, Head of Merchandising**, changes prices constantly and has no idea
whether it works. He wants to know: when we drop the price of a product by 10%,
what actually happens to views, add-to-carts and purchases — and how long does
the effect last?

**Lena, Head of Growth**, wants the funnel. View → cart → purchase, by category
and by hour of day. She has been told "the data is too big for that" and does
not believe it.

You have one more constraint the others do not know about. The event volume is
growing fast. **The table you design on day 1 for a modest daily volume will be
wrong by day 3**, and you will have to change its physical layout **without
rewriting history and without taking the table offline.** That is the exam.

---

## 1 · Dataset

**eCommerce Events History in Electronics Store** (Kaggle) — or the Cosmetics
Store variant. Either is acceptable; state which you used.

Fields: `event_time`, `event_type` (view, cart, remove_from_cart, purchase),
`product_id`, `category_id`, `category_code`, `brand`, `price`, `user_id`,
`user_session`.

### Known problems in this data — you must handle all of them

These are real properties of the file, not hypotheticals. Find them, quantify
them, and decide what to do:

1. `category_code` is **null for a large minority of rows**. Report the exact
   percentage. Decide whether those rows are dropped, bucketed as `unknown`, or
   back-filled from `product_id` — and defend it.
2. `brand` is null on a different, overlapping set of rows.
3. `user_session` can be **null**, and the same session ID can reappear after a
   long gap — treat that carefully when you sessionise.
4. `price` for the same `product_id` **changes over time**. That is not dirt —
   that is your entire day-3 analysis. Do not deduplicate it away.
5. Events are not perfectly ordered by `event_time` within a file.

### Volume staging — mandatory

You will process the data in **three waves**, because the layout decision
depends on volume:

- **Wave A (day 1):** the first 7 days of events
- **Wave B (day 2):** extend to 30 days
- **Wave C (day 3):** the full file

Do not shortcut this by loading everything on day 1. The partition-evolution
task on day 2 only means something if you felt the problem first.

---

## 2 · What you are building

```
raw/            event files as landed
  |
bronze.events           every event, as received, typed only
  |
silver.events           cleaned, deduplicated, sessionised
silver.price_history    price per product over time (SCD-style)
  |
gold.funnel_hourly      view -> cart -> purchase by category and hour
gold.price_elasticity   response to price changes
  |
Redshift                Marco's and Lena's marts
```

---

# DAY 1 — Bronze, and a partitioning decision you will regret

**Goal: land Wave A and commit to a layout.**

### 1.1 Bronze table
Create `bronze.events` as an Iceberg table over Wave A.

Partition it with **`day(event_time)`** and nothing else. Yes, this will be the
wrong choice by day 2. Choose it anyway — you will evolve it, and the exam is
about doing that safely.

Record now, because you will compare against it later:

- number of data files
- average file size
- rows per partition

### 1.2 Prove hidden partitioning works
Write a query filtering on the **raw `event_time` column** — no `day=` predicate
anywhere — for a single day. Record the **bytes scanned** from the Athena results
pane. Then run the same query with no filter at all and record bytes scanned
again.

Report the ratio. Then explain in your own words **why** the first query pruned,
given that you never mentioned the partition column. If your answer is "because
Iceberg is clever", you have not answered.

### 1.3 Profile the dirt
Produce a data-quality report over Wave A:

| Column | Null % | Distinct | Notes |
|---|---|---|---|

Include at minimum `category_code`, `brand`, `user_session`, `price`. State your
decision for each null case and why.

### Day 1 deliverables
- bronze DDL with the partition spec
- the file-count / average-size / rows-per-partition baseline
- the two bytes-scanned figures and the ratio
- the data-quality report with decisions

---

# DAY 2 — Silver, sessionisation, and evolving the layout live

**Goal: extend to 30 days, feel the pain, fix it without a rewrite.**

### 2.1 Load Wave B
Extend bronze to 30 days. Now re-measure what you baselined on day 1:

- files, average size, rows per partition

Your daily partitions are now large. Report the numbers and state, with a
sentence of reasoning, why `day()` has become the wrong granularity for Lena's
hour-of-day funnel.

### 2.2 Partition evolution — the marked task
Evolve the partition spec from `day(event_time)` to **`hours(event_time)`**
without rewriting history and without dropping the table.

Then prove all four of these:

1. The table still returns the **same total row count** as before the change.
2. **No data files were rewritten** by the evolution itself — show the file
   count immediately before and immediately after the `ALTER`.
3. **Two partition specs now coexist.** Query the `partitions` metadata table
   grouped by `spec_id` and paste the result.
4. A query spanning old and new data returns correct results.

In your report, explain what the engine does at plan time when a single query
covers two partition specs.

> **Which engine can do this?** Not all of them. Find out which of your available
> engines supports `ALTER TABLE ... ADD PARTITION FIELD` and which does not, and
> record that in your report. Choosing the wrong tool here costs you an hour.

### 2.3 Sessionisation
Build `silver.events` with a proper session model. `user_session` in the raw data
is not trustworthy on its own — you must define a session yourself:

> A session ends after **30 minutes of inactivity** for a given `user_id`.

Implement it with window functions. Assign your own `session_key`. Report:

- total sessions
- median events per session
- how many of *your* sessions disagree with the raw `user_session` value, and why

### 2.4 `silver.price_history`
The same product appears at different prices over time. Build a table with one
row per `(product_id, price)` change: the price, the interval it was in force,
and a flag for whether it was an increase or a decrease.

State how you decided a price "changed" rather than being noise or a data error.

### 2.5 Skew
Find your most common `category_code`. Report what share of all events it holds.
If any single value exceeds ~20% of the dataset, state what that does to a
`groupBy(category_code)` shuffle and what you would do about it. Show evidence
from the Spark UI — max vs median task duration on that stage.

### Day 2 deliverables
- the before/after volume measurements
- the `ALTER` statement and all four proofs
- the sessionisation logic + the three session statistics
- `silver.price_history` DDL and sample rows
- the skew measurement with Spark UI evidence

---

# DAY 3 — Gold, elasticity, and Redshift

**Goal: full volume, and answers Marco and Lena can act on.**

### 3.1 Load Wave C
Full dataset. Report final volume: rows, files, table size, partition count by
`spec_id`.

### 3.2 `gold.funnel_hourly` for Lena
Per `category_code`, per hour of day: views, carts, purchases, view→cart rate,
cart→purchase rate, overall conversion.

Then answer her actual question in prose, with numbers: **which hours convert
best, and does that differ by category?** One paragraph, three numbers minimum.

### 3.3 `gold.price_elasticity` for Marco
This is the analytical core of the exam.

For each product with at least one price change and sufficient events on both
sides of it, compute:

- mean daily views, carts and purchases in the **7 days before** the change
- the same for the **7 days after**
- the percentage price change
- an elasticity figure: **% change in purchases ÷ % change in price**

Then write the honest paragraph. Include at minimum:

- how many products had enough data to measure at all, out of how many
- your median elasticity, and whether the sign is what economics predicts
- **at least two confounders you cannot rule out** with this dataset
  (seasonality, promotions, stock-outs, competitor pricing — pick real ones)

**A confident answer with no stated confounders scores zero on this section.**
Marco will act on this number. Tell him what it can and cannot support.

### 3.4 Redshift
Expose gold to Redshift. Build:

- `mart_funnel_daily` — Lena's funnel, daily grain, category dimension
- `mart_price_response` — Marco's elasticity with product and brand attributes

Document the access path you chose and why.

Measure and report: **the same funnel question answered from Redshift vs from
Iceberg directly** — runtime and data scanned for both. Say which you would put
behind a dashboard refreshing every five minutes, and why.

---

## 4 · Deliverables

```
/pipeline/        bronze, silver, gold PySpark jobs
/sql/             Athena / Redshift SQL
/evidence/        the ledger below, plus pasted outputs and Spark UI screenshots
/REPORT.md        8-12 pages
```

---

## 5 · Evidence Ledger — mandatory

| # | Item | Value |
|---|---|---|
| 1 | EMR Serverless application ID | |
| 2 | Dataset variant used (electronics / cosmetics) | |
| 3 | Wave A: data files, avg file size | |
| 4 | Wave B: data files, avg file size (before evolution) | |
| 5 | File count immediately **before** `ADD PARTITION FIELD` | |
| 6 | File count immediately **after** it (must match #5) | |
| 7 | `spec_id` values present, with partition count for each | |
| 8 | Bytes scanned: filtered single-day query | |
| 9 | Bytes scanned: unfiltered query | |
| 10 | Total sessions after your 30-minute rule | |
| 11 | Largest `category_code` share of total events (%) | |
| 12 | Max vs median task duration on the skewed stage | |
| 13 | Products with measurable elasticity / total products | |
| 14 | Median elasticity value | |
| 15 | Athena query execution ID for the funnel query | |
| 16 | Redshift vs Iceberg: runtime and bytes for the same funnel question | |

### Break Log — mandatory
Three genuine failures with real error text, diagnosis and fix. At least one must
come from the partition evolution step.

---

## 6 · Defence (15 minutes, live)

1. Show me `partitions` grouped by `spec_id`. Explain what each row means.
2. You filtered on `event_time` and it pruned. Trace exactly how, from the
   predicate to the files opened.
3. Why did you not just recreate the table with hourly partitions on day 2?
   What would that have cost?
4. Your 30-minute session rule disagrees with `user_session` on N sessions.
   Pick one and walk me through it.
5. Category X is Y% of your data. Show me the stage where that hurt.
6. Your median elasticity is Z. Is the sign right? What would make you distrust it?
7. Name a confounder in your elasticity number and tell me what data would
   remove it.
8. A product changed price twice in one week. How does your analysis handle that?
9. Which of your engines could not have done the partition evolution, and why?
10. Lena wants this dashboard refreshing every 5 minutes. What breaks first?

---

## 7 · Rubric

| Area | Marks |
|---|---|
| Bronze design + hidden-partitioning proof with measured bytes | 10 |
| Data-quality profiling with defended decisions | 8 |
| **Partition evolution: executed correctly + all four proofs** | 20 |
| Sessionisation logic and its statistics | 12 |
| `silver.price_history` design | 8 |
| Skew identified and measured with Spark UI evidence | 7 |
| Funnel correctness + the prose answer for Lena | 8 |
| **Elasticity analysis + honest confounders** | 14 |
| Redshift marts + measured comparison | 7 |
| Evidence Ledger complete and internally consistent | 5 |
| Break Log — three real failures | 5 |
| **Defence** | 20 |
| | **/124, scaled to 100** |

---

## 8 · Rules

- **Using an LLM is allowed and expected.** You are marked on judgement,
  evidence and defence — not on typing.
- Every number must be reproducible from your own account.
- Do not share tables, snapshots or query IDs with another student.
- Load in three waves as specified. Loading everything on day 1 forfeits the
  partition-evolution marks, because you cannot prove the before-state.
- Stop your EMR Serverless application at the end of each day.
- Ambiguity is deliberate. Decide, document the assumption, defend it.
