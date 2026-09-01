# PySpark: the 30 Functions — companion code

Runnable examples for **Module 03 — PySpark: The 30 Functions** (Surender Sara, NorthBay Solutions).

Every output printed in the handbook PDF was captured from a real run of these files.

---

## Quick start

```bash
pip install pyspark          # Spark 3.5+ or 4.x
python 00_setup.py           # builds raw -> bronze -> silver
python 01_shape.py           # functions 01-04
...
./run_all.sh                 # everything, stopping at the first failed check
```

No AWS account, no Iceberg JAR and no network are needed for the default mode.

---

## The three modes

`config.py` is the **only** file you edit. Nothing else contains a path, a bucket
name or a catalog name.

| `MODE` | Storage | Needs |
|---|---|---|
| `local_parquet` *(default)* | Parquet dirs under `./warehouse` | nothing |
| `local_iceberg` | real Iceberg tables, local Hadoop catalog | the Iceberg runtime JAR (Maven fetches it once) |
| `emr` | Glue Data Catalog + S3 | an AWS account; set `S3_BUCKET` |

**Not one line of example code changes between them.** Submit the same files to
EMR Serverless, EMR on EC2, or a Glue 5.x job.

---

## Files

| File | Contents | Checks |
|---|---|---|
| `config.py` | the mode flag, paths, `SCALE_ROWS` | — |
| `common.py` | session builder, table I/O, `block()` / `show()` / `check()` | — |
| `00_setup.py` | writes the raw files, builds bronze, builds silver | 5 |
| `01_shape.py` | **01** select · **02** withColumn · **03** cast · **04** withColumnRenamed/drop | 5 |
| `02_filter.py` | **05** filter/where · **06** when/otherwise · **07** coalesce · **08** fillna/dropna | 5 |
| `03_strings.py` | **09** trim/lower/upper · **10** split · **11** regexp_replace · **12** concat_ws | 4 |
| `04_dates.py` | **13** to_date · **14** date_trunc · **15** datediff/date_add | 5 |
| `05_aggregate.py` | **16** groupBy/agg · **17** count/countDistinct · **18** approx_count_distinct · **19** collect_set · *bonus:* pivot | 5 |
| `06_join_reshape.py` | **20** join · **21** broadcast · **22** left_anti/semi · **23** unionByName · **24** explode | 5 |
| `07_windows.py` | **25** Window/row_number · **26** rank/dense_rank · **27** lag/lead/rowsBetween | 5 |
| `08_control.py` | **28** dropDuplicates · **29** orderBy · **30** repartition/coalesce · *bonus:* cache | 5 |
| `expected/` | captured stdout from a real run — compare yours against it | — |

**44 assertions in total.** A file exits non-zero on the first failure, so
`run_all.sh` stops where something broke.

---

## Running one function instead of the whole family

Each function sits inside a delimited block:

```python
# ===== 10 · split ==========================================
block("10", "split", "string -> array. Then [n] to pull an element.")
show(orders.select(...), label="email -> local part + domain")
```

Copy a block into a notebook, or run the file and read the section you want.

---

## The dataset

Two source files land in `raw/`, then flow to bronze and silver.

**`orders`** (master) — 13 records, 12 distinct. JSON, with an array
(`promo_codes`) and a struct (`ship_address`).
**`order_items`** (child) — 18 rows. CSV.

It is **deliberately dirty**. Twelve defects were planted so that specific
functions have something honest to demonstrate:

- `region` spelled seven ways → functions 09, 16
- padded, mixed-case emails; one order with **no** email → 09, 07, 12, 17
- a null `channel`; empty and null `promo_codes` → 08, 24
- **ORD-1002 arrives twice**, the later version correcting `completed` → `refunded` → 28
- **ORD-1012** is a completed order with **zero** line items → 20
- **ORD-9999** is a line item with **no parent order** → 22
- a negative `qty` (a return) → 06
- timestamps stored as strings → 13

Set `SCALE_ROWS = 2_000_000` in `config.py` to append synthetic rows (deliberately
skewed ~60% to one region) when you want to feel a real shuffle. The seed rows stay
first, so every printed output still matches.

---

## Two Spark-version differences the code demonstrates

`spark.sql.ansi.enabled` is **false** in Spark 3.5 (so Glue 5.x and EMR 7.x) and
**true** in Spark 4.0+.

- A malformed `cast()` returns NULL under the former and **raises**
  `CAST_INVALID_INPUT` under the latter. `01_shape.py` block 03 shows both.
- `to_date()` on a non-ISO string behaves the same way. `04_dates.py` block 13.

`try_cast()` returns NULL in every version. Use it when you mean "best effort".

A pipeline that has silently dropped bad rows for years will start failing the day
it moves to a Spark 4 runtime. That is a good failure — but know it is coming.

---

## Two settings already made for you in `common.py`

- `spark.sql.session.timeZone = UTC` — see function 13 for the silent
  wrong-partition bug this prevents.
- `spark.sql.shuffle.partitions = 8` — the default of 200 would give you 200 tasks
  over 18 rows. On a real cluster you want **2–4× your core count**, not 8 and not 200.

---

Verified on Spark 4.2.0 / OpenJDK 21. All 30 functions are stable on Spark 3.5.
