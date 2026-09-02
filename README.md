# Data Engineering Foundations — PySpark, EMR & Iceberg on AWS

Teaching material for the three-module course by **Surender Sara**, NorthBay Solutions.
Everything is ordered by the sequence it is taught in: read `1_decks` top to bottom,
run `3_labs` alongside module 3, and keep `2_atlases` open on the second screen.

```
1_decks/       the three lecture PDFs, in teaching order
2_atlases/     interactive diagram companions (open in a browser)
3_labs/        runnable PySpark — 44 assertions, laptop or EMR
4_reference/   source material and deeper reading
```

---

## 1 · Decks

| # | File | Covers | Length |
|---|---|---|---|
| 1 | [1_PySpark_on_AWS_Lecture.pdf](1_decks/1_PySpark_on_AWS_Lecture.pdf) | **Zero to Architecture** — why distributed compute exists. HDFS, MapReduce, the shuffle, the S3 flip, Parquet and partitioning, one real job end to end, five ways your job dies. | ~50 slides |
| 2 | [2_Mindset_EMR_101.pdf](1_decks/2_Mindset_EMR_101.pdf) | **Mindset: EMR 101** — Glue vs EMR vs EMR Serverless with real money, three kinds of DataFrame, the 30 functions, *the 1 TB question*, Iceberg and the write, the four knobs, reviewing generated code. | 61 slides · 90 min |
| 3 | [3_PySpark_30_Functions_Handbook.pdf](1_decks/3_PySpark_30_Functions_Handbook.pdf) | **The 30 Functions** — eight families of PySpark functions, each with a runnable example and its real captured output. | handbook |

Module 1 answers *why*, module 2 answers *how*, module 3 is the daily vocabulary.
Decks 1 and 2 label themselves "Module 01/02" internally, matching these numbers.

## 2 · Visual atlases

Self-contained HTML — no build step, no server. Open the file, or use the hosted link.

| File | What it is |
|---|---|
| [EMR_Visual_Atlas.html](2_atlases/EMR_Visual_Atlas.html) | Companion to **deck 2**. Eleven progressive diagrams: the AWS platform, EMR cluster anatomy, Py4J, then the full 1 TB story — waves → map-side combine → hash routing → the global answer → range sort → skew → the Iceberg commit → the four knobs. |
| [EMR_ICEBERG_ATLAS.html](2_atlases/EMR_ICEBERG_ATLAS.html) | Sixteen deep diagrams of **Apache Iceberg internals with Spark**: metadata tree, write path, commit protocol, pruning cascade, time travel, hidden partitioning, schema evolution, copy-on-write vs merge-on-read, compaction, maintenance, catalogs, branching/WAP, migration, streaming, EMR wiring. Traced from the Definitive Guide in `4_reference`. |

Both are theme-aware (light/dark) and print cleanly to one figure per page for handouts.

## 3 · Labs

Two suites. Run `pyspark30` alongside deck 3, then `iceberg_deep` alongside
deck 2's Iceberg sections and the Iceberg atlas.

| Suite | Covers | Checks |
|---|---|---|
| [pyspark30/](3_labs/pyspark30/) | The 30 DataFrame functions — the daily vocabulary. Companion to deck 3. | 44 |
| [iceberg_deep/](3_labs/iceberg_deep/) | **Spark × Iceberg interactions** — MERGE, snapshots, time travel, rollback, schema and partition evolution, delete files, CoW vs MoR, compaction, expiry, orphans, write-audit-publish. | 59 |

### pyspark30 — the 30 functions

Every output printed in the handbook was captured from a real run of these files.

```bash
cd 3_labs/pyspark30
pip install pyspark
python 00_setup.py      # raw -> bronze -> silver
python 01_shape.py      # then 02 … 08
./run_all.sh            # everything, stops at the first failed check
```

**44 assertions across 9 files.** `config.py` is the only file you edit:

| `MODE` | Storage | Needs |
|---|---|---|
| `local_parquet` | Parquet dirs under `./warehouse` | nothing |
| `local_iceberg` | **real Iceberg tables**, local Hadoop catalog | the Iceberg runtime JAR (Maven fetches it once) |
| `emr` | Glue Data Catalog + S3 | an AWS account; set `S3_BUCKET` |

Not one line of example code changes between them — submit the same files to
EMR Serverless, EMR on EC2, or a Glue 5.x job.

### Verified working combination

Both modes were run end to end on Windows: **44/44 checks pass in `local_parquet`
and 44/44 in `local_iceberg`**, plus MERGE, time travel, `.snapshots`/`.history`
and DELETE against the local Iceberg catalog.

- **PySpark 4.1.3** with `iceberg-spark-runtime-4.1_2.13:1.11.0` — this pair works.
  There is no Iceberg runtime for Spark 4.2 yet, and the 4.1 runtime fails on it
  with `IncompatibleClassChangeError`.
- **Windows:** run `call windows_env.bat` once per shell first. It sets `HADOOP_HOME`
  (winutils — without it every write dies with `NativeIO$Windows.access0`),
  the `JAVA_TOOL_OPTIONS` security-manager flag for Java 17+, and pins
  `PYSPARK_PYTHON` so workers don't start the wrong interpreter.
- Use `python -m pip install …`, not bare `pip`, if several Pythons are installed.
- **If you move or clone this repo, delete `warehouse/` and re-run `00_setup.py`.**
  Iceberg manifests record **absolute** file paths, so a warehouse built at an old
  path fails with `NotFoundException: Failed to open input stream for file: …`.
  That is the format behaving correctly, and it is a good live demo of why the
  metadata tree points at real locations rather than relative directories.

### iceberg_deep — Spark × Iceberg internals

```bash
cd 3_labs/iceberg_deep
call windows_env.bat        # Windows, once per shell
./run_all.sh                # 59 checks, six files, in order
```

Six labs that make the Iceberg atlas concrete — see
[its README](3_labs/iceberg_deep/README.md) for the file-by-file map and for
**five behaviours these labs surfaced that the documentation glosses over**,
including the big one: a plain `rewrite_data_files` does **not** reconcile
merge-on-read delete files, so a naive compaction schedule silently lets read
cost grow forever.

## 4 · Reference

| File | Notes |
|---|---|
| `apache-iceberg-TDG_ER1.PDF` | *Apache Iceberg: The Definitive Guide* (Aakulov, Merced & Gidon, O'Reilly). Source for the Iceberg atlas. **Not in the repo** — gitignored as third-party material. Bring your own copy to `4_reference/`. |
| [s3_tables_iceberg_metadata_guide.md](4_reference/s3_tables_iceberg_metadata_guide.md) | S3 Tables / Iceberg metadata notes. |

---

## The dataset used throughout

Two dirty source files land in `raw/`, then flow to bronze and silver:
**orders** (13 records, 12 distinct — JSON with an array and a struct) and
**order_items** (18 rows, CSV). Twelve defects are planted deliberately so that
specific functions have something honest to demonstrate — `region` spelled seven
ways, padded and mixed-case emails, a null channel, an order that arrives twice
with a corrected status, an order with zero line items, an orphan line item, a
negative quantity, timestamps stored as strings.

Set `SCALE_ROWS = 2_000_000` in `config.py` to append synthetic rows (skewed ~60%
to one region) when you want to feel a real shuffle and see the skew from
deck 2's section 07 on your own machine.
