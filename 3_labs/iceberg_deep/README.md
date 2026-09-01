# Iceberg Deep Labs — Spark × Iceberg interactions

Six runnable files covering what the `pyspark30` labs deliberately leave out:
**everything a plain Parquet directory cannot do.** MERGE, snapshots, time
travel, rollback, schema and partition evolution, delete files, compaction,
expiry, orphans, and write-audit-publish.

Companion to [EMR_ICEBERG_ATLAS.html](../../2_atlases/EMR_ICEBERG_ATLAS.html) —
each lab names the atlas chapters it makes concrete.

```bash
cd 3_labs/iceberg_deep
call windows_env.bat          # Windows only, once per shell
python 00_setup.py            # then 01 … 05, or:
./run_all.sh
```

**59 assertions across 6 files.** Every one was run end to end and passes on
PySpark 4.1.3 + `iceberg-spark-runtime-4.1_2.13:1.11.0`.

---

## The files

| File | Atlas ch. | What it proves | Checks |
|---|---|---|---|
| `00_setup.py` | 02–03 | `CREATE TABLE` writes metadata and **zero** data files. Three appends → three snapshots. Prints the real on-disk tree and the `version-hint.text` that *is* the Hadoop catalog. | 6 |
| `01_anatomy.py` | 02, 05 | Descends catalog → metadata.json → manifest list → manifest → data file using only metadata tables. Measures the pruning cascade: 18 files, 3 can match the predicate. | 7 |
| `02_commits_time_travel.py` | 03, 04, 06 | INSERT/UPDATE/DELETE/MERGE each append one snapshot with a distinct `operation`. Time travel by id and timestamp. Rollback, and **the fingerprint it leaves** in `.history`. | 10 |
| `03_evolution.py` | 07, 08 | Rename adds **no snapshot and rewrites no file** — column IDs make it free. Added columns read NULL on old rows. Narrowing is refused. Two partition specs coexist in one table. | 12 |
| `04_cow_mor_deletes.py` | 09 | Two identical tables, opposite write modes. CoW rewrites files; MoR writes position delete files. Both return identical rows. Delete debt accumulates, then compaction clears it. | 10 |
| `05_maintenance_wap.py` | 10, 11, 13 | 70 small files → 6. Sort clustering. `rewrite_manifests`. Expiry frees 76 files **and ends time travel in the same transaction**. Plants an orphan and removes it. Full WAP cycle: audit fails → drop branch → corrected run → cherry-pick → tag. | 14 |

Labs 02–04 mutate the `orders` table in sequence, so run them in order.
Labs 04 and 05 build their own tables and are independent.

---

## Five things these labs found that the docs gloss over

Each of these is a real behaviour discovered by running the code, not theory.

**1 · Compaction does not reconcile delete files by default.**
A plain `rewrite_data_files` on a merge-on-read table with five delete files
rewrites *nothing* — binpack only rewrites files it considers badly sized, and
delete files pointing at a well-sized file are not, on their own, a reason to
rewrite it. You need `rewrite-all` (small tables) or a tuned
`delete-file-threshold` (production). Lab 04 demonstrates both outcomes.

**2 · Compaction converges rather than completing in one pass.**
Even with `rewrite-all`, the first pass left one delete file behind; the second
cleared it. A delete file committed at a sequence number above the files being
rewritten survives that pass.

**3 · `TIMESTAMP AS OF` needs a moment *strictly after* a commit.**
Passing a snapshot's own `committed_at` finds nothing older and raises
`Cannot find a snapshot older than …`.

**4 · Never let a Spark timestamp become a Python `datetime` on the way into SQL.**
PySpark converts it to the **driver's local zone**; Iceberg then parses your
string as session time, silently shifting it by your UTC offset. Format it
inside SQL with `date_format()`. This cost real debugging time.

**5 · `remove_orphan_files` refuses intervals under 24 hours.**
Deleting a file a concurrent writer is still committing would corrupt the
table, so Iceberg blocks it outright. Lab 05 ages the planted orphan by a week
to get a legitimate demonstration.

---

## Notes

- **Lab 03 prints a wall of red on purpose.** It attempts a narrowing type
  change (`decimal(10,2) → int`), which Spark logs at ERROR level before
  refusing. That is the safety net working; the lab announces it beforehand.
- **Expiry is destructive by design.** Lab 05 expires down to a single snapshot
  to make the storage effect visible. Do not copy those retention values into
  production — `retain_last => 1` throws away every recovery point you have.
- **On EMR/Glue:** set `MODE = "emr"` in `config.py`. Not one line of lab code
  changes. `common.py` switches from the shorthand `type=hadoop` style to
  `catalog-impl=…GlueCatalog` with `S3FileIO`.
- **Windows shutdown noise.** Every lab ends with a
  `Failed to delete: …iceberg-spark-runtime….jar` stack trace from Spark's
  shutdown hook. The JVM still holds the JAR open. It is cosmetic and happens
  after all checks have already passed.
- **Moving this folder invalidates `warehouse/`.** Iceberg records absolute
  paths, so delete it and re-run `00_setup.py` after a move or clone.
