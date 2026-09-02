# CAPSTONE — Student 1
## Nightly CDC ingestion and a reconstructable account ledger

**Duration:** 3 days · **Stack:** EMR (PySpark) → Apache Iceberg (3 layers) → Redshift
**Total marks:** 100

---

## 0 · The situation

You have joined the data platform team at a mobile money provider. The core
banking system is an OLTP database you are **not** allowed to query directly.
Every night it drops a **change-data-capture extract** onto S3: inserts, updates
and deletes that occurred that day, each row carrying the balance before and
after the movement.

Two people depend on you.

**Priya, Head of Financial Control**, must close the books every morning. She
needs a daily balance per account that reconciles to the penny, and she must be
able to ask *"what did this account look like at 23:59 last Tuesday?"* when an
auditor calls — six months later.

**Sam, Fraud Operations**, needs a feed of accounts whose balance moved in ways
the transaction record does not explain, within an hour of the extract landing.

The previous engineer left. The pipeline they built re-ran twice one night and
**double-applied a day of transfers**. Nobody noticed for nine days. Your first
job is to build something where that cannot happen.

---

## 1 · Dataset

**PaySim — Synthetic Financial Dataset for Fraud Detection** (Kaggle).

Fields you will use: `step`, `type` (CASH-IN, CASH-OUT, TRANSFER, DEBIT,
PAYMENT), `amount`, `nameOrig`, `oldbalanceOrg`, `newbalanceOrig`, `nameDest`,
`oldbalanceDest`, `newbalanceDest`, `isFraud`, `isFlaggedFraud`.

`step` is an hour counter starting at 1. Treat **step 1 as 2026-01-01 00:00 UTC**
and derive a real `event_ts` from it. Every downstream partition depends on this,
so get it right on day 1.

### You must manufacture the CDC feed yourself

The raw file is a flat transaction log. A real CDC extract is not. Before you
build anything, write a **generator** that splits PaySim into a sequence of daily
CDC files under `s3://<class-bucket>/raw/<your_id>/cdc/dt=YYYY-MM-DD/`, where
each row carries:

- `op` — one of `I`, `U`, `D`
- `commit_ts` — when the change was committed in the source system
- `extract_ts` — when the CDC tool wrote it out
- the account key and the balance columns

Rules the generator must obey, because the real source system does:

1. **Roughly 3% of rows arrive late** — their `commit_ts` belongs to a previous
   day but they appear in today's file. Your pipeline must place them correctly.
2. **Roughly 1% of account keys appear more than once in the same daily file**,
   with different `commit_ts`. Only the latest may win.
3. **About 0.5% are `D` (delete) operations** — an account closure. A deleted
   account must disappear from the current ledger but must remain visible in
   history.
4. Produce **at least 20 consecutive days** of extracts.

Commit the generator script. It is marked.

---

## 2 · What you are building

```
raw/            daily CDC extracts, as landed, never modified
  |
bronze.cdc_events      every row ever received, append-only, with audit columns
  |
silver.accounts        ONE current row per account — the MERGE target
silver.transactions    cleaned, typed, deduplicated movement log
  |
gold.daily_balances    per account per day, closing balance, reconciled
gold.fraud_signals     unexplained balance movements for Sam
  |
Redshift               Priya's close-of-books marts
```

---

# DAY 1 — Bronze, and the idempotency contract

**Goal: land the CDC feed so that re-running any day is provably harmless.**

### 1.1 Bronze table
Create `bronze.cdc_events` as an Iceberg table. Append-only: bronze is the
system of record for *what arrived*, so it never gets an `UPDATE` or a `DELETE`.

Partition it on the **extract date**, not the commit date. Justify that choice in
one paragraph — the two dates disagree for 3% of your rows, and the reason you
partition on one rather than the other is a marked answer.

Add audit columns: source file path, ingestion timestamp, and a `batch_id` you
control.

### 1.2 The idempotency contract
Write the loader so that **running the same day's extract twice produces a table
identical to running it once.** Prove it:

- load day 5, record `SELECT count(*)` and the snapshot ID
- load day 5 again with the same `batch_id`
- record the count and snapshot ID again

Both counts must match. Explain in your report what mechanism you used and why
it holds. There is more than one correct answer; a bare `INSERT INTO` is not one
of them.

### 1.3 Late-arriving rows
Show, with a query, how many rows in your day-12 extract have a `commit_ts`
belonging to an earlier day. State what your bronze layer does with them.

### Day 1 deliverables
- generator script + loader script
- the idempotency proof (counts and snapshot IDs before/after the double load)
- the late-arrival count for day 12
- the full output of the `snapshots` metadata table for `bronze.cdc_events`
  after all 20 days

---

# DAY 2 — Silver, and the MERGE that must not lie

**Goal: one correct current row per account, and a movement log you can trust.**

### 2.1 `silver.accounts` — the MERGE target
This is the heart of the exam. Build a table holding exactly one row per
account with its current balance, current status (`ACTIVE` / `CLOSED`), and the
`commit_ts` of the change that produced it.

Load it with `MERGE INTO`. Your MERGE must handle all three of:

- `I` and `U` — upsert
- `D` — mark closed (decide: soft-delete flag, or row removal — and defend it)
- multiple changes for the same account in one batch — **only the latest applies**

> **You will hit an error here.** A `MERGE INTO` whose source contains more than
> one row per join key fails. That is not a bug to work around silently — it is
> the correctness guarantee doing its job. Capture the exact error text, then fix
> it properly. Both the error and the fix are marked.

### 2.2 Out-of-order safety
A late-arriving row from day 3 must **not** overwrite a newer value already in
the table. Add the guard. Then prove it:

1. Note account X's balance and `commit_ts`.
2. Feed a synthetic late row for X with an *older* `commit_ts` and a wrong balance.
3. Show the balance did not change.
4. Show the row was still recorded in bronze.

### 2.3 `silver.transactions`
Clean and type the movement log. Handle at minimum: the balance columns arrive as
strings in some extracts; `type` has inconsistent casing; `amount` can be
negative on reversals; some `nameDest` values are null for DEBIT rows.

Set a sort order on the table and say which queries you chose it for.

### 2.4 Time travel as an audit tool
Priya's auditor asks for account `C1231006815` as it stood at the end of day 10.
Answer it **twice**:

- once with `TIMESTAMP AS OF`
- once with `VERSION AS OF` a snapshot ID you looked up

Paste both queries, both results, and the snapshot ID.

### Day 2 deliverables
- the MERGE statement, final version
- the duplicate-key error text you hit, and the fix
- the out-of-order proof, all four steps
- both time-travel queries returning identical results
- the full output of the `history` metadata table for `silver.accounts`

---

# DAY 3 — Gold, reconciliation, and Redshift

**Goal: numbers Priya can sign, and a feed Sam can act on.**

### 3.1 `gold.daily_balances`
Per account, per day: opening balance, total in, total out, closing balance,
transaction count.

**The reconciliation rule, and it is absolute:**

```
opening_balance + total_in - total_out = closing_balance
```

Write a check query that returns **zero rows**. If it returns rows, you have a
bug — find it, do not filter it away. Report how many accounts failed before you
fixed it and what the cause was.

### 3.2 `gold.fraud_signals` for Sam
An "unexplained movement" is a row where the balance delta does not equal the
transaction amount, beyond a tolerance of **0.01**.

Produce, per account per day: the number of unexplained movements, the total
unexplained value, and a severity band you define and justify.

Cross-check your signal against PaySim's own `isFraud` column. Report precision
and recall. **A low score is fine — an unexamined score is not.** Explain in a
paragraph why your rule catches what it catches and misses what it misses.

### 3.3 Rerun the whole pipeline
Re-run days 1–20 end to end, from the same raw files, into the same tables.
Show `gold.daily_balances` is unchanged: same row count, same
`sum(closing_balance)`, and state the snapshot count before and after.

If the numbers moved, your pipeline is not idempotent. Fix it. This is the
scenario that cost your predecessor their job.

### 3.4 Redshift
Expose gold to Redshift and build two marts for Priya:

- `mart_daily_close` — per day: accounts, total closing balance, movement volume
- `mart_account_statement` — statement view for one account over a date range

Document your chosen access path (external schema over the Glue catalog, a
`CREATE DATABASE ... FROM ARN`, or the auto-mounted `awsdatacatalog`) and say why.

Then answer, with a number: **how long does a `mart_daily_close` query take, and
how much data does it scan, compared with the same query straight against
Iceberg?** Explain the difference.

---

## 4 · Deliverables

Submit one repository:

```
/generator/       the CDC feed generator
/pipeline/        bronze, silver, gold PySpark jobs
/sql/             Athena / Redshift SQL
/evidence/        the ledger below, plus pasted query outputs
/REPORT.md        8-12 pages
```

`REPORT.md` must contain: the architecture diagram, your idempotency mechanism,
the MERGE design and why, the reconciliation result, the fraud precision/recall
with your interpretation, and the Redshift comparison.

---

## 5 · Evidence Ledger — mandatory

Fill this in from **your own account**. Submissions without it are not marked.

| # | Item | Value |
|---|---|---|
| 1 | EMR Serverless application ID | |
| 2 | S3 Tables bucket ARN | |
| 3 | `bronze.cdc_events` snapshot ID after day-5 first load | |
| 4 | …snapshot ID after the day-5 **repeat** load | |
| 5 | Row count at both of the above (must match) | |
| 6 | `silver.accounts` — total snapshots after day 20 | |
| 7 | Snapshot ID used for the time-travel audit query | |
| 8 | `committed_at` of that snapshot (UTC, to the millisecond) | |
| 9 | Athena query execution ID for the reconciliation check | |
| 10 | Rows returned by the reconciliation check (must be 0) | |
| 11 | `sum(closing_balance)` before the full rerun | |
| 12 | `sum(closing_balance)` after the full rerun | |
| 13 | Data scanned: Iceberg query vs Redshift mart | |

### Break Log — mandatory
Document **three** things that genuinely broke, with the real error text, the
diagnosis, and the fix. One of them must be the duplicate-key MERGE failure.
A submission with no error text in it did not run.

---

## 6 · Defence (15 minutes, live)

You will be asked, among others:

1. Show me the `history` metadata table for `silver.accounts`. Point at the
   rollback, if there is one.
2. Why did you partition bronze on extract date and not commit date?
3. Account X had a late row arrive on day 12. Walk me through what happened to it
   at every layer.
4. Your MERGE failed the first time. What was the exact error, and why is that
   error *correct* behaviour rather than a bug?
5. Show the two time-travel queries returning the same answer. Now expire that
   snapshot and tell me which one still works.
6. Your reconciliation returned N rows before you fixed it. What was the cause?
7. Your fraud recall is X. Name two fraud patterns your rule structurally cannot
   see.
8. If the CDC extract for day 14 arrived twice, what stops the double-apply —
   precisely which line of code?
9. What does your pipeline do if day 13 never arrives at all?
10. Priya asks for a balance as of a date **eight months** ago. Does your table
    still answer that? What determines whether it can?

---

## 7 · Rubric

| Area | Marks |
|---|---|
| CDC generator realism — late arrivals, duplicates, deletes all present | 10 |
| Bronze design + partition justification | 8 |
| **Idempotency: mechanism, and the proof it works** | 15 |
| **MERGE correctness incl. out-of-order guard** | 18 |
| Silver cleaning and typing | 7 |
| Time travel used correctly as an audit tool | 7 |
| **Reconciliation passing at zero rows** | 10 |
| Fraud signal + honest precision/recall discussion | 8 |
| Redshift marts + measured comparison | 7 |
| Evidence Ledger complete and internally consistent | 5 |
| Break Log — three real failures | 5 |
| **Defence** | 20 |
| | **/120, scaled to 100** |

---

## 8 · Rules

- **Using an LLM is allowed and expected.** You are marked on judgement,
  evidence and defence — not on typing.
- Every number in your report must be reproducible from your own account.
- Do not share tables, snapshots or query IDs with another student. Your
  timestamps and IDs are checked against CloudTrail.
- Stop your EMR Serverless application at the end of each day.
- If something is ambiguous, **make a decision, write down the assumption, and
  defend it.** Real briefs are ambiguous. That is part of the exam.
