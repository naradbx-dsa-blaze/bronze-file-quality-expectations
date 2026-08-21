# Detailed answers

Restating the original questions and answering each with the pattern in this repo.

---

## Context / the reported challenge

> Auto Loader batches files together (default 1000 per micro-batch), and DLT
> expectations operate on the entire batch, not individual files. This prevents
> us from accepting valid files while rejecting only the problematic ones.

### Important correction to the premise

Expectations do **not** operate on "the entire batch." Per the
[docs](https://docs.databricks.com/aws/en/dlt/expectations), they *"apply data
quality checks on each record passing through a query"* — i.e. **row level**.

What you observed as "all-or-nothing" is the behavior of `expect_or_fail`
specifically:

| Action | Effect on a violation |
|---|---|
| `expect` (warn) | Row is **kept**; violation is counted in the event log. |
| `expect_or_drop` | Only the **violating rows** are dropped. Good rows continue. |
| `expect_or_fail` | The **entire update is atomically rolled back** and the pipeline stops. |

So `expect_or_fail` doesn't reject "the batch" — it fails the whole update, which
is even broader. And `expect_or_drop` would already accept Files 1 & 3 and drop
only the bad rows of File 2. Neither rejects a *file*, because the engine has no
file-level concept for expectations.

---

## Q1. Is there a configuration to make expectations operate at file-level granularity with Auto Loader?

**No.** Expectations are row-level by design and there is no setting that changes
that. Two anti-patterns to avoid:

- **Don't** set `cloudFiles.maxFilesPerTrigger = 1` + `expect_or_fail` hoping to
  get per-file failure. It still just *fails the whole update* on the first bad
  row (it doesn't skip the file and continue), and it destroys throughput.
- **Don't** rely on batch boundaries — they are non-deterministic and unrelated
  to file boundaries.

File-level granularity is something you **compose** on top of row-level
expectations. This repo shows how.

---

## Q2. Recommended pattern for bronze file-based expectations when per-file pass/fail and tracking is required

Four moves (all implemented in
[`src/transformations/file_quality_pipeline.sql`](../src/transformations/file_quality_pipeline.sql)):

### 1. Capture the source file on every row
Auto Loader exposes the hidden `_metadata` column
([docs](https://docs.databricks.com/aws/en/ingestion/file-metadata-column)) with
`file_path`, `file_name`, `file_size`, `file_modification_time`. Select
`_metadata.file_path` into a `source_file` column in bronze. This is what
resolves *"no `source_file` in the event log."* You carry attribution in the
**data**, not the event log.

### 2. Tag + quarantine instead of drop/fail
Add an `is_valid` boolean and split:
- `fq_bronze_clean` = `WHERE is_valid` → good rows from **every** file.
- `fq_bronze_quarantine` = `WHERE NOT is_valid` → bad rows, **with** `source_file`.

Keep the `EXPECT` constraints in **WARN mode** so the event log still records
passed/failed counts for dashboards and alerting — without dropping anything.

### 3. Report per-file failures from the data
```sql
SELECT source_file, COUNT(*) AS failed_records
FROM fq_bronze_quarantine
GROUP BY source_file;
```
This is the vendor-facing report: "file X had N bad rows." No `event_log()`
parsing required.

### 4. Enforce whole-file accept/reject (the primary requirement here)
Since the goal is to **accept clean files and reject a problematic file
entirely** (not just its bad rows), aggregate per file and gate downstream:
```sql
-- fq_file_quality_summary: one row per file, file_accepted = (failed_rows = 0)
-- fq_silver_payments: promote rows only WHERE file_accepted
```
This gives true file-atomic semantics that expectations alone cannot express:
File 2 is rejected in full (including its good rows), while Files 1 & 3 pass.

---

## Which mode should we use?

| Requirement | Use |
|---|---|
| **Reject the entire file if any row is bad** (whole-file accept/reject) | `fq_file_quality_summary` + `fq_silver_payments` — **recommended for this use case** |
| Report exactly which file was problematic | `fq_bronze_quarantine` grouped by `source_file` |
| Keep valid rows even from a partially-bad file (row-level salvage) | `fq_bronze_clean` |
| Observability / alerting on failure rates | `EXPECT` (WARN) metrics from the event log |

---

## Reprocessing a fixed file

Because bad rows are quarantined (not lost) and files are attributed, remediation
is straightforward: the vendor resends the corrected file, Auto Loader picks it up
as a new file, and it flows through the same checks. If you used whole-file
rejection, the corrected file will now pass the gate and its rows land in silver.
