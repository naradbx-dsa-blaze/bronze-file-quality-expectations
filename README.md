# Bronze file-level data quality with Lakeflow / DLT expectations

A runnable reference implementation for enforcing **file-aware** data quality at
the bronze layer when ingesting files with Auto Loader — accepting good files,
rejecting only the bad ones, and reporting exactly which file was problematic.

---

## TL;DR — the key insight

> **DLT / Lakeflow expectations operate at the _row_ level, not the file or
> batch level.** There is no configuration that makes them file-atomic.

Because of that:

- `expect_or_drop` drops only the **offending rows**, not the batch or file —
  so the good rows of a partially-bad file still flow through.
- `expect_or_fail` is *stricter than* "reject the batch": a single bad row
  **atomically rolls back the entire update** and halts the pipeline. That's the
  all-or-nothing behavior you want to avoid.

So instead of forcing file-level behavior into expectations, this repo builds it
**on top of** the row-level model, using three cheap ingredients:

1. **`_metadata.file_path`** captured on every row → know which file each row came from.
2. A **quarantine table** → isolate bad rows *with* their source file (your vendor report).
3. A **per-file summary + gate** → accept/reject whole files when you need atomicity.

---

## Architecture

```
                         data/payments/*.json  (Auto Loader)
                                   │
                                   ▼
          ┌───────────────────────────────────────────────┐
          │ fq_bronze_raw        (STREAMING TABLE)         │
          │  every row + source_file (_metadata.file_path) │
          └───────────────────────────────────────────────┘
                                   │
                                   ▼
          ┌───────────────────────────────────────────────┐
          │ fq_bronze_validated  (STREAMING TABLE)         │
          │  adds is_valid; EXPECT constraints = metrics    │
          │  (WARN mode → event log counts, drops nothing)  │
          └───────────────────────────────────────────────┘
                     │                         │
        is_valid ────┘                         └──── NOT is_valid
                     ▼                                     ▼
   ┌───────────────────────────┐        ┌───────────────────────────────┐
   │ fq_bronze_clean           │        │ fq_bronze_quarantine          │
   │ valid rows from ALL files │        │ bad rows + source_file        │
   │ (row-level acceptance)    │        │ (vendor report)               │
   └───────────────────────────┘        └───────────────────────────────┘

                     ┌───────────────────────────────────────────────┐
   (from validated)  │ fq_file_quality_summary  (MATERIALIZED VIEW)   │
        ───────────► │ per file: total, failed, file_accepted         │
                     └───────────────────────────────────────────────┘
                                   │
                                   ▼
          ┌───────────────────────────────────────────────┐
          │ fq_silver_payments   (MATERIALIZED VIEW)       │
          │  rows only from fully-clean files              │
          │  (optional whole-file rejection)               │
          └───────────────────────────────────────────────┘
```

| Table | Type | Purpose |
|---|---|---|
| `fq_bronze_raw` | Streaming table | All rows, all files, with `source_file` attribution |
| `fq_bronze_validated` | Streaming table | Adds `is_valid`; `EXPECT` constraints emit event-log metrics |
| `fq_bronze_clean` | Streaming table | **Row-level** acceptance — valid rows from every file |
| `fq_bronze_quarantine` | Streaming table | Invalid rows + `source_file` — the vendor report |
| `fq_file_quality_summary` | Materialized view | **File-level** accept/reject decision |
| `fq_silver_payments` | Materialized view | Optional **whole-file** acceptance (rejects entire bad file) |

---

## Sample data & expected results

Three files, five rows each. File 2 contains two rows with a NULL `payment_date`.

| File | Rows | Bad rows |
|---|---|---|
| `payments_2024_01.json` | 5 | 0 |
| `payments_2024_02.json` | 5 | **2** (NULL `payment_date`) |
| `payments_2024_03.json` | 5 | 0 |

After running the pipeline you should see:

| Table | Row count | Why |
|---|---|---|
| `fq_bronze_raw` | **15** | everything ingested |
| `fq_bronze_clean` | **13** | 15 − 2 bad rows (good rows survive from every file, incl. File 2) |
| `fq_bronze_quarantine` | **2** | both bad rows, both attributed to `payments_2024_02.json` |
| `fq_file_quality_summary` | **3** | File 02 → `file_accepted = false` |
| `fq_silver_payments` | **10** | File 02 rejected whole (its 3 good rows dropped too) |

---

## The two questions this answers

**Q: Which file was problematic?** — no event-log parsing needed:

```sql
SELECT source_file, COUNT(*) AS failed_records
FROM file_quality_demo.fq_bronze_quarantine
GROUP BY source_file;
-- payments_2024_02.json | 2
```

**Q: Accept good files, reject only the bad one?**

- *Row-level* (recommended default): `fq_bronze_clean` already contains the good
  rows from every file.
- *Whole-file* (strict): `fq_file_quality_summary` gives the accept/reject flag,
  and `fq_silver_payments` promotes rows only from fully-clean files.

See [`docs/ANSWERS.md`](docs/ANSWERS.md) for the full write-up of the original
questions.

---

## Run it

**Prerequisites:** Databricks CLI ≥ 0.230, a Unity Catalog workspace with
serverless pipelines enabled.

```bash
# 1. Point the bundle at your workspace: edit databricks.yml (host) or use a profile.
# 2. (Optional) override catalog/schema/volume via variables.
databricks bundle deploy -t dev \
  --var="catalog=main" --var="schema=file_quality_demo" --var="volume=fq_demo_landing"

# 3. One command runs setup (creates volume + loads sample files) then the pipeline:
databricks bundle run file_quality_demo_job -t dev
```

Then inspect the results:

```sql
SELECT * FROM file_quality_demo.fq_file_quality_summary ORDER BY source_file;
SELECT source_file, COUNT(*) FROM file_quality_demo.fq_bronze_quarantine GROUP BY source_file;
```

### Manual alternative (no bundle)

The sample files live in [`data/payments/`](data/payments). Copy them to a volume
and run the SQL in [`src/transformations/`](src/transformations) as a pipeline,
setting the `source_path` configuration to your volume path.

---

## Event-log metrics (observability)

The `EXPECT` constraints on `fq_bronze_validated` run in WARN mode, so they
populate the pipeline event log's `data_quality` metrics (passed / failed counts)
**without** dropping or failing anything:

```sql
SELECT
  e.name        AS expectation_name,
  e.dataset,
  e.passed_records,
  e.failed_records
FROM (
  SELECT explode(from_json(
    details:flow_progress:data_quality:expectations,
    'array<struct<name:string,dataset:string,passed_records:long,failed_records:long>>'
  )) AS e
  FROM event_log(TABLE(file_quality_demo.fq_file_quality_summary))
  WHERE event_type = 'flow_progress'
);
```

Note the event log reports **counts only — no `source_file`**. That is exactly
why file attribution comes from the quarantine table, not the event log.

---

## Repository layout

```
.
├── databricks.yml                       # Asset Bundle definition + variables
├── resources/
│   ├── file_quality.pipeline.yml        # the Lakeflow pipeline
│   └── demo.job.yml                      # setup + run job (one-command demo)
├── src/
│   ├── setup/create_volume_and_load.py   # creates volume, writes sample files
│   └── transformations/file_quality_pipeline.sql  # the pipeline logic
├── data/payments/                        # sample input files (for inspection)
└── docs/ANSWERS.md                       # detailed answers to the original questions
```

---

## References

- [Manage data quality with pipeline expectations](https://docs.databricks.com/aws/en/dlt/expectations)
- [File metadata column (`_metadata`)](https://docs.databricks.com/aws/en/ingestion/file-metadata-column)
- [Lakeflow Declarative Pipelines](https://docs.databricks.com/aws/en/ldp/)
