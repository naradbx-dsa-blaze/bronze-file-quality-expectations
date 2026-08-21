-- =====================================================================
-- Bronze file-level data-quality pattern (Lakeflow Declarative Pipeline)
-- ---------------------------------------------------------------------
-- Key idea: DLT / Lakeflow expectations are ROW-level, not file-level or
-- batch-level. Auto Loader batches files, but expectations still evaluate
-- each record independently. This pipeline shows how to build the
-- file-level behavior teams actually want ON TOP of the row-level model:
--
--   * Keep valid rows from every file            -> fq_bronze_clean
--   * Isolate invalid rows WITH their source file -> fq_bronze_quarantine
--   * Decide accept/reject per whole file         -> fq_file_quality_summary
--   * (Optional) promote only fully-clean files   -> fq_silver_payments
--
-- `source_path` is supplied by the pipeline configuration (see the
-- resources/*.pipeline.yml file / bundle variables).
-- =====================================================================

-- 1) BRONZE RAW ---------------------------------------------------------
-- Ingest every row from every file with Auto Loader. Capture
-- `_metadata.file_path` so every row knows which file it came from.
-- This is what closes the "no source_file in the event log" gap.
-- NOTE: we intentionally do NOT use expect_or_fail here -- a single bad
-- row would atomically roll back the entire update (all files), which is
-- the all-or-nothing behavior we are trying to avoid.
CREATE OR REFRESH STREAMING TABLE fq_bronze_raw
COMMENT 'Raw payment rows from all files, with source-file attribution'
AS
SELECT
  payment_id,
  vendor_id,
  payment_date,
  amount,
  currency,
  _metadata.file_path              AS source_file,
  _metadata.file_modification_time AS file_mod_time,
  current_timestamp()              AS _ingested_at
FROM STREAM read_files('${source_path}', format => 'json');

-- 2) BRONZE VALIDATED ---------------------------------------------------
-- Add a per-row is_valid flag. The EXPECT constraints run in WARN mode
-- (no ON VIOLATION clause), so the pipeline event log records passed /
-- failed counts for observability WITHOUT dropping or failing anything --
-- every row still flows through so we can route it downstream.
CREATE OR REFRESH STREAMING TABLE fq_bronze_validated
(CONSTRAINT payment_date_not_null EXPECT (payment_date IS NOT NULL),
 CONSTRAINT amount_positive       EXPECT (amount > 0))
COMMENT 'Row-level quality flag; keeps both good and bad rows for routing'
AS
SELECT
  *,
  (payment_date IS NOT NULL AND amount > 0) AS is_valid
FROM STREAM(fq_bronze_raw);

-- 3) BRONZE CLEAN (row-level acceptance) --------------------------------
-- Valid rows from ALL files flow through: File 1 & File 3 fully, plus the
-- good rows of File 2. This is `expect_or_drop` semantics made explicit --
-- it drops only offending ROWS, never whole files.
CREATE OR REFRESH STREAMING TABLE fq_bronze_clean
COMMENT 'Valid rows only, from every file (row-level acceptance)'
AS
SELECT * EXCEPT (is_valid)
FROM STREAM(fq_bronze_validated)
WHERE is_valid;

-- 4) BRONZE QUARANTINE (attribution / vendor report) --------------------
-- Invalid rows, isolated and tagged with the file that produced them.
-- Query this table grouped by source_file to tell a vendor exactly which
-- file was problematic -- no event-log parsing required.
CREATE OR REFRESH STREAMING TABLE fq_bronze_quarantine
COMMENT 'Invalid rows, attributed to the source file'
AS
SELECT * EXCEPT (is_valid)
FROM STREAM(fq_bronze_validated)
WHERE NOT is_valid;

-- 5) FILE QUALITY SUMMARY (this is where "file-level" lives) ------------
-- One row per file: total rows, failed rows, and an accept/reject
-- decision. A file is accepted only if it has zero failing rows.
CREATE OR REFRESH MATERIALIZED VIEW fq_file_quality_summary
COMMENT 'Per-file totals and accept/reject decision (file-atomic gate)'
AS
SELECT
  source_file,
  count(*)                                        AS total_rows,
  sum(CASE WHEN is_valid THEN 0 ELSE 1 END)       AS failed_rows,
  (sum(CASE WHEN is_valid THEN 0 ELSE 1 END) = 0) AS file_accepted
FROM fq_bronze_validated
GROUP BY source_file;

-- 6) SILVER (optional file-atomic acceptance) ---------------------------
-- Stricter mode: promote rows ONLY from files with zero failures. This
-- rejects ALL of a bad file -- including its good rows -- giving true
-- whole-file accept/reject that expectations alone cannot express.
-- Omit this table if row-level acceptance (fq_bronze_clean) is enough.
CREATE OR REFRESH MATERIALIZED VIEW fq_silver_payments
COMMENT 'Rows promoted only from fully-clean files (whole-file rejection)'
AS
SELECT v.* EXCEPT (is_valid)
FROM fq_bronze_validated v
JOIN fq_file_quality_summary s
  ON v.source_file = s.source_file
WHERE s.file_accepted;
