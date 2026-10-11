### 1. Why is `record_hash` useful for rerun-safe loading, and which columns should not be included in it?
`record_hash` is a SHA-256 fingerprint of a row's business content (`HASH_KEYS` in `src/transform/curated.py`: the ids, order timestamp, source `updated_at`, customer city/tier, product name/category/brand, quantity, unit price, discount and status). During loading, `INSERT ... ON CONFLICT (order_id) DO UPDATE ... WHERE stored.record_hash <> incoming.record_hash` rewrites a row only when its content really changed, so a rerun of unchanged data affects 0 rows instead of rewriting everything or inserting duplicates.

**Exclude** values that change on every run without the business data changing: `pipeline_run_id`, `processed_at_utc`, `staged_at_utc`, and the hash itself. Including any of them would make every rerun look like "all rows changed". (`source_updated_at` is included on purpose: it is a property of the source record, not of the run.)

### 2. Why should raw data usually be preserved even when staging/curated outputs are sufficient for analytics?
Raw data is the unmodified record of what was received. If a cleaning rule, join or quarantine rule changes later, or a bug is found, staging and curated can be rebuilt from the same raw snapshot without asking the source again. Here each run copies the sources into its own `data/raw/run_id=<run_id>/` folder, so every run can be reproduced exactly.

### 3. What is the difference between a data-quality rejection and a system exception?
- **Data-quality rejection:** an expected defect in the data (negative price, quantity outside 1–20, unknown status, order for a customer or product that does not exist). The row is written to a quarantine Parquet file with a reason, and the run continues with the valid rows.
- **System exception:** the pipeline or its environment failed (database unreachable, a source file missing, disk full). The stage raises `PipelineStageError` carrying the stage name, the CLI exits with a non-zero code, and an orchestrator such as Airflow can retry or alert.

### 4. Why might Parquet outperform CSV for selected analytical workloads even if both contain the same rows?
Parquet stores data by column, compressed, with the schema and per-column statistics in the file. A reader can fetch only the columns it needs and skip blocks that cannot match a filter, and it does not re-parse text or re-infer types. CSV is plain text that must be read and parsed row by row. In the saved benchmark (`docs/evidence/report.md`, section 3) the Parquet file is much smaller than the CSV and reads faster on the measured machine; those figures are specific to that machine and dataset.

### 5. Why is a DAG that contains all transformation logic directly considered harder to maintain?
Business logic inside a DAG file couples orchestration to transformation: it can only be exercised by loading Airflow, it is harder to unit test, and an error in that code can stop the whole DAG file from parsing. Here the DAG (`dags/dss150p_pipeline.py`) only calls `python -m src.cli <stage>`; the stages live in `src/pipeline.py` and are covered by tests that run without Airflow.

### 6. How do retries interact with idempotency? Give an example where retries without idempotency cause damage.
A retry runs the same task again with the same inputs. If the task is idempotent, the end state is the same as after one success. If not, the retry repeats its side effects. Example: a load task using a plain `INSERT` without a unique key that fails after inserting half its rows; the retry inserts those rows again, creating duplicates and inflating counts. Here the load is an UPSERT on `order_id`, and Airflow retries reuse the same run id, whose files are written atomically, so a retry cannot duplicate rows or leave partial files.

### 7. What trade-off is introduced by partitioning too aggressively?
Many tiny partitions mean many tiny files: more directory listing and file opens, more metadata, and less benefit from compression and from reading large contiguous blocks, so reads can become slower than with fewer, larger files. Partitioning by year and month gave 21 partitions of 951 to 2,540 rows each for this dataset (`docs/evidence/logs/09_partition_write.txt`); partitioning by day or by order id would produce hundreds or tens of thousands of far smaller files.

### 8. How would you adapt the pipeline if the source became an API or database instead of local files?
Only the extract stage (`src/extract/files.py`) changes: replace the file copy with an API client or SQL query that reads incrementally (a watermark on `updated_at` or a page cursor) and saves the raw response into the same `data/raw/run_id=<run_id>/` folder. Staging, curated, loading, validation and the run-id/manifest handling stay as they are because they only read the raw snapshot. The extract step must handle network failures and rate limits, and record the watermark so a rerun can resume.
