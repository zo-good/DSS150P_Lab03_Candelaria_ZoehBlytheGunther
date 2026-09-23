### 1. Why is `record_hash` useful for rerun-safe loading, and which columns should not be included in it?
`record_hash` acts as a unique digital fingerprint for a record's core business attributes, allowing `UPSERT` operations (`ON CONFLICT DO UPDATE`) to detect modifications and avoid inserting duplicates during reruns. 

**Exclude:** Volatile metadata such as execution timestamps (`loaded_at`), execution IDs (`run_id`), or auto-incrementing database primary keys.

### 2. Why should raw data usually be preserved even when staging/curated outputs are sufficient for analytics?
Raw data serves as the immutable single source of truth. If business rules, cleaning logic, or quarantine formulas change later, having the original raw data allows you to reprocess and rebuild downstream datasets without losing historical data.

### 3. What is the difference between a data-quality rejection and a system exception?
- **Data-Quality Rejection:** An expected business data defect (e.g., missing mandatory field, invalid data type). Handled gracefully by routing bad records to a quarantine table/folder while processing valid records.
- **System Exception:** An unexpected infrastructure or runtime failure (e.g., lost database connection, full disk). Halts execution and triggers Airflow retries or alerts.

### 4. Why might Parquet outperform CSV for selected analytical workloads even if both contain the same rows?
Parquet uses columnar storage, binary compression, and embedded metadata. When querying a subset of columns, query engines read only the required columns off disk (column projection) and skip irrelevant data blocks using embedded min/max statistics, whereas CSV requires reading and parsing the entire file line-by-line.

### 5. Why is a DAG that contains all transformation logic directly considered harder to maintain?
Putting data transformation code inside DAG files tightly couples orchestration with business logic. It makes local unit testing impossible without instantiating Airflow, and a bug in the transformation logic can break DAG parsing for the scheduler.

### 6. How do retries interact with idempotency? Give an example where retries without idempotency cause damage.
Retries automatically re-run a task upon failure. If a task is idempotent, repeating it produces the exact same end state. If it is not idempotent, retries duplicate actions and corrupt state.

*Example:* Retrying a task that uses a standard `INSERT` query without unique key constraints will insert all records a second time, duplicating data and inflating metrics.

### 7. What trade-off is introduced by partitioning too aggressively?
Aggressive partitioning causes the **Small File Problem**. Creating thousands of tiny partition files introduces significant file system overhead from directory scanning and file handle allocations, ultimately degrading read performance.

### 8. How would you adapt the pipeline if the source became an API or database instead of local files?
- **Extraction:** Replace file readers with HTTP API clients (`requests`/`httpx`) or direct SQL database connections (`SQLAlchemy`).
- **Ingestion:** Implement pagination or timestamp watermarking queries to fetch incremental updates.
- **Storage:** Save raw API payloads directly into `data/raw/` first before executing downstream staging and curated transformations.