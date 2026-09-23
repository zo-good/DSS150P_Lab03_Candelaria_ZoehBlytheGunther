# Laboratory Activity #3: End-to-End Data Pipeline Report

## 1. Goal 1 Evidence: Environment & Configuration

### External Configuration & Network Routing
The pipeline uses a dual-layer configuration design:
- **`config/settings.yml`**: Manages environment-agnostic settings, schema definitions, directory templates, and threshold parameters.
- **`.env`**: Stores sensitive database credentials and dynamic execution variables.
- **Network Routing**: Local CLI execution connects to PostgreSQL via `localhost:5432`. Airflow inside Docker routes via internal DNS to `postgres:5432`. Endpoints are dynamically resolved using the `POSTGRES_HOST` environment variable to avoid hardcoding values.

## 2. Goal 2 Evidence: Data Quality & Rerun-Safe Loading

### Pipeline Execution Summary
- **Raw Snapshot:** Saved under `data/raw/run_id=<RUN_ID>/`
- **Extracted Records:** Customers (3,000), Products (599), Orders (49,998)
- **Quarantine Counts:**
  - `staging_quarantine`: 3 records rejected (schema/data type mismatches)
  - `curated_quarantine`: 101 records rejected (orphan foreign keys failing relational checks)
  - `curated`: 49,897 valid records processed

### Audit Columns & Rerun Safety
Each record in curated and PostgreSQL storage contains three audit fields:
1. `record_hash`: Cryptographic MD5 hash of core business attributes used for deduplication.
2. `run_id`: Dynamic batch execution ID.
3. `loaded_at`: UTC load timestamp.

Executing `python -m src.cli load` multiple times triggers PostgreSQL `UPSERT` statements (`ON CONFLICT (record_hash) DO UPDATE`), updating existing records without generating duplicates. The total row count in PostgreSQL remains strictly constant at **49,897**.

## 3. Goal 3 Evidence: Storage & Performance Benchmarks

### Benchmark Results Table

| Storage Format | File Size (Bytes) | File Size (MB) | Write Time (s) | Full Read Time (s) | Filtered Read Time (s) | Total Row Count |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **CSV** | 15,276,675 | ~15.28 MB | 0.7401 | 0.1870 | 0.1962 | 49,897 |
| **JSON Lines** | 30,707,468 | ~30.71 MB | 0.5310 | 0.5107 | 0.5277 | 49,897 |
| **Parquet** | 5,458,388 | ~5.46 MB | 0.0904 | 0.0380 | 0.0404 | 49,897 |
| **PostgreSQL** | 16,564,224 | ~16.56 MB | N/A | 0.3276 | 0.0494 | 49,897 |

### Performance Analysis
- **Footprint:** Parquet achieved a **64.3% space reduction vs CSV** and **82.2% vs JSON Lines**.
- **Speed:** Parquet recorded the fastest write (0.0904s) and full read times (0.0380s).
- **Filtered Read:** PostgreSQL B-Tree indexing optimized filtered query scans down to **0.0494s** from 0.3276s.
- **Partitioning:** Data is organized in Hive format under `data/partitioned/sales_order_lines/year=YYYY/month=MM/`, enabling target partition pruning during queries.


## 4. Technical Reflection

- **Modularity:** Separates orchestration (`dags/`) from business code (`src/`), allowing individual CLI command testing (`src/cli.py`) without running an Airflow scheduler.
- **Idempotency:** Utilizes cryptographic `record_hash` keys and `UPSERT` database logic to ensure multiple runs produce identical states without duplicate records.
- **Storage Trade-offs:** Plaintext (CSV/JSONL) offers human readability at the cost of high disk and I/O overhead; Parquet delivers columnar compression for analytical queries; PostgreSQL provides transactional integrity and indexed point lookups.
- **Orchestration vs. Business Logic:** Airflow manages workflow dependencies, retries, timeouts, and callbacks, while standalone Python scripts handle data parsing, cleaning, and database loading.

## 5. AI Tool Use and Academic Integrity

In accordance with course policies on academic integrity and Generative AI usage, AI tools (Gemini) were used strictly as an auxiliary aid for brainstorming, technical guidance, code debugging, and documentation refinement.

### Summary of AI Assistance
* **Code & Syntax Assistance:** Debugging local Virtual Environment configurations, resolving Git branch merge conflicts, and troubleshooting pipeline execution commands.
* **Terminal & Log Analysis:** Reading and interpreting terminal outputs, pipeline error stack traces, and storage benchmark metrics.
* **Evidence Collection Guidance:** Structuring the final submission deliverables and organizing required project evidence within VS Code.
* **Writing Refinement:** Refining written responses for the technical reflections and polishing answers to the technical questions for clarity and conciseness.
