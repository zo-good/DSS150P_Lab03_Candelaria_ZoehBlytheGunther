# Laboratory Activity #3: End-to-End Data Pipeline — Evidence Report

**How to read this report.** Every number in a table below is written by `python -m src.report_tables` from a file saved under `docs/evidence/` (execution logs in `logs/`, benchmark results in `benchmark/`). The text between `<!-- BEGIN:... -->` and `<!-- END:... -->` markers is machine-generated; a test (`tests/test_report_consistency.py`) fails if the report and the saved results disagree. Statements about behaviour name the source file that implements them. Section 6 lists what was **not** verified.

---

## 1. Goal 1 — Environment and configuration

**Configuration layers.**
- `config/settings.yml` holds non-secret settings: folder layout, quality thresholds (quantity 1–20, six allowed order statuses), benchmark settings (`repeats: 5`, `filter_status: DELIVERED`).
- `.env` holds the PostgreSQL connection values (`POSTGRES_HOST/PORT/DB/USER/PASSWORD`). It is ignored by Git (`.gitignore`); the committed template is **`.env.example`**, which contains placeholders only.
- Host-side commands reach PostgreSQL on `localhost` (from `.env`). `docker-compose.yml` and `docker-compose.airflow.yml` set `POSTGRES_HOST: postgres` for the containers, so the same code reaches the database by its Compose service name.

**`validate-env` performs real checks** (`src/common/env_check.py`) and exits with code 1 if any fails:
`settings.yml` keys and value sanity; all five environment variables present, non-empty, port in 1–65535 (placeholder password is a warning only); the three source files exist, are non-empty and have the expected columns; every output folder is creatable and writable (a probe file is written and removed); PostgreSQL accepts a connection and the tables `curated.sales_order_lines`, `audit.partition_loads`, `audit.pipeline_runs` exist. The password is never printed.

- Healthy run: `logs/01_validate_env.txt` (19 ok, 1 warning for the placeholder password, 0 failures, exit code 0).
- Failing run: `logs/14_failure_handling.txt` shows `POSTGRES_PORT=abc` producing `[FAIL] POSTGRES_PORT` and a non-zero exit code.
- Each failure mode has an automated test in `tests/test_env_validation.py`.

Python version and pinned packages: `docs/evidence/environment_verification.md`.

---

## 2. Goal 2 — Data quality, run state and rerun-safe loading

### 2.1 Row counts of the full run

<!-- BEGIN:pipeline_counts -->
Run `evidence_run` (from `docs/evidence/logs/02_run_all.txt`):

| Stage | customers | products | orders |
| :--- | ---: | ---: | ---: |
| Raw rows read from the source files | 3,003 | 601 | 50,005 |
| Valid rows in staging (after removing superseded duplicates and quarantined rows) | 3,000 | 599 | 49,998 |

| Result | Rows |
| :--- | ---: |
| Staging quarantine (rejected by cleaning rules) | 3 |
| Curated (valid rows after joining orders to customers and products) | 49,897 |
| Curated quarantine (orders whose customer or product does not exist) | 101 |
| First load: rows inserted or updated in PostgreSQL | 49,897 |
<!-- END:pipeline_counts -->

"Raw rows" are physical rows in the source files before any cleaning (`count_raw_rows` in `src/transform/staging.py`). The difference between raw and valid staging rows is superseded duplicate versions (the latest `updated_at` wins) plus quarantined rows.

### 2.2 Why rows were quarantined

Data defects are routed to quarantine files (`data/quarantine/`) and never stop the run; only system problems raise exceptions (section 2.5). Saved reasons (`logs/06_quarantine_reasons.txt`):

<!-- BEGIN:quarantine_reasons -->
| Quarantine file | Source | Reason | Rows |
| :--- | :--- | :--- | ---: |
| staging (3 rows) | orders | quantity outside 1..20 | 1 |
| staging (3 rows) | orders | status not in allowed set | 1 |
| staging (3 rows) | products | invalid or negative unit_price | 1 |
| curated (101 rows) | curated_join | customer_id <id> not found in customers | 1 |
| curated (101 rows) | curated_join | product_id <id> not found in products | 100 |
<!-- END:quarantine_reasons -->

### 2.3 Audit columns, `record_hash` and the UPSERT

- Curated rows and the PostgreSQL table carry four audit/lineage columns: `source_updated_at`, `pipeline_run_id`, `processed_at_utc`, `record_hash` (`KEEP_COLUMNS` in `src/transform/curated.py`, `sql/init/01_warehouse_schema.sql`). Staging files carry `pipeline_run_id` and `staged_at_utc`.
- `record_hash` is a **SHA-256** digest (`src/common/audit.py`) of the 14 business columns listed in `HASH_KEYS`. It deliberately excludes `pipeline_run_id`, `processed_at_utc` and `staged_at_utc`, so the same business row hashes identically in every run (tested in `tests/test_transform_rules.py`).
- Loading (`src/load/postgres.py`) uses `INSERT ... ON CONFLICT (order_id) DO UPDATE ... WHERE` with two guards: the stored `record_hash` differs from the incoming one (unchanged rows are not rewritten) **and** the incoming `source_updated_at` is not older than the stored one (a delayed or out-of-order run cannot overwrite newer data).
- A batch is one transaction (all rows or none), and loads take a PostgreSQL advisory lock for the length of the transaction so concurrent loaders cannot interleave or deadlock.
- Repeating the load changed nothing: `logs/03_rerun_load.txt` reports **0** rows affected, and `logs/04_db_counts.txt` shows the table holding **49,897** rows with **49,897** distinct `order_id` values — the same as the first load in section 2.1.

### 2.4 Safe concurrent and repeated runs

Earlier code shared mutable files between runs (a single "current run id" file tracked in Git, fixed staging file names, fixed benchmark file names). The fixes, in `src/common/runtime.py` and `src/pipeline.py`:

| Hazard | Fix |
| :--- | :--- |
| Stages learned their run id from one shared file; overlapping runs could read each other's id | Every stage takes `--run-id` (or `PIPELINE_RUN_ID`, which the Airflow DAG already sets). The shared file is no longer used or tracked. `state/latest_run.txt` remains only as a convenience for typing commands by hand, updated under a lock. |
| Staging, benchmark and partition outputs were written to fixed paths that overlapping runs could overwrite | All outputs live under the run's own id: `data/raw/run_id=…`, `data/staging/run_id=…/`, `data/curated/sales_order_lines_run_id=….parquet`, `data/quarantine/…_run_id=….parquet`, `data/benchmarks/run_id=…/`, `data/partitioned/run_id=…/`. |
| A crash mid-write could leave half-written files | Files are written to a hidden temporary name and moved into place with `os.replace`; the partitioned dataset is written to a temporary folder and swapped in. |
| Airflow run ids (`manual__2026-09-23T12:53:45+00:00`) contain `:` and `+`, invalid in Windows paths | `sanitize_run_id` maps every character outside `[A-Za-z0-9_.=-]` to `_`, and the same id is used for paths and for `pipeline_run_id` in PostgreSQL. |
| There was no per-run record of stage status, counts or errors | Each run has its own manifest `state/runs/<run_id>.json` (stage, status, counts, error), updated under a lock. Example: `logs/manifest_after_run_all.json`. |
| Writing the partitioned dataset twice into the same folder appended a second set of files, doubling the rows on read (demonstrated in `tests/test_partitions.py`: 3 rows written, 6 read back) | Re-writing replaces the dataset (written to a temporary folder, then swapped in); tested by writing twice in a row and by simulated crashes (`tests/test_partitions.py`). |

**Evidence.** `logs/07_concurrent_runs.txt`: three full pipeline runs started at the same moment (run ids `conc_c`, `conc_d`, `conc_e`) against an **empty** table all exit 0 and pass validation; exactly one of them inserted the 49,897 rows and the other two inserted 0; each run has its own files; `logs/08_db_counts_after_concurrent.txt` shows 49,897 rows, 49,897 distinct `order_id`s. Further automated tests: eight threads and four separate processes contending for the lock lose no updates; six overlapping runs with different ids and four overlapping runs with the *same* id (an Airflow retry) leave complete, readable, uncorrupted files and no temporary leftovers (`tests/test_run_state.py`); concurrent loads insert each row exactly once (`tests/test_loading.py`).

**Known limits.** Two runs sharing one run id at the same moment write the same files atomically, so files stay valid but the last writer wins. The lock is a lock *file*; a lock left behind by a crashed process is removed after 300 s, and a crash combined with two waiters breaking it in the same instant could let both proceed (it only guards bookkeeping files).

### 2.5 Error handling

- `run_stage` (`src/pipeline.py`) wraps every system failure in `PipelineStageError`, which carries the stage name and keeps the original exception as `__cause__`; the CLI prints `ERROR [stage] ExceptionType: message` to stderr and exits 1, and the run's manifest records the stage as `failed`.
- Demonstrated in `logs/14_failure_handling.txt`: an unreachable database fails in stage `load` (exit 1); a bad port fails `validate-env` (exit 1); transforming a run that was never extracted fails with `run extract first` (exit 1).
- Data defects do not raise; they are quarantined (section 2.2). `validate` additionally confirms that every loaded row is present in PostgreSQL, limited to what `load` actually loaded (the whole file after a full load, or one month after a partition load).
- Tests: `tests/test_error_handling.py`.

---

## 3. Goal 3 — Storage formats, partitioned reads and the benchmark

### 3.1 Method (what the numbers mean)

`src/benchmark/storage.py` benchmarks the run's curated data in CSV, JSON Lines, Parquet (snappy) and PostgreSQL:

- Every operation runs once un-timed (warm-up) and then **5 timed runs**; the table reports the **median**, and every individual timing is saved in `benchmark/benchmark_timings.json`. The CLI refuses fewer than 5 repeats.
- Reads are measured into a pandas DataFrame for every format, PostgreSQL included.
- Filtered read: CSV and JSON Lines must parse everything and then filter; Parquet pushes the filter into the reader; PostgreSQL uses `WHERE status = 'DELIVERED'`. There is **no index on `status`** (see `sql/init/01_warehouse_schema.sql`; only `order_id` is the primary key), so PostgreSQL's filtered read is a scan of the table, not an index lookup.
- PostgreSQL is measured on a temporary copy of the table (same columns and primary key) that holds exactly this run's rows, written with `COPY`; the real `curated.sales_order_lines` table is never touched. Its size is `pg_total_relation_size` (table plus primary-key index).
- After timing, every format is read back and its full and filtered row counts are compared with the source; any difference stops the benchmark with an error. The saved table's `Rows` and `Rows after filter` columns show the agreement.
- Hardware, OS and library versions are saved in `benchmark/benchmark_context.json` and shown below. Results are specific to that machine.

### 3.2 Results

<!-- BEGIN:benchmark -->
Measured on: Linux-6.18.44-fc-v114-x86_64-with-glibc2.39; Intel(R) Xeon(R) Processor @ 2.80GHz, 2 logical CPUs, 7.8 GiB RAM; Python 3.11.17, pandas 2.2.3, pyarrow 17.0.0, psycopg 3.2.3, PostgreSQL 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1). Each value is the median of 5 timed runs after 1 un-timed warm-up (measured 2026-10-11 04:48:18 UTC). The numbers describe this machine only; see `docs/evidence/benchmark/benchmark_timings.json` for every individual timing.

| Storage format | Size (bytes) | Size (MB) | Write (s) | Full read (s) | Filtered read (s) | Rows | Rows after filter |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| CSV | 14,378,528 | 14.38 | 0.9117 | 0.2409 | 0.2288 | 49,897 | 8,355 |
| JSON Lines | 29,859,219 | 29.86 | 0.6776 | 0.6332 | 0.6719 | 49,897 | 8,355 |
| Parquet | 5,458,269 | 5.46 | 0.0976 | 0.0582 | 0.0257 | 49,897 | 8,355 |
| PostgreSQL | 15,818,752 | 15.82 | 0.7596 | 1.2134 | 0.2068 | 49,897 | 8,355 |

Reading one month (2025-05):

| Approach | Size (bytes) | Write (s) | Read one month (s) | Rows returned |
| :--- | ---: | ---: | ---: | ---: |
| Parquet single file, one-month read | 5,458,269 | — | 0.0601 | 2,540 |
| Parquet partitioned, one-month read | 6,588,492 | 0.1123 | 0.0074 | 2,540 |

- Parquet file size is 62.0% smaller than CSV and 81.7% smaller than JSON Lines (5,458,269 vs 14,378,528 and 29,859,219 bytes).
- Fastest median full read: Parquet (0.0582 s); slowest: PostgreSQL (1.2134 s).
- Fastest median write among the file formats: Parquet (0.0976 s).
- One-month read: 0.0074 s from the partitioned dataset vs 0.0601 s from the single file (8.1x faster).
<!-- END:benchmark -->

Reading the table: the speed and size figures are measurements. The reasons are standard explanations, not measured here — CSV and JSON Lines are text that must be parsed on every read and repeat column names (JSON Lines) per row; Parquet is columnar, compressed and stores its schema. PostgreSQL figures include moving rows through the client driver into a DataFrame. Because `status` has no index, a faster filtered read in PostgreSQL than its full read reflects returning fewer rows, not indexing.

### 3.3 Partitioned Parquet and partition-pruned reads

Layout (`src/benchmark/partitions.py`): `data/partitioned/run_id=<run_id>/order_year=YYYY/order_month=M/<file>.parquet` (months are **not** zero-padded, e.g. `order_month=5`). Partition keys come from `order_timestamp` in **UTC**; the same function drives the Parquet write, the partition read and the PostgreSQL load, and boundary cases (`23:59:59Z` vs `00:00:00Z`, non-UTC offsets) are tested.

`partition-read` reads one partition and then **verifies** it (`verify_partition`): (1) only files under that `order_year/order_month` folder were selected; (2) fewer files were opened than exist, i.e. pruning really skipped data; (3) the row count equals the count computed independently from the curated file; (4) every returned row's timestamp falls in that month; (5) the returned `order_id` set equals the expected set. Any failure raises an error. Tests plant a foreign file in a partition and delete a partition file and confirm both are detected (`tests/test_partitions.py`).

<!-- BEGIN:partition_facts -->
| Item | Value |
| :--- | ---: |
| Partitions written (order_year / order_month) | 21 |
| Rows across all partitions | 49,897 |
| Partition read and verified | 2025-05 |
| Rows read / rows expected | 2,540 / 2,540 |
| Files opened / files in dataset | 1 / 21 |
| Pruning verified (other partitions never opened) | True |
<!-- END:partition_facts -->

(`logs/09_partition_write.txt`, `logs/10_partition_read.txt`.)

### 3.4 Selected-partition load into PostgreSQL

`load-partition` loads only the rows of one month (it refuses a frame containing any other month) and writes the upsert and the `audit.partition_loads` row in **one transaction**. Counting rows per month uses `AT TIME ZONE 'UTC'`, so the answer does not depend on the database session's time zone (tested with the database set to `Asia/Manila`).

<!-- BEGIN:partition_load -->
| Item | Value |
| :--- | ---: |
| Partition loaded | 2025-05 |
| First load: rows in partition / rows inserted or updated | 2,540 / 2,540 |
| Rerun of the same load: rows inserted or updated | 0 |
| Months present in PostgreSQL afterwards (verification query) | 2025-05 (2,540 rows) |
| `audit.partition_loads` row | 2025-05, row_count 2,540, run `evidence_run` |
| `validate` after the partition load | 49,897 rows checked; 2,540 confirmed in PostgreSQL (scope 2025-05) |
<!-- END:partition_load -->

Sources: `logs/11_partition_load.txt`, `logs/12_partition_db_verification.txt` (the PostgreSQL verification query, run against the table after the load) and `logs/13_partition_audit.txt`.

---

## 4. Goal 4 — Airflow orchestration

`dags/dss150p_pipeline.py` defines DAG `dss150p_sales_pipeline`: schedule `0 2 * * *`, `catchup=False`, `retries=2` with a 1-minute delay (so a failing task is tried 3 times), a 10-minute `execution_timeout` per task, a 30-minute `dagrun_timeout`, an `on_failure_callback` that prints dag, run, task and error, and parameters `run_mode` (`full` or `partition`), `year` and `month`. Tasks `extract >> transform >> load >> validate` are `BashOperator`s that call `python -m src.cli …` with `PIPELINE_RUN_ID` set to the Airflow run id; the DAG contains no business logic.

What the saved screenshots (`docs/evidence/screenshots/`) show — and no more:

| File | What it shows |
| :--- | :--- |
| `full_run.png` | Manual run `manual__2026-09-23T12:53:45+00:00` with `extract`, `transform`, `load`, `validate` all green; status *success*. |
| `partition_run.png` | Manual run `manual__2026-09-23T12:55:47+00:00`, status *success*, run duration 00:00:11. The screenshot does not show the run's parameters, so it does not by itself show that `run_mode=partition` was used; partition behaviour is evidenced by sections 3.3–3.4 and the tests. |
| `failed.png` | Run `manual__2026-09-23T13:00:29+00:00`, task `extract`, Task Tries 1–3 all red; the log shows `RuntimeError: Test failure` raised while importing `src/validate/quality.py` (a deliberately injected error) and the command exiting with code 1; the downstream tasks did not run. |
| `recovered.png` | The same run id, task `validate`, status *success* on try 5 after earlier red tries. |

---

## 5. Technical reflection

- **Modularity.** Orchestration (`dags/`) is separate from business code (`src/`). Stage logic lives in `src/pipeline.py` and is called by the CLI, the DAG (through the CLI) and the tests, so each stage can be run and tested without Airflow.
- **Idempotency.** A deterministic SHA-256 `record_hash` over business columns plus a conditional `ON CONFLICT (order_id)` UPSERT means a rerun changes nothing (0 rows affected, section 2.3), and the `source_updated_at` guard means a stale run cannot undo newer data.
- **Concurrency.** Safety comes from isolation (per-run paths and manifests), atomic file replacement, one transaction per batch and a database advisory lock for the shared table — not from hoping runs do not overlap.
- **Storage trade-offs.** See section 3.2: text formats are human-readable but larger and slower to parse; Parquet is compact and fast for analytical reads here; PostgreSQL adds transactions, constraints and queries but its read path goes through a client driver. These conclusions apply to this dataset on the measured machine.
- **Orchestration vs. business logic.** Airflow supplies scheduling, retries, timeouts and failure callbacks; the Python modules do the parsing, cleaning and loading.

---

## 6. What was not verified

- **Airflow was not re-run after the review fixes.** The DAG file is unchanged and calls the same CLI commands, which now resolve the run id from `PIPELINE_RUN_ID` and sanitise it; the screenshots in section 4 predate those fixes.
- **Operating systems.** The automated tests and saved logs were produced on Linux (see the "Measured on" line above). The code uses only portable primitives (`os.replace`, exclusive file creation, `pathlib`), but the lock and path handling have not been executed on Windows by these tests.
- **Benchmark numbers are machine-specific.** They are valid for the machine recorded above. To measure your own machine, run `python -m src.cli benchmark --run-id <id> --repeats 5 --evidence-dir docs/evidence/benchmark` and then `python -m src.report_tables`.
- The PostgreSQL used for the saved logs is the server named in the "Measured on" line, not necessarily the Docker Compose container.

---

## 7. Evidence index and reproduction

| Evidence | Proves |
| :--- | :--- |
| `logs/01_validate_env.txt` | `validate-env` checks (exit 0) |
| `logs/02_run_all.txt`, `logs/manifest_after_run_all.json` | Full run counts and run manifest |
| `logs/03_rerun_load.txt`, `logs/04_db_counts.txt` | Rerun loads 0 rows; table holds 49,897 distinct orders |
| `logs/05_validate.txt` | Validation including the PostgreSQL presence check |
| `logs/06_quarantine_reasons.txt` | Why rows were quarantined |
| `logs/07_concurrent_runs.txt`, `logs/08_db_counts_after_concurrent.txt` | Three overlapping runs: isolated files, one insert of 49,897 rows, no duplicates |
| `logs/09_…`, `logs/10_…` | Partitioned write; verified one-partition read |
| `logs/11_…` – `logs/13_…` | Selected-partition load, verification query, audit row |
| `logs/14_failure_handling.txt` | Stage-tagged failures and non-zero exit codes |
| `logs/15_benchmark.txt`, `benchmark/*` | Benchmark output, medians, raw timings, machine context |
| `logs/16_pytest.txt` | Full test-suite run |

Reproduce everything (empties `curated.sales_order_lines` first): `bash scripts/collect_evidence.sh`. Check the report against the saved results: `python -m src.report_tables --check`.

---

## 8. AI tool use and academic integrity

In accordance with course policies on academic integrity and Generative AI usage, AI tools were used as an auxiliary aid for guidance, debugging and documentation:

- **Gemini** — as previously declared: brainstorming, technical guidance, debugging local environment and Git issues, interpreting terminal output and benchmark metrics, and refining written answers.
- **Claude (Anthropic)** — step-by-step guidance and code for Laboratory 3 Goals 1–3, and the fixes made in response to the review comments (`.env.example` and `validate-env`, tests, concurrency-safe run state, partition reads and the benchmark harness, and the correction of this report against saved results).

All commands were run and all outputs saved as described in section 7; the author reviewed the resulting code and report.
