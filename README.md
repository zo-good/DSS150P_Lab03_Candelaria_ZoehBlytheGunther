# DSS150P Laboratory 3 Starter Repository

This repository supports Module 2: Pipeline Construction, Storage, and Orchestration.
It is intentionally incomplete. Students must implement the marked TODOs and document their decisions.

## Main progression
- Goal 1: reproducible environment, modularization, Git, Docker, configuration
- Goal 2: raw -> staging -> curated transformations; audit/error handling; rerun-safe loading
- Goal 3: CSV/JSON/Parquet/PostgreSQL comparison; partitioning; selected-partition load
- Goal 4: Apache Airflow DAG for extract -> transform -> load -> validate

Start with `DSS150P_Laboratory_Activity_3.pdf`.

## Recommended commands
```bash
cp .env.example .env
python -m venv .venv
# activate .venv then:
pip install -r requirements.txt
python -m src.cli validate-env
```
The provided `.env.example` uses `POSTGRES_HOST=localhost` for host-side commands. Docker Compose overrides the application containers to use the service hostname `postgres`.

Docker/PostgreSQL:
```bash
docker compose up -d postgres
docker compose run --rm pipeline python -m src.cli validate-env
```

Airflow in Goal 4:
```bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up airflow-init
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d airflow-webserver airflow-scheduler
```
Airflow UI: http://localhost:8080 (training credentials: admin/admin; change if reused outside the lab).

---

## Running the pipeline (after the review fixes)

```bash
cp .env.example .env                      # PowerShell: Copy-Item .env.example .env
pip install -r requirements-dev.txt       # app requirements + pytest
docker compose up -d postgres
python -m src.cli validate-env            # real checks; exit code 1 if anything is wrong

python -m src.cli run-all --run-id my_run # extract -> transform -> load -> validate
python -m src.cli load --run-id my_run    # rerun: 0 rows affected
python -m src.cli partition-write --run-id my_run
python -m src.cli partition-read  --run-id my_run --year 2025 --month 5
python -m src.cli load-partition  --run-id my_run --year 2025 --month 5
python -m src.cli benchmark       --run-id my_run --repeats 5
```

### Run ids and run state
* Every stage takes `--run-id` (or reads `PIPELINE_RUN_ID`, which the Airflow DAG sets). If neither is given, manual commands fall back to the most recent `extract` recorded in `state/latest_run.txt`; **scheduled or concurrent runs should always pass an explicit id.**
* Run ids are sanitised for file names (`:` `+` `/` and spaces become `_`), so Airflow ids such as `manual__2026-09-23T12:53:45+00:00` work on Windows too.
* All outputs are per run (`data/<layer>/run_id=…`), written atomically; `state/runs/<run_id>.json` records each stage's status and counts. Nothing in `state/` is committed.

### Tests
```bash
pytest                # 130+ tests; the PostgreSQL ones use a throwaway database and skip if no server is reachable
```

### Evidence
`docs/evidence/report.md` is the evidence report. Its numeric tables are generated from saved results:
```bash
bash scripts/collect_evidence.sh          # re-run everything and re-save logs + benchmark (empties curated.sales_order_lines)
python -m src.report_tables               # refresh the report tables from the saved files
python -m src.report_tables --check       # fails if the report no longer matches the saved results
```
`docs/review_response.md` maps each review comment to the change that addresses it.
