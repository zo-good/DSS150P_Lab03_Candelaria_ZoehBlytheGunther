#!/usr/bin/env bash
# Regenerate ALL saved evidence (docs/evidence/logs and docs/evidence/benchmark)
# by really running the pipeline, then refresh the report tables.
#
#   bash scripts/collect_evidence.sh
#
# Needs: a Python environment with requirements-dev.txt installed, PostgreSQL
# reachable with the settings in .env (docker compose up -d postgres), and
# `psql` on PATH. Works in Git Bash / WSL / Linux / macOS. WARNING: it empties
# curated.sales_order_lines and audit.partition_loads so the counts start clean.
set -u
cd "$(dirname "$0")/.."
PY="${PYTHON:-python}"
LOGS=docs/evidence/logs
RUN=evidence_run
mkdir -p "$LOGS" docs/evidence/benchmark
rm -f "$LOGS"/*.txt "$LOGS"/*.json
set -a; [ -f .env ] && . ./.env; set +a
export PGPASSWORD="${POSTGRES_PASSWORD:-change_me}"
psql_q() { psql -h "${POSTGRES_HOST:-localhost}" -p "${POSTGRES_PORT:-5432}" -U "${POSTGRES_USER:-dss150p}" -d "${POSTGRES_DB:-dss150p}" "$@"; }

# cap <log name> <command...>: save "$ command", its output, and its exit code.
cap() { local name=$1; shift; { echo "\$ $*"; "$@" 2>&1; echo "[exit code: $?]"; } > "$LOGS/$name.txt"; }
# cap_sql <log name> <sql>
cap_sql() { local name=$1; shift; { echo "\$ psql -c \"$*\""; psql_q -c "$*" 2>&1; echo "[exit code: $?]"; } > "$LOGS/$name.txt"; }
# cap_multi <log name>: command lines on stdin as "label|||command"
cap_multi() {
  local name=$1; : > "$LOGS/$name.txt"
  while IFS= read -r line; do
    [ -z "$line" ] && continue
    local label="${line%%|||*}" cmd="${line#*|||}"
    { echo "\$ $label"; eval "$cmd" 2>&1; echo "[exit code: $?]"; } >> "$LOGS/$name.txt"
  done
}

# Start from clean state for this evidence run.
rm -rf state data/raw data/staging data/curated data/quarantine data/benchmarks data/partitioned
psql_q -qc "TRUNCATE curated.sales_order_lines, audit.partition_loads, audit.pipeline_runs" || exit 1

cap 01_validate_env  $PY -m src.cli validate-env
cap 02_run_all       $PY -m src.cli run-all --run-id $RUN
cp "state/runs/$RUN.json" "$LOGS/manifest_after_run_all.json"
cap 03_rerun_load    $PY -m src.cli load --run-id $RUN
cap_sql 04_db_counts "SELECT count(*) AS total_rows, count(DISTINCT order_id) AS distinct_order_ids FROM curated.sales_order_lines"
cap 05_validate      $PY -m src.cli validate --run-id $RUN
cap 06_quarantine_reasons $PY scripts/quarantine_summary.py --run-id $RUN

# Overlapping runs: three full runs started at the same moment, empty table.
psql_q -qc "TRUNCATE curated.sales_order_lines, audit.partition_loads"
{
  echo '$ psql -c "TRUNCATE curated.sales_order_lines"   # start from an empty table'
  echo "\$ ($PY -m src.cli run-all --run-id conc_c & ... conc_d & ... conc_e & wait)" | sed "s#$PY#python#"
  for id in conc_c conc_d conc_e; do
    ( $PY -m src.cli run-all --run-id $id > "/tmp/$id.out" 2>&1; echo "[$id exit code: $?]" >> "/tmp/$id.out" ) &
  done
  wait
  for id in conc_c conc_d conc_e; do
    echo "--- $id ---"; grep -E "run_id=|Rows affected|Validation|completed|exit code|ERROR" "/tmp/$id.out" | sed "s#$PWD/##"
  done
  echo '--- per-run files ---'
  ls data/curated  | grep conc_ | sed 's#^#data/curated/#'
  ls data/staging  | grep conc_ | sed 's#^#data/staging/#'
  ls state/runs    | grep conc_ | sed 's#^#state/runs/#'
} > "$LOGS/07_concurrent_runs.txt" 2>&1
cap_sql 08_db_counts_after_concurrent "SELECT count(*) AS total_rows, count(DISTINCT order_id) AS distinct_order_ids, count(DISTINCT pipeline_run_id) AS runs_that_wrote_rows FROM curated.sales_order_lines"

# Partitioned Parquet: write, read one month, verify.
cap 09_partition_write $PY -m src.cli partition-write --run-id $RUN
cap 10_partition_read  $PY -m src.cli partition-read  --run-id $RUN --year 2025 --month 5

# Selected-partition load into an empty table.
psql_q -qc "TRUNCATE curated.sales_order_lines, audit.partition_loads"
cap_multi 11_partition_load <<EOF
psql -c "TRUNCATE curated.sales_order_lines, audit.partition_loads"   # start from an empty table|||true
python -m src.cli load-partition --run-id $RUN --year 2025 --month 5|||$PY -m src.cli load-partition --run-id $RUN --year 2025 --month 5
python -m src.cli load-partition --run-id $RUN --year 2025 --month 5   # rerun|||$PY -m src.cli load-partition --run-id $RUN --year 2025 --month 5
python -m src.cli validate --run-id $RUN   # scoped to the loaded month|||$PY -m src.cli validate --run-id $RUN
EOF
cap_sql 12_partition_db_verification "SELECT to_char(order_timestamp AT TIME ZONE 'UTC','YYYY-MM') AS month_utc, count(*) AS rows FROM curated.sales_order_lines GROUP BY 1 ORDER BY 1"
cap_sql 13_partition_audit "SELECT partition_key, row_count, pipeline_run_id FROM audit.partition_loads"

# Error handling: a system failure is reported with its stage and a non-zero exit code.
cap_multi 14_failure_handling <<EOF
POSTGRES_PORT=1 python -m src.cli load --run-id $RUN   # nothing listens on port 1|||POSTGRES_PORT=1 $PY -m src.cli load --run-id $RUN
POSTGRES_PORT=abc python -m src.cli validate-env --skip-db|||POSTGRES_PORT=abc $PY -m src.cli validate-env --skip-db
python -m src.cli transform --run-id never_extracted|||$PY -m src.cli transform --run-id never_extracted
EOF

# Benchmark (5 repeats + warm-up) on the full run; results copied into docs/evidence/benchmark.
cap 15_benchmark $PY -m src.cli benchmark --run-id $RUN --repeats 5 --evidence-dir docs/evidence/benchmark

# Refresh the numeric tables in report.md from the files saved above, then run the tests
# (one of them checks that report.md matches the saved results).
$PY -m src.report_tables
cap 16_pytest $PY -m pytest -v

# Leave the database holding the full dataset again.
$PY -m src.cli load --run-id $RUN > /dev/null 2>&1
echo "Evidence regenerated in $LOGS and docs/evidence/benchmark"
