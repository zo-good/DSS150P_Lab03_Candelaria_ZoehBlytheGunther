"""The evidence report must agree with the code and with the saved execution results."""
import json
import re
from pathlib import Path

import pandas as pd
import pytest

from src import config, report_tables
from src.common import audit, env_check
from src.transform.curated import HASH_KEYS, KEEP_COLUMNS

REPO = Path(__file__).resolve().parents[1]
EVIDENCE = REPO / 'docs' / 'evidence'
LOGS = EVIDENCE / 'logs'
REPORT = (EVIDENCE / 'report.md').read_text(encoding='utf-8')
CODE = lambda rel: (REPO / rel).read_text(encoding='utf-8')


def log(name):
    return (LOGS / name).read_text(encoding='utf-8')


def n(value):
    return f'{int(value):,}'


# ---------------------------------------------------- tables match saved results
def test_generated_tables_match_the_saved_results():
    assert report_tables.main(['--check']) == 0


def test_every_generated_block_is_filled():
    for name in ('pipeline_counts', 'quarantine_reasons', 'partition_load', 'partition_facts', 'benchmark'):
        body = re.search(rf'<!-- BEGIN:{name} -->\n(.*?)\n<!-- END:{name} -->', REPORT, re.DOTALL).group(1)
        assert '|' in body and len(body) > 100, name


# -------------------------------------------------- stale / wrong claims are gone
@pytest.mark.parametrize('stale', [
    'MD5', 'ON CONFLICT (record_hash)', 'B-Tree', 'B-tree', 'year=YYYY/month=MM',
    '`loaded_at`', '`run_id`: Dynamic', 'schema/data type mismatches', 'Extracted Records',
    'current_run_id.txt',
])
def test_known_wrong_claims_do_not_appear(stale):
    assert stale not in REPORT


# ------------------------------------------------------ claims match the code
def test_hash_algorithm_claim():
    assert 'SHA-256' in REPORT and 'hashlib.sha256' in CODE('src/common/audit.py')
    assert 'md5' not in CODE('src/common/audit.py').lower()
    assert len(HASH_KEYS) == 14 and '14 business columns' in REPORT
    assert set(HASH_KEYS).isdisjoint({'pipeline_run_id', 'processed_at_utc', 'staged_at_utc', 'record_hash'})


def test_upsert_claim():
    sql = CODE('src/load/postgres.py')
    assert 'ON CONFLICT (order_id) DO UPDATE' in sql and 'ON CONFLICT (order_id) DO UPDATE' in REPORT
    assert 'record_hash <> EXCLUDED.record_hash' in sql
    assert 'EXCLUDED.source_updated_at >= curated.sales_order_lines.source_updated_at' in sql
    assert 'pg_advisory_xact_lock' in sql and 'advisory lock' in REPORT


def test_audit_columns_named_in_the_report_exist():
    for column in ('source_updated_at', 'pipeline_run_id', 'processed_at_utc', 'record_hash'):
        assert column in KEEP_COLUMNS and column in CODE('sql/init/01_warehouse_schema.sql') and column in REPORT
    assert 'staged_at_utc' in CODE('src/transform/staging.py')


def test_no_index_on_status_so_no_index_claim():
    schema = CODE('sql/init/01_warehouse_schema.sql').upper()
    assert 'CREATE INDEX' not in schema and 'CREATE UNIQUE INDEX' not in schema
    assert 'no index on `status`' in REPORT.lower().replace('**', '')


def test_settings_cited_in_the_report_are_the_real_ones():
    q, b = config.SETTINGS['quality'], config.SETTINGS['storage_benchmark']
    assert f'quantity {q["min_quantity"]}–{q["max_quantity"]}' in REPORT
    assert len(q['allowed_order_statuses']) == 6 and 'six allowed order statuses' in REPORT
    assert f'`repeats: {b["repeats"]}`' in REPORT and f'`filter_status: {b["filter_status"]}`' in REPORT


def test_dag_facts_in_the_report_match_the_dag_file():
    dag = CODE('dags/dss150p_pipeline.py')
    assert 'dag_id="dss150p_sales_pipeline"' in dag and 'dss150p_sales_pipeline' in REPORT
    assert 'schedule="0 2 * * *"' in dag and '`0 2 * * *`' in REPORT
    assert '"retries": 2' in dag and 'retries=2' in REPORT and 'tried 3 times' in REPORT
    assert 'dagrun_timeout=timedelta(minutes=30)' in dag and '30-minute `dagrun_timeout`' in REPORT
    assert dag.count('execution_timeout=timedelta(minutes=10)') == 4 and '10-minute `execution_timeout`' in REPORT
    assert 'extract >> transform >> load >> validate' in dag and 'extract >> transform >> load >> validate' in REPORT
    assert 'enum=["full", "partition"]' in dag.replace("\n", "").replace(" ", "").replace('enum=["full","partition"]', 'enum=["full", "partition"]')


def test_partition_layout_and_validation_checks_match_the_code():
    assert 'order_year=YYYY/order_month=M' in REPORT
    code = CODE('src/benchmark/partitions.py')
    assert 'PARTITION_COLUMNS = [\'order_year\', \'order_month\']' in code
    assert "AT TIME ZONE 'UTC'" in CODE('src/load/postgres.py')
    assert 'Asia/Manila' in CODE('tests/test_loading.py') and 'Asia/Manila' in REPORT


def test_validate_env_claims_match_the_checks():
    for table in env_check.REQUIRED_TABLES:
        assert f'`{table}`' in REPORT
    for var in config.REQUIRED_ENV_VARS:
        assert var.removeprefix('POSTGRES_') in REPORT or var in REPORT


def test_env_example_is_complete_and_holds_only_placeholders():
    text = CODE('.env.example')
    for var in config.REQUIRED_ENV_VARS:
        assert re.search(rf'^{var}=', text, re.MULTILINE), var
    assert 'POSTGRES_PASSWORD=change_me' in text
    assert re.search(r'^\.env$', CODE('.gitignore'), re.MULTILINE)
    assert not re.search(r'^\.env\.example', CODE('.gitignore'), re.MULTILINE)


# ------------------------------------------- numbers in prose match saved logs
def test_validate_env_numbers_match_the_log():
    last = log('01_validate_env.txt').splitlines()[-2]
    ok, warn, fail = map(int, re.search(r'(\d+) ok, (\d+) warning\(s\), (\d+) failure\(s\)', last).groups())
    assert f'{ok} ok, {warn} warning for the placeholder password, {fail} failures' in REPORT


def test_idempotency_numbers_match_the_logs():
    assert re.search(r'Rows affected \(inserted or updated\): 0\n', log('03_rerun_load.txt'))
    counts = re.search(r'^\s*(\d+)\s*\|\s*(\d+)\s*$', log('04_db_counts.txt'), re.MULTILINE).groups()
    run = report_tables.parse_run_all(log('02_run_all.txt'))
    assert counts[0] == counts[1] == str(run['curated']) == str(run['first_load_affected'])
    assert f'**{n(counts[0])}** rows with **{n(counts[1])}** distinct' in REPORT
    assert '**0** rows affected' in REPORT


def test_concurrency_numbers_match_the_logs():
    text = log('07_concurrent_runs.txt')
    affected = [int(x) for x in re.findall(r'Rows affected \(inserted or updated\): (\d+)', text)]
    run = report_tables.parse_run_all(log('02_run_all.txt'))
    assert len(affected) == 3 and sorted(affected) == [0, 0, run['curated']]
    assert text.count('[conc_') == 3 and text.count('exit code: 0]') == 3
    assert text.count('Validation PASSED') == 3
    final = re.search(r'^\s*(\d+)\s*\|\s*(\d+)\s*\|\s*(\d+)\s*$', log('08_db_counts_after_concurrent.txt'), re.MULTILINE).groups()
    assert final[0] == final[1] == str(run['curated']) and final[2] == '1'
    assert 'three full pipeline runs' in REPORT and f'{n(run["curated"])} rows, {n(run["curated"])} distinct' in REPORT


def test_failure_handling_log_shows_what_the_report_says():
    text = log('14_failure_handling.txt')
    assert 'ERROR [load] OperationalError' in text
    assert '[FAIL] POSTGRES_PORT' in text and 'Traceback' not in text
    assert 'run extract first' in text
    assert text.count('[exit code: 1]') == 3


def test_partition_numbers_match_the_logs_and_the_technical_answers():
    rows = [int(r) for r in re.findall(r'^\s*\d{4}\s+\d+\s+1\s+(\d+)$', log('09_partition_write.txt'), re.MULTILINE)]
    partitions = int(re.search(r'^(\d+) partitions', log('09_partition_write.txt'), re.MULTILINE).group(1))
    assert partitions == len(rows) == 21
    questions = CODE('docs/technical_questions.md')
    assert f'{partitions} partitions of {n(min(rows))} to {n(max(rows))} rows' in questions


def test_benchmark_is_complete_and_repeatable_on_record():
    results = pd.read_csv(EVIDENCE / 'benchmark' / 'benchmark_results.csv')
    assert list(results['storage_type'][:4]) == ['CSV', 'JSON Lines', 'Parquet', 'PostgreSQL']
    assert set(results['repeats']) == {5} and set(results['row_count']) == {results['row_count'].iloc[0]}
    timings = json.loads((EVIDENCE / 'benchmark' / 'benchmark_timings.json').read_text())
    assert all(len(v) == 5 for metrics in timings['timings_seconds'].values() for v in metrics.values())
    context = json.loads((EVIDENCE / 'benchmark' / 'benchmark_context.json').read_text())
    assert {'os', 'cpu_model', 'cpu_count', 'ram_bytes', 'python', 'pandas', 'pyarrow'} <= set(context)
    assert timings['partition_verification']['pruned'] is True


# ------------------------------------------------------ cited files exist
def test_every_evidence_file_named_in_the_report_exists():
    named = set(re.findall(r'`?(logs/[\w.]+\.(?:txt|json))`?', REPORT)) | set(re.findall(r'logs/(\d\d_)…', REPORT))
    for name in (m for m in named if m.startswith('logs/')):
        assert (EVIDENCE / name).is_file(), name
    for name in re.findall(r'`(\w+\.png)`', REPORT):
        assert (EVIDENCE / 'screenshots' / name).is_file(), name
    for name in re.findall(r'`(benchmark/[\w.]+)`', REPORT):
        assert (EVIDENCE / name).is_file(), name
