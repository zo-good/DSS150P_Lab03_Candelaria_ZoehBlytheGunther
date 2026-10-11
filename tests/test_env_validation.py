"""validate-env performs real checks and fails (exit code 1) when something is wrong."""
import pytest

from factories import cust, default_dataset, order, prod, write_source
from src import config
from src.cli import main
from src.common import env_check
from src.common.env_check import has_failures, run_checks

ENV = {'POSTGRES_HOST': 'localhost', 'POSTGRES_PORT': '5432', 'POSTGRES_DB': 'x',
       'POSTGRES_USER': 'u', 'POSTGRES_PASSWORD': 'a-real-secret'}


@pytest.fixture
def healthy(project, monkeypatch):
    for key, value in ENV.items():
        monkeypatch.setenv(key, value)
    write_source(project, *default_dataset())
    return project


def by_name(checks):
    return {c.name: c for c in checks}


def test_a_healthy_environment_passes_without_a_database(healthy):
    checks = run_checks(check_db=False)
    assert not has_failures(checks)
    assert all(c.status == 'ok' for c in checks if c.name != '.env file')


@pytest.mark.parametrize('missing', config.REQUIRED_ENV_VARS)
def test_each_required_variable_is_checked(healthy, monkeypatch, missing):
    monkeypatch.delenv(missing)
    check = by_name(run_checks(check_db=False))['environment variables']
    assert check.status == 'fail' and missing in check.detail


def test_empty_variable_counts_as_missing(healthy, monkeypatch):
    monkeypatch.setenv('POSTGRES_USER', '   ')
    assert by_name(run_checks(check_db=False))['environment variables'].status == 'fail'


@pytest.mark.parametrize('port', ['abc', '0', '70000', '-1'])
def test_invalid_port_fails(healthy, monkeypatch, port):
    monkeypatch.setenv('POSTGRES_PORT', port)
    assert by_name(run_checks(check_db=False))['POSTGRES_PORT'].status == 'fail'


def test_placeholder_password_only_warns(healthy, monkeypatch):
    monkeypatch.setenv('POSTGRES_PASSWORD', 'change_me')
    checks = run_checks(check_db=False)
    assert by_name(checks)['POSTGRES_PASSWORD'].status == 'warn' and not has_failures(checks)


def test_missing_source_file_fails(healthy):
    (healthy / 'data' / 'source' / 'orders.csv').unlink()
    check = by_name(run_checks(check_db=False))['source orders.csv']
    assert check.status == 'fail' and 'missing' in check.detail


def test_empty_source_file_fails(healthy):
    (healthy / 'data' / 'source' / 'customers.csv').write_text('')
    assert 'empty' in by_name(run_checks(check_db=False))['source customers.csv'].detail


def test_source_csv_with_missing_columns_fails(healthy):
    (healthy / 'data' / 'source' / 'orders.csv').write_text('order_id,status\nO1,PAID\n')
    check = by_name(run_checks(check_db=False))['source orders.csv']
    assert check.status == 'fail' and 'missing column(s)' in check.detail and 'quantity' in check.detail


def test_malformed_products_json_fails(healthy):
    (healthy / 'data' / 'source' / 'products.json').write_text('{not json')
    assert by_name(run_checks(check_db=False))['source products.json'].status == 'fail'


def test_unwritable_output_folder_fails(healthy):
    (healthy / 'data' / 'raw').write_text('i am a file, not a folder')
    check = by_name(run_checks(check_db=False))['writable raw_dir']
    assert check.status == 'fail'


@pytest.mark.parametrize('mutate, name', [
    (lambda s: s['quality'].update(min_quantity=30), 'quality thresholds'),
    (lambda s: s['quality'].update(allowed_order_statuses=[]), 'quality thresholds'),
    (lambda s: s['pipeline'].pop('partition_dir'), 'settings.yml [pipeline]'),
    (lambda s: s['storage_benchmark'].update(filter_status='NOPE'), 'benchmark settings'),
])
def test_invalid_settings_fail(healthy, monkeypatch, mutate, name):
    import copy
    settings = copy.deepcopy(config.SETTINGS)
    mutate(settings)
    monkeypatch.setattr(config, 'SETTINGS', settings)
    assert by_name(run_checks(check_db=False))[name].status == 'fail'


def test_benchmark_repeats_below_five_warns(healthy, monkeypatch):
    import copy
    settings = copy.deepcopy(config.SETTINGS)
    settings['storage_benchmark']['repeats'] = 2
    monkeypatch.setattr(config, 'SETTINGS', settings)
    assert by_name(run_checks(check_db=False))['benchmark settings'].status == 'warn'


# --------------------------------------------------------------- database
def test_unreachable_database_fails(healthy, monkeypatch):
    monkeypatch.setitem(config.DB, 'port', 1)
    assert by_name(run_checks())['database connection'].status == 'fail'


@pytest.mark.postgres
def test_reachable_database_with_schema_passes(healthy, pg):
    checks = by_name(run_checks())
    assert checks['database connection'].status == 'ok' and checks['database tables'].status == 'ok'


@pytest.mark.postgres
def test_database_without_the_warehouse_tables_fails(healthy, pg_database, monkeypatch):
    monkeypatch.setitem(config.DB, 'dbname', 'postgres')    # exists, but has no curated/audit tables
    checks = by_name(run_checks())
    assert checks['database connection'].status == 'ok'
    assert checks['database tables'].status == 'fail' and 'curated.sales_order_lines' in checks['database tables'].detail


# -------------------------------------------------------------------- CLI
def test_cli_exit_code_follows_the_checks_and_never_prints_the_password(healthy, capsys):
    assert main(['validate-env', '--skip-db']) == 0
    out = capsys.readouterr().out
    assert 'a-real-secret' not in out and '0 failure(s)' in out


def test_cli_exits_1_and_names_the_failure(healthy, monkeypatch, capsys):
    monkeypatch.setenv('POSTGRES_PORT', 'abc')
    assert main(['validate-env', '--skip-db']) == 1
    out = capsys.readouterr().out
    assert '[FAIL] POSTGRES_PORT' in out and '1 failure(s)' in out


def test_formatting_summarises_counts():
    text = env_check.format_report([env_check.Check('a', 'ok', 'x'), env_check.Check('b', 'warn', 'y'),
                                    env_check.Check('c', 'fail', 'z')])
    assert text.splitlines()[-1] == 'validate-env: 1 ok, 1 warning(s), 1 failure(s)'


def test_a_non_numeric_port_is_reported_by_a_fresh_process_not_a_traceback():
    """config.py runs at import time, so only a brand-new process exercises this path."""
    import os, subprocess, sys
    from pathlib import Path
    repo = Path(__file__).resolve().parents[1]
    env = {**os.environ, 'POSTGRES_PORT': 'abc', 'PYTHONPATH': str(repo)}
    done = subprocess.run([sys.executable, '-m', 'src.cli', 'validate-env', '--skip-db'],
                          cwd=repo, env=env, capture_output=True, text=True, timeout=60)
    assert done.returncode == 1
    assert '[FAIL] POSTGRES_PORT' in done.stdout and 'Traceback' not in done.stderr


def test_connecting_with_a_non_numeric_port_fails_loudly_instead_of_using_the_default(monkeypatch):
    import psycopg
    from src.common.db import connect
    monkeypatch.setitem(config.DB, 'port', 'abc')
    with pytest.raises(psycopg.Error):
        connect()
