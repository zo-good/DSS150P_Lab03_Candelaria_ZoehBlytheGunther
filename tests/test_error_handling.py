"""Error handling: stage wrapping, exit codes, quarantine vs exception, failure bookkeeping."""
import pandas as pd
import pytest

from factories import cust, order, prod, write_source
from src import config, pipeline
from src.cli import main
from src.common.errors import PipelineStageError
from src.common.runtime import atomic_write_parquet, curated_path_for, read_manifest
from src.pipeline import run_stage


# ------------------------------------------------------------- run_stage
def test_run_stage_returns_the_result_and_passes_arguments():
    assert run_stage('demo', lambda a, b=0: a + b, 2, b=3) == 5


def test_run_stage_wraps_errors_with_stage_name_and_keeps_the_cause():
    original = ValueError('boom')

    def fail():
        raise original

    with pytest.raises(PipelineStageError) as info:
        run_stage('transform.staging', fail)
    err = info.value
    assert err.stage == 'transform.staging'
    assert err.original is original and err.__cause__ is original  # exception chaining kept
    assert str(err) == '[transform.staging] ValueError: boom'


# ------------------------------------------------ rejections vs exceptions
def test_bad_data_is_quarantined_while_missing_files_raise(project):
    write_source(project, [cust()], [prod()], [order(order_id='OK'), order(order_id='BAD', quantity=0)])
    pipeline.do_extract('r1')
    result = pipeline.do_transform('r1')          # data defect: no exception
    assert len(result['staging_quarantine']) == 1

    (project / 'data' / 'source' / 'orders.csv').unlink()
    with pytest.raises(PipelineStageError) as info:  # system problem: exception
        pipeline.do_extract('r2')
    assert info.value.stage == 'extract' and isinstance(info.value.original, FileNotFoundError)


# ---------------------------------------------------------- CLI behaviour
def test_cli_reports_missing_source_with_stage_and_exit_code_1(project, capsys):
    (project / 'data' / 'source' / 'customers.csv').write_text('x')  # others missing
    assert main(['extract', '--run-id', 'r1']) == 1
    err = capsys.readouterr().err
    assert 'ERROR [extract]' in err and 'Missing source file(s)' in err
    assert read_manifest('r1')['stages']['extract']['status'] == 'failed'


def test_cli_transform_without_a_raw_snapshot_fails_clearly(project, capsys):
    assert main(['transform', '--run-id', 'never_extracted']) == 1
    err = capsys.readouterr().err
    assert 'ERROR [transform.staging]' in err and 'run extract first' in err
    assert read_manifest('never_extracted')['stages']['transform']['status'] == 'failed'


def test_cli_without_any_run_id_explains_what_to_do(project, capsys):
    assert main(['transform']) == 1
    assert 'No run id given' in capsys.readouterr().err


def test_cli_load_with_unreachable_database_fails_in_the_load_stage(project, capsys, monkeypatch, make_curated):
    make_curated('r1')
    monkeypatch.setitem(config.DB, 'port', 1)   # nothing listens here
    assert main(['load', '--run-id', 'r1']) == 1
    err = capsys.readouterr().err
    assert err.startswith('ERROR [load]') and 'OperationalError' in err
    assert read_manifest('r1')['stages']['load']['status'] == 'failed'


def test_cli_validate_fails_with_exit_code_1_on_bad_curated_data(project, capsys, make_curated):
    curated = make_curated('r1')
    broken = pd.concat([curated, curated.iloc[[0]]], ignore_index=True)   # duplicate order_id
    atomic_write_parquet(broken, curated_path_for('r1'))
    assert main(['validate', '--run-id', 'r1', '--skip-db']) == 1
    out = capsys.readouterr().out
    assert 'Validation FAILED' in out and 'duplicate order_id' in out
    assert read_manifest('r1')['stages']['validate']['status'] == 'failed'


def test_cli_validate_passes_on_clean_data(project, capsys, make_curated):
    make_curated('r1')
    assert main(['validate', '--run-id', 'r1', '--skip-db']) == 0
    assert 'Validation PASSED: 5 curated rows checked' in capsys.readouterr().out


def test_cli_validate_output_states_how_many_rows_were_confirmed_in_the_database(project, capsys, make_curated, pg):
    make_curated('r1')
    assert main(['load-partition', '--run-id', 'r1', '--year', '2026', '--month', '5']) == 0
    capsys.readouterr()
    assert main(['validate', '--run-id', 'r1']) == 0
    assert ('Validation PASSED: 5 curated rows checked; 2 of them confirmed present in PostgreSQL '
            '(scope: 2026-05).') in capsys.readouterr().out


def test_cli_rejects_fewer_than_five_benchmark_repeats(project, capsys, make_curated):
    make_curated('r1')
    assert main(['benchmark', '--run-id', 'r1', '--repeats', '2', '--skip-postgres']) == 2
    assert 'at least 5' in capsys.readouterr().err


def test_cli_partition_read_before_write_fails_clearly(project, capsys, make_curated):
    make_curated('r1')
    assert main(['partition-read', '--run-id', 'r1', '--year', '2026', '--month', '5']) == 1
    assert 'run partition-write first' in capsys.readouterr().err
