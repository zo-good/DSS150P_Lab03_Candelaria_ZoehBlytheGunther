"""Benchmark: complete (all formats + partitions), repeatable, auditable, row-count checked."""
import json
import statistics
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from factories import cust, order, prod
from src import config
from src.benchmark.partitions import select_partition
from src.benchmark.storage import BenchmarkError, run_benchmark
from src.cli import main
from src.common.db import connect
from src.common.runtime import benchmark_dir_for, curated_path_for

STATUSES = ['PENDING', 'PAID', 'PACKED', 'SHIPPED', 'DELIVERED', 'CANCELLED']


@pytest.fixture
def curated_path(make_curated):
    start = datetime(2026, 1, 3, tzinfo=timezone.utc)
    orders = [order(order_id=f'O{i:04d}', status=STATUSES[i % 6],
                    order_timestamp=(start + timedelta(days=i % 120)).isoformat(),
                    updated_at='2026-06-01T00:00:00+00:00') for i in range(120)]
    make_curated('bench', customers=[cust()], products=[prod()], orders=orders)
    return curated_path_for('bench')


def bench(curated_path, out, **kw):
    return run_benchmark(curated_path, out, 5, postgres=False, **kw)


def test_all_formats_and_partition_scenarios_are_present(curated_path, tmp_path):
    results = bench(curated_path, tmp_path / 'b')
    assert list(results['storage_type']) == [
        'CSV', 'JSON Lines', 'Parquet',
        'Parquet single file, one-month read', 'Parquet partitioned, one-month read']
    assert list(results.columns[:7]) == ['storage_type', 'file_size_bytes', 'write_seconds',
                                         'full_read_seconds', 'filtered_read_seconds', 'row_count', 'notes']


def test_every_format_read_back_the_same_number_of_rows(curated_path, tmp_path):
    results = bench(curated_path, tmp_path / 'b')
    expected_delivered = 120 // 6
    assert set(results['row_count']) == {120}
    full = results[results['scenario'] == 'full_table']
    assert set(full['filtered_row_count']) == {expected_delivered}
    month = results[results['scenario'].str.startswith('partition_')]
    assert len(set(month['filtered_row_count'])) == 1                    # both paths agree
    key = month['scenario'].iloc[0].removeprefix('partition_')
    year, mon = map(int, key.split('-'))
    assert month['filtered_row_count'].iloc[0] == len(select_partition(pd.read_parquet(curated_path), year, mon))


def test_medians_in_the_csv_are_the_medians_of_the_saved_raw_timings(curated_path, tmp_path):
    out = tmp_path / 'b'
    results = bench(curated_path, out).set_index('storage_type')
    payload = json.loads((out / 'benchmark_timings.json').read_text())
    assert set(results['repeats']) == {5} and payload['run_settings']['repeats'] == 5
    for name, metrics in payload['timings_seconds'].items():
        for metric, column in (('write', 'write_seconds'), ('full_read', 'full_read_seconds'),
                               ('filtered_read', 'filtered_read_seconds')):
            if metric in metrics:
                assert len(metrics[metric]) == 5 and all(t > 0 for t in metrics[metric])
                assert results.loc[name, column] == pytest.approx(round(statistics.median(metrics[metric]), 4))


def test_context_records_hardware_os_and_library_versions(curated_path, tmp_path):
    out = tmp_path / 'b'
    bench(curated_path, out)
    context = json.loads((out / 'benchmark_context.json').read_text())
    for key in ('os', 'machine', 'cpu_model', 'cpu_count', 'ram_bytes', 'python', 'pandas', 'pyarrow',
                'psycopg', 'measured_at_utc'):
        assert key in context
    assert context['cpu_count'] >= 1 and context['pandas'] == pd.__version__


def test_rerunning_is_repeatable_and_does_not_accumulate_files(curated_path, tmp_path):
    out = tmp_path / 'b'
    first = bench(curated_path, out)
    names_after_first = sorted(p.relative_to(out).as_posix() for p in out.rglob('*') if p.is_file())
    second = bench(curated_path, out)
    names_after_second = sorted(p.relative_to(out).as_posix() for p in out.rglob('*') if p.is_file())
    stable = ['storage_type', 'row_count', 'filtered_row_count', 'scenario', 'repeats']
    pd.testing.assert_frame_equal(first[stable], second[stable])
    sizes = lambda r: list(r.set_index('storage_type').loc[['CSV', 'JSON Lines', 'Parquet'], 'file_size_bytes'])
    assert sizes(first) == sizes(second)                                  # deterministic outputs
    # partition files carry random names, but the number of files must not grow
    strip = lambda names: sorted(n.rsplit('/', 1)[0] if 'partitioned/' in n else n for n in names)
    assert strip(names_after_first) == strip(names_after_second)
    assert len(pd.read_parquet(out / 'partitioned')) == 120               # not duplicated


def test_fewer_than_five_repeats_is_refused_unless_explicitly_allowed(curated_path, tmp_path):
    with pytest.raises(ValueError, match='at least 5'):
        run_benchmark(curated_path, tmp_path / 'b', 3, postgres=False)
    results = run_benchmark(curated_path, tmp_path / 'c', 2, postgres=False, min_repeats=1)
    assert set(results['repeats']) == {2}


def test_a_format_that_loses_rows_is_detected(curated_path, tmp_path, monkeypatch):
    real = pd.read_csv
    monkeypatch.setattr(pd, 'read_csv', lambda *a, **k: real(*a, **k).iloc[1:])
    with pytest.raises(BenchmarkError, match='CSV'):
        bench(curated_path, tmp_path / 'b')


def test_result_files_can_be_copied_to_an_evidence_folder(curated_path, tmp_path):
    bench(curated_path, tmp_path / 'b', evidence_dir=tmp_path / 'evidence')
    assert sorted(p.name for p in (tmp_path / 'evidence').iterdir()) == [
        'benchmark_context.json', 'benchmark_results.csv', 'benchmark_timings.json']
    assert (tmp_path / 'evidence' / 'benchmark_results.csv').read_text() == (tmp_path / 'b' / 'benchmark_results.csv').read_text()


def test_the_partition_check_inside_the_benchmark_is_recorded(curated_path, tmp_path):
    out = tmp_path / 'b'
    bench(curated_path, out)
    check = json.loads((out / 'benchmark_timings.json').read_text())['partition_verification']
    assert check['pruned'] is True and check['files_read'] == 1 and check['files_total'] > 1
    assert check['rows_read'] == check['rows_expected']


@pytest.mark.postgres
def test_postgres_is_benchmarked_on_a_temp_copy_and_the_real_table_is_untouched(curated_path, tmp_path, pg):
    out = tmp_path / 'b'
    results = run_benchmark(curated_path, out, 5)
    row = results[results['storage_type'] == 'PostgreSQL'].iloc[0]
    assert row['row_count'] == 120 and row['filtered_row_count'] == 20 and row['file_size_bytes'] > 0
    with connect() as conn, conn.cursor() as cur:
        cur.execute('SELECT count(*) FROM curated.sales_order_lines')
        assert cur.fetchone() == (0,)
    assert 'postgres_server_version' in json.loads((out / 'benchmark_context.json').read_text())


def test_cli_benchmark_writes_to_a_folder_named_after_the_run(project, curated_path, capsys):
    assert main(['benchmark', '--run-id', 'bench', '--repeats', '5', '--skip-postgres']) == 0
    assert (benchmark_dir_for('bench') / 'benchmark_results.csv').is_file()
    assert 'Results saved in' in capsys.readouterr().out
    assert not benchmark_dir_for('other').exists()
