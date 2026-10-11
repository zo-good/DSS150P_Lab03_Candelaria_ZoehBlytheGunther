"""Partition behaviour: key derivation, layout, pruned reads, verification, repeatability."""
import shutil

import pandas as pd
import pytest

from src.benchmark.partitions import (
    PartitionVerificationError, add_partition_columns, partition_files, partition_key,
    partition_summary, read_partition, select_partition, verify_partition,
    write_partitioned_parquet,
)
from src.cli import main


@pytest.fixture
def dataset(make_curated, tmp_path):
    df = make_curated('r1')                     # 5 rows: May x2, June x2, July x1
    root = write_partitioned_parquet(df, tmp_path / 'parts')
    return df, root


# ------------------------------------------------------------------ keys
def test_partition_key_is_zero_padded():
    assert partition_key(2026, 5) == '2026-05' and partition_key(2026, 12) == '2026-12'


def test_partition_columns_use_utc_not_the_local_offset():
    df = pd.DataFrame({'order_timestamp': pd.to_datetime(
        ['2026-01-31T23:30:00-05:00',      # = 2026-02-01 04:30 UTC  -> February
         '2026-06-30T23:59:59+00:00',      # last second of June
         '2026-07-01T00:00:00+00:00'],     # first second of July
        utc=True)})
    out = add_partition_columns(df)
    assert list(zip(out['order_year'], out['order_month'])) == [(2026, 2), (2026, 6), (2026, 7)]


def test_null_timestamps_are_rejected():
    with pytest.raises(ValueError, match='nulls'):
        add_partition_columns(pd.DataFrame({'order_timestamp': [pd.NaT]}))


@pytest.mark.parametrize('year, month', [(2026, 0), (2026, 13), (1800, 5)])
def test_invalid_year_or_month_is_rejected(dataset, year, month):
    _, root = dataset
    with pytest.raises(ValueError):
        read_partition(root, year, month)


# ---------------------------------------------------------------- layout
def test_layout_is_hive_style_year_then_month(dataset):
    _, root = dataset
    assert (root / 'order_year=2026' / 'order_month=5').is_dir()
    assert (root / 'order_year=2026' / 'order_month=6').is_dir()
    assert (root / 'order_year=2026' / 'order_month=7').is_dir()


def test_summary_accounts_for_every_row(dataset):
    df, root = dataset
    summary = partition_summary(root)
    assert len(summary) == 3 and int(summary['rows'].sum()) == len(df)
    assert dict(zip(summary['order_month'], summary['rows'])) == {5: 2, 6: 2, 7: 1}


# ------------------------------------------------------------ pruned reads
def test_reading_one_partition_returns_only_its_rows_and_skips_other_files(dataset):
    df, root = dataset
    files, total = partition_files(root, 2026, 6)
    assert total == 3 and len(files) == 1
    assert 'order_year=2026' in files[0] and 'order_month=6' in files[0]
    part = read_partition(root, 2026, 6)
    assert sorted(part['order_id']) == ['O3', 'O4']
    ts = pd.to_datetime(part['order_timestamp'], utc=True)
    assert (ts.dt.month == 6).all() and (ts.dt.year == 2026).all()


def test_month_boundary_rows_land_in_the_right_partition(dataset):
    _, root = dataset
    assert 'O4' in set(read_partition(root, 2026, 6)['order_id'])      # 23:59:59Z -> June
    assert set(read_partition(root, 2026, 7)['order_id']) == {'O5'}   # 00:00:00Z -> July


def test_an_empty_partition_reads_zero_rows(dataset):
    df, root = dataset
    assert len(read_partition(root, 2030, 1)) == 0
    assert verify_partition(root, 2030, 1, df)['rows_read'] == 0


def test_select_partition_matches_the_written_partition(dataset):
    df, root = dataset
    assert sorted(select_partition(df, 2026, 5)['order_id']) == sorted(read_partition(root, 2026, 5)['order_id'])


# ----------------------------------------------------------- verification
def test_verification_passes_and_reports_pruning(dataset):
    df, root = dataset
    result = verify_partition(root, 2026, 5, df)
    assert result == {'partition_key': '2026-05', 'rows_read': 2, 'rows_expected': 2,
                      'files_read': 1, 'files_total': 3, 'partitions_total': 3, 'pruned': True}


def test_verification_catches_a_foreign_file_planted_in_a_partition(dataset):
    df, root = dataset
    june_file = next((root / 'order_year=2026' / 'order_month=6').glob('*.parquet'))
    shutil.copy(june_file, root / 'order_year=2026' / 'order_month=5' / 'planted.parquet')
    with pytest.raises(PartitionVerificationError) as info:
        verify_partition(root, 2026, 5, df)
    assert 'read 4 row(s) but expected 2' in str(info.value)
    assert 'outside' in str(info.value)       # the planted rows' timestamps are June


def test_verification_catches_a_missing_partition_file(dataset):
    df, root = dataset
    for f in (root / 'order_year=2026' / 'order_month=5').glob('*.parquet'):
        f.unlink()
    with pytest.raises(PartitionVerificationError, match='read 0 row'):
        verify_partition(root, 2026, 5, df)


# ------------------------------------------------------------ repeatability
def test_writing_twice_replaces_the_dataset_instead_of_duplicating_rows(make_curated, tmp_path):
    df = make_curated('r1')
    out = tmp_path / 'out'
    root = out / 'parts'
    write_partitioned_parquet(df, root)
    write_partitioned_parquet(df, root)
    assert len(pd.read_parquet(root)) == len(df)                       # not 2x
    assert len(partition_files(root, 2026, 5)[0]) == 1
    assert sorted(p.name for p in out.iterdir()) == ['parts']          # no temp/old folders left


def test_plain_pandas_partitioned_writes_append_which_is_why_we_replace_the_dataset(tmp_path):
    """Documents the library behaviour the replace-on-write logic protects against:
    writing twice into one folder with to_parquet(partition_cols=...) doubles the rows."""
    frame = pd.DataFrame({'order_id': ['a', 'b', 'c'], 'order_year': [2026, 2026, 2026], 'order_month': [5, 5, 6]})
    for _ in range(2):
        frame.to_parquet(tmp_path / 'naive', partition_cols=['order_year', 'order_month'], index=False)
    assert len(pd.read_parquet(tmp_path / 'naive')) == 6      # 3 rows written, 6 read back


def test_a_crash_at_the_final_swap_leaves_the_previous_dataset_intact(make_curated, tmp_path, monkeypatch):
    df = make_curated('r1')
    out = tmp_path / 'out'
    root = out / 'parts'
    write_partitioned_parquet(df, root)
    smaller = select_partition(df, 2026, 5)

    def crash(new_dir, final_dir):
        raise OSError('power cut')

    monkeypatch.setattr('src.benchmark.partitions.replace_directory', crash)
    with pytest.raises(OSError):
        write_partitioned_parquet(smaller, root)
    monkeypatch.undo()
    assert len(pd.read_parquet(root)) == len(df)              # old, complete dataset still there
    assert sorted(p.name for p in out.iterdir()) == ['parts']  # temp folder cleaned up


def test_a_failure_while_writing_partitions_leaves_the_previous_dataset_intact(make_curated, tmp_path, monkeypatch):
    df = make_curated('r1')
    out = tmp_path / 'out'
    root = out / 'parts'
    write_partitioned_parquet(df, root)
    real = pd.DataFrame.to_parquet

    def half_write_then_fail(self, path, *a, **k):
        real(self, path, *a, **k)
        raise OSError('disk full')

    monkeypatch.setattr(pd.DataFrame, 'to_parquet', half_write_then_fail)
    with pytest.raises(OSError):
        write_partitioned_parquet(select_partition(df, 2026, 5), root)
    monkeypatch.undo()
    assert len(pd.read_parquet(root)) == len(df)
    assert sorted(p.name for p in out.iterdir()) == ['parts']


def test_stale_partitions_from_older_data_do_not_survive_a_rewrite(make_curated, tmp_path):
    df = make_curated('r1')
    root = tmp_path / 'parts'
    write_partitioned_parquet(df, root)
    write_partitioned_parquet(select_partition(df, 2026, 5), root)    # smaller dataset, same folder
    assert list(partition_summary(root)['order_month']) == [5]


# --------------------------------------------------------------- CLI flow
def test_cli_partition_write_then_read_verifies_one_month(project, make_curated, capsys):
    make_curated('r1')
    assert main(['partition-write', '--run-id', 'r1']) == 0
    assert '3 partitions, 5 rows' in capsys.readouterr().out
    assert main(['partition-read', '--run-id', 'r1', '--year', '2026', '--month', '5']) == 0
    out = capsys.readouterr().out
    assert 'read 2 rows (expected 2) from 1 of 3 files across 3 partitions; pruned=True' in out
    assert 'Verification PASSED.' in out
