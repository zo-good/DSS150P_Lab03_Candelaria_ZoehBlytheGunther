"""Partitioned Parquet: write, prune-read, and verify.

Layout written (Hive style, one directory level per column):

    <root>/order_year=2026/order_month=5/<uuid>-0.parquet

Months are NOT zero padded (``order_month=5``), because that is how pyarrow
writes integer partition values.

Partition keys are derived from ``order_timestamp`` in UTC. The same function
(`add_partition_columns`) is used for writing, for reading and for the
PostgreSQL selected-partition load, so the three can never disagree.
"""
import uuid
from pathlib import Path

import pandas as pd
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from src.common.runtime import replace_directory

PARTITION_COLUMNS = ['order_year', 'order_month']


class PartitionVerificationError(RuntimeError):
    """A partition read did not return exactly the rows it should."""


def partition_key(year: int, month: int) -> str:
    return f'{int(year):04d}-{int(month):02d}'


def _check_year_month(year: int, month: int) -> None:
    if not 1 <= int(month) <= 12:
        raise ValueError(f'month must be 1..12, got {month}')
    if not 1900 <= int(year) <= 2200:
        raise ValueError(f'year looks wrong: {year}')


def add_partition_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy with integer order_year / order_month (UTC) columns."""
    ts = pd.to_datetime(df['order_timestamp'], utc=True)
    if ts.isna().any():
        raise ValueError('order_timestamp contains nulls; cannot derive partitions')
    out = df.copy()
    out['order_year'] = ts.dt.year.astype('int32')
    out['order_month'] = ts.dt.month.astype('int32')
    return out


def select_partition(df: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    """Rows of ``df`` that belong to one year/month partition (partition columns dropped)."""
    _check_year_month(year, month)
    with_cols = add_partition_columns(df)
    mask = (with_cols['order_year'] == int(year)) & (with_cols['order_month'] == int(month))
    return df[mask.to_numpy()].reset_index(drop=True)


def write_partitioned_parquet(df, output_dir):
    """Write Parquet partitioned by order_year/order_month, safely re-runnable.

    pyarrow adds a *new uniquely named file* to a partition directory on every
    write, so writing twice into the same folder would silently duplicate every
    row on read. To avoid that the data is written to a fresh temporary
    directory and then swapped in for the old one, so a repeat write replaces
    the dataset instead of appending to it, and a crash mid-write leaves the
    previous dataset untouched.
    """
    output_dir = Path(output_dir)
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    tmp_dir = output_dir.with_name(f'.{output_dir.name}.{uuid.uuid4().hex[:8]}.tmp')
    try:
        add_partition_columns(df).to_parquet(
            tmp_dir, partition_cols=PARTITION_COLUMNS, index=False
        )
        replace_directory(tmp_dir, output_dir)
    finally:
        if tmp_dir.exists():
            import shutil
            shutil.rmtree(tmp_dir, ignore_errors=True)
    return output_dir


def _dataset(root):
    return ds.dataset(str(root), format='parquet', partitioning='hive')


def _filter(year: int, month: int):
    return (ds.field('order_year') == int(year)) & (ds.field('order_month') == int(month))


def partition_files(root, year: int, month: int) -> tuple[list[str], int]:
    """Files a read of one partition would touch, and the total file count.

    Uses pyarrow's partition pruning: fragments whose directory values do not
    match the filter are never opened.
    """
    _check_year_month(year, month)
    dataset = _dataset(root)
    selected = sorted(f.path for f in dataset.get_fragments(filter=_filter(year, month)))
    total = len(list(dataset.get_fragments()))
    return selected, total


def read_partition(root, year: int, month: int) -> pd.DataFrame:
    """Read only one year/month partition (other partitions are never opened)."""
    _check_year_month(year, month)
    return _dataset(root).to_table(filter=_filter(year, month)).to_pandas()


def partition_summary(root) -> pd.DataFrame:
    """One row per partition: year, month, files, rows (from Parquet metadata)."""
    rows = []
    for fragment in _dataset(root).get_fragments():
        expr = str(fragment.partition_expression)
        rows.append({
            'path': fragment.path,
            'rows': pq.ParquetFile(fragment.path).metadata.num_rows,
            'expression': expr,
        })
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=['order_year', 'order_month', 'files', 'rows'])
    frame['order_year'] = frame['expression'].str.extract(r'order_year == (\d+)')[0].astype(int)
    frame['order_month'] = frame['expression'].str.extract(r'order_month == (\d+)')[0].astype(int)
    return (
        frame.groupby(['order_year', 'order_month'])
        .agg(files=('path', 'count'), rows=('rows', 'sum'))
        .reset_index()
        .sort_values(['order_year', 'order_month'])
        .reset_index(drop=True)
    )


def verify_partition(root, year: int, month: int, source_df: pd.DataFrame) -> dict:
    """Read one partition and prove it is exactly the expected rows.

    Checks, in order (any failure raises PartitionVerificationError):
      1. only files under order_year=<Y>/order_month=<M>/ were selected;
      2. when more than one partition exists, fewer files were read than exist
         (pruning actually skipped something);
      3. the row count equals the count derived independently from ``source_df``;
      4. every returned row's order_timestamp really falls in that year/month;
      5. the returned order_id set equals the expected order_id set.
    """
    _check_year_month(year, month)
    expected = select_partition(source_df, year, month)
    files, total_files = partition_files(root, year, month)
    problems = []

    marker = f'order_year={int(year)}'
    month_marker = f'order_month={int(month)}'
    outside = [f for f in files if marker not in f or month_marker not in f]
    if outside:
        problems.append(f'{len(outside)} selected file(s) are outside the partition')

    partitions_total = len(partition_summary(root))
    if partitions_total > 1 and files and len(files) >= total_files:
        problems.append('pruning did not skip any files')

    actual = read_partition(root, year, month) if files else pd.DataFrame(columns=source_df.columns)

    if len(actual) != len(expected):
        problems.append(f'read {len(actual)} row(s) but expected {len(expected)}')

    if len(actual):
        ts = pd.to_datetime(actual['order_timestamp'], utc=True)
        wrong = ~((ts.dt.year == int(year)) & (ts.dt.month == int(month)))
        if wrong.any():
            problems.append(f'{int(wrong.sum())} row(s) have a timestamp outside {partition_key(year, month)}')

    if set(actual.get('order_id', [])) != set(expected['order_id']):
        problems.append('order_id set differs from the expected set')

    if problems:
        raise PartitionVerificationError(
            f'partition {partition_key(year, month)}: ' + '; '.join(problems)
        )

    return {
        'partition_key': partition_key(year, month),
        'rows_read': len(actual),
        'rows_expected': len(expected),
        'files_read': len(files),
        'files_total': total_files,
        'partitions_total': partitions_total,
        'pruned': partitions_total > 1 and len(files) < total_files,
    }
