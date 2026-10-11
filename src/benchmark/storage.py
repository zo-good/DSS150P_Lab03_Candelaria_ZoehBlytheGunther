"""Storage benchmark: CSV vs JSON Lines vs Parquet vs PostgreSQL (+ partitions).

Method (what the numbers mean)
------------------------------
* Same logical data for every format: the run's curated Parquet file.
* Every operation is run once as an un-timed warm-up, then ``repeats`` timed
  times (at least 5). The reported value is the MEDIAN; every individual
  timing is saved in ``benchmark_timings.json`` so the result can be audited.
* Reads are measured "into a pandas DataFrame" for every format, including
  PostgreSQL, so the formats pay the same final conversion cost.
* Filtered read: CSV and JSON Lines must parse everything and then filter;
  Parquet pushes the filter into the reader; PostgreSQL uses a WHERE clause.
  (There is no index on ``status``, so PostgreSQL scans the table.)
* PostgreSQL is measured on a TEMPORARY copy of the table that holds exactly
  this run's rows (same columns and primary key as the real table). The real
  curated table is never touched, so the row count always matches the files.
* After timing, every format's full-read and filtered-read row counts are
  checked against the source; any difference raises BenchmarkError.
* Results depend on the machine. ``benchmark_context.json`` records OS, CPU,
  RAM and library versions so the numbers are interpreted with their hardware.
"""
import gc
import json
import os
import platform
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow

from src.benchmark.partitions import (  # noqa: F401  (re-exported for the CLI/tests)
    add_partition_columns,
    partition_summary,
    read_partition,
    select_partition,
    verify_partition,
    write_partitioned_parquet,
)
from src.common.db import connect
from src.common.runtime import atomic_write_text
from src.config import SETTINGS

MIN_REPEATS = 5
RESULT_COLUMNS = [
    'storage_type', 'file_size_bytes', 'write_seconds', 'full_read_seconds',
    'filtered_read_seconds', 'row_count', 'notes',
    'scenario', 'filtered_row_count', 'repeats',
]


class BenchmarkError(RuntimeError):
    """The benchmark could not produce trustworthy results."""


def _time_runs(func, repeats: int, warmup: int = 1) -> list[float]:
    for _ in range(warmup):
        func()
    times = []
    for _ in range(repeats):
        gc.collect()
        start = time.perf_counter()
        func()
        times.append(time.perf_counter() - start)
    return times


def _median(times: list[float]) -> float:
    return round(statistics.median(times), 4)


def _dir_size(path: Path) -> int:
    return sum(p.stat().st_size for p in Path(path).rglob('*') if p.is_file())


def collect_context() -> dict:
    """Hardware / OS / library details that the timings must be read against."""
    cpu_model = platform.processor() or None
    try:
        with open('/proc/cpuinfo', encoding='utf-8') as f:
            for line in f:
                if line.lower().startswith('model name'):
                    cpu_model = line.split(':', 1)[1].strip()
                    break
    except OSError:
        pass
    try:
        ram = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
    except (AttributeError, ValueError, OSError):
        ram = None
    import psycopg
    return {
        'measured_at_utc': datetime.now(timezone.utc).isoformat(),
        'os': platform.platform(),
        'machine': platform.machine(),
        'cpu_model': cpu_model,
        'cpu_count': os.cpu_count(),
        'ram_bytes': ram,
        'python': sys.version.split()[0],
        'pandas': pd.__version__,
        'pyarrow': pyarrow.__version__,
        'psycopg': psycopg.__version__,
    }


def _check(name: str, label: str, actual: int, expected: int) -> None:
    if actual != expected:
        raise BenchmarkError(f'{name}: {label} returned {actual} rows, expected {expected}')


def _df_to_rows(df: pd.DataFrame, columns: list[str]) -> list[tuple]:
    frame = df[columns].astype(object)
    frame = frame.where(frame.notna(), None)
    return list(frame.itertuples(index=False, name=None))


def _benchmark_postgres(df, columns, filter_status, repeats, expected_filtered):
    """Time a temp copy of the table holding exactly this run's rows."""
    col_list = ', '.join(columns)
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute('CREATE TEMP TABLE bench_sales_order_lines '
                        '(LIKE curated.sales_order_lines INCLUDING ALL)')
        conn.commit()

        def write():
            with conn.cursor() as cur:
                cur.execute('TRUNCATE bench_sales_order_lines')
                rows = _df_to_rows(df, columns)
                with cur.copy(f'COPY bench_sales_order_lines ({col_list}) FROM STDIN') as copy:
                    for row in rows:
                        copy.write_row(row)
            conn.commit()

        def read(sql, params=None):
            with conn.cursor() as cur:
                cur.execute(sql, params)
                data = cur.fetchall()
                names = [d.name for d in cur.description]
            conn.commit()
            return pd.DataFrame(data, columns=names)

        write_t = _time_runs(write, repeats)
        with conn.cursor() as cur:
            cur.execute('ANALYZE bench_sales_order_lines')
        conn.commit()

        full_sql = f'SELECT {col_list} FROM bench_sales_order_lines'
        filt_sql = f'SELECT {col_list} FROM bench_sales_order_lines WHERE status = %s'
        full_t = _time_runs(lambda: read(full_sql), repeats)
        filt_t = _time_runs(lambda: read(filt_sql, (filter_status,)), repeats)

        _check('PostgreSQL', 'full read', len(read(full_sql)), len(df))
        _check('PostgreSQL', 'filtered read', len(read(filt_sql, (filter_status,))), expected_filtered)

        with conn.cursor() as cur:
            cur.execute("SELECT pg_total_relation_size('bench_sales_order_lines')")
            size = cur.fetchone()[0]
            cur.execute('SHOW server_version')
            server_version = cur.fetchone()[0]
    return write_t, full_t, filt_t, size, server_version


def run_benchmark(curated_path, output_dir, repeats: int = 5, *, postgres: bool = True,
                  min_repeats: int = MIN_REPEATS, evidence_dir=None):
    """Run the storage benchmark and save repeatable, auditable results.

    Files written under ``output_dir`` (which callers make run-specific):
      benchmark_results.csv   medians, one row per storage/scenario
      benchmark_timings.json  context + every individual timing + checks
      benchmark_context.json  hardware / OS / library versions
    ``evidence_dir`` (optional) receives copies of the three result files.
    """
    if repeats < min_repeats:
        raise ValueError(f'repeats must be at least {min_repeats}, got {repeats}')

    df = pd.read_parquet(curated_path)
    if df.empty:
        raise BenchmarkError('curated data is empty; nothing to benchmark')

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    filter_status = SETTINGS['storage_benchmark']['filter_status']
    expected_filtered = int((df['status'] == filter_status).sum())
    columns = list(df.columns)
    context = collect_context()

    timings: dict[str, dict[str, list[float]]] = {}
    rows = []

    def add_row(name, scenario, size, write_t, full_t, filt_t, row_count, filtered_count, notes):
        timings[name] = {
            k: [round(x, 6) for x in v]
            for k, v in (('write', write_t), ('full_read', full_t), ('filtered_read', filt_t))
            if v is not None
        }
        rows.append({
            'storage_type': name,
            'file_size_bytes': size,
            'write_seconds': _median(write_t) if write_t else '',
            'full_read_seconds': _median(full_t) if full_t else '',
            'filtered_read_seconds': _median(filt_t) if filt_t else '',
            'row_count': row_count,
            'notes': notes,
            'scenario': scenario,
            'filtered_row_count': filtered_count,
            'repeats': repeats,
        })

    # --- CSV ---
    csv_path = output_dir / 'sales_order_lines.csv'
    write_t = _time_runs(lambda: df.to_csv(csv_path, index=False), repeats)
    full_t = _time_runs(lambda: pd.read_csv(csv_path), repeats)
    filt_t = _time_runs(lambda: pd.read_csv(csv_path).pipe(lambda d: d[d['status'] == filter_status]), repeats)
    _check('CSV', 'full read', len(pd.read_csv(csv_path)), len(df))
    _check('CSV', 'filtered read',
           len(pd.read_csv(csv_path).pipe(lambda d: d[d['status'] == filter_status])), expected_filtered)
    add_row('CSV', 'full_table', csv_path.stat().st_size, write_t, full_t, filt_t, len(df), expected_filtered,
            'Plain text; every read re-parses and re-infers types; filter applied after reading everything.')

    # --- JSON Lines ---
    jsonl_path = output_dir / 'sales_order_lines.jsonl'
    write_t = _time_runs(lambda: df.to_json(jsonl_path, orient='records', lines=True, date_format='iso'), repeats)
    full_t = _time_runs(lambda: pd.read_json(jsonl_path, lines=True), repeats)
    filt_t = _time_runs(lambda: pd.read_json(jsonl_path, lines=True).pipe(lambda d: d[d['status'] == filter_status]), repeats)
    _check('JSON Lines', 'full read', len(pd.read_json(jsonl_path, lines=True)), len(df))
    _check('JSON Lines', 'filtered read',
           len(pd.read_json(jsonl_path, lines=True).pipe(lambda d: d[d['status'] == filter_status])), expected_filtered)
    add_row('JSON Lines', 'full_table', jsonl_path.stat().st_size, write_t, full_t, filt_t, len(df), expected_filtered,
            'One JSON object per line; key names repeated on every row; filter applied after reading everything.')

    # --- Parquet (single file) ---
    parquet_path = output_dir / 'sales_order_lines.parquet'
    write_t = _time_runs(lambda: df.to_parquet(parquet_path, index=False, compression='snappy'), repeats)
    filters = [('status', '==', filter_status)]
    full_t = _time_runs(lambda: pd.read_parquet(parquet_path), repeats)
    filt_t = _time_runs(lambda: pd.read_parquet(parquet_path, filters=filters), repeats)
    _check('Parquet', 'full read', len(pd.read_parquet(parquet_path)), len(df))
    _check('Parquet', 'filtered read', len(pd.read_parquet(parquet_path, filters=filters)), expected_filtered)
    add_row('Parquet', 'full_table', parquet_path.stat().st_size, write_t, full_t, filt_t, len(df), expected_filtered,
            'Columnar + snappy compression; schema stored in file; filter pushed into the reader.')

    # --- PostgreSQL ---
    if postgres:
        write_t, full_t, filt_t, size, server_version = _benchmark_postgres(
            df, columns, filter_status, repeats, expected_filtered)
        context['postgres_server_version'] = server_version
        add_row('PostgreSQL', 'full_table', size, write_t, full_t, filt_t, len(df), expected_filtered,
                'Temp copy of curated.sales_order_lines holding this run only; write = COPY; '
                'size = pg_total_relation_size (table + primary-key index); no index on status.')

    # --- Partitioning: read one month from one file vs from the partitioned dataset ---
    parts = add_partition_columns(df)
    counts = parts.groupby(['order_year', 'order_month']).size().sort_values(ascending=False, kind='stable')
    top = counts[counts == counts.max()].sort_index().index[0]
    year, month = int(top[0]), int(top[1])
    month_rows = len(select_partition(df, year, month))
    partition_root = output_dir / 'partitioned'
    part_write_t = _time_runs(lambda: write_partitioned_parquet(df, partition_root), repeats)
    check = verify_partition(partition_root, year, month, df)

    def one_file_month():
        full = pd.read_parquet(parquet_path)
        ts = pd.to_datetime(full['order_timestamp'], utc=True)
        return full[((ts.dt.year == year) & (ts.dt.month == month)).to_numpy()]

    base_t = _time_runs(one_file_month, repeats)
    part_t = _time_runs(lambda: read_partition(partition_root, year, month), repeats)
    _check('Parquet single file', 'one-month read', len(one_file_month()), month_rows)
    _check('Parquet partitioned', 'one-month read', len(read_partition(partition_root, year, month)), month_rows)
    label = f'{year:04d}-{month:02d}'
    add_row('Parquet single file, one-month read', f'partition_{label}', parquet_path.stat().st_size,
            None, None, base_t, len(df), month_rows,
            f'Baseline for partitioning: read the whole file, then keep {label}.')
    add_row('Parquet partitioned, one-month read', f'partition_{label}', _dir_size(partition_root),
            part_write_t, None, part_t, len(df), month_rows,
            f'Only the {label} partition directory is opened '
            f'({check["files_read"]} of {check["files_total"]} files); write_seconds = writing the whole partitioned dataset.')

    results = pd.DataFrame(rows, columns=RESULT_COLUMNS)

    payload = {
        'run_settings': {
            'curated_file': str(curated_path), 'repeats': repeats, 'warmup_runs': 1,
            'statistic': 'median', 'filter_status': filter_status, 'postgres_included': postgres,
            'partition_benchmarked': label,
        },
        'context': context,
        'partition_verification': check,
        'timings_seconds': timings,
    }
    results_csv = output_dir / 'benchmark_results.csv'
    atomic_write_text(results_csv, results.to_csv(index=False))
    atomic_write_text(output_dir / 'benchmark_timings.json', json.dumps(payload, indent=2, default=str))
    atomic_write_text(output_dir / 'benchmark_context.json', json.dumps(context, indent=2, default=str))

    if evidence_dir is not None:
        evidence_dir = Path(evidence_dir)
        for name in ('benchmark_results.csv', 'benchmark_timings.json', 'benchmark_context.json'):
            atomic_write_text(evidence_dir / name, (output_dir / name).read_text(encoding='utf-8'))

    return results
