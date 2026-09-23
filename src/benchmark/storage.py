import statistics
import time
from pathlib import Path

import pandas as pd
import psycopg

from src.config import DB, SETTINGS


def _median_time(func, repeats: int) -> float:
    times = []
    for _ in range(repeats):
        start = time.perf_counter()
        func()
        times.append(time.perf_counter() - start)
    return statistics.median(times)


def run_benchmark(curated_path, output_dir, repeats: int = 5):
    """Compare CSV, JSON Lines, Parquet, and PostgreSQL for the same logical dataset."""
    df = pd.read_parquet(curated_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    filter_status = SETTINGS['storage_benchmark']['filter_status']

    results = []

    # --- CSV ---
    csv_path = output_dir / 'sales_order_lines.csv'
    t0 = time.perf_counter()
    df.to_csv(csv_path, index=False)
    write_s = time.perf_counter() - t0
    full_s = _median_time(lambda: pd.read_csv(csv_path), repeats)
    filt_s = _median_time(lambda: pd.read_csv(csv_path).pipe(lambda d: d[d['status'] == filter_status]), repeats)
    results.append({
        'storage_type': 'CSV', 'file_size_bytes': csv_path.stat().st_size,
        'write_seconds': round(write_s, 4), 'full_read_seconds': round(full_s, 4),
        'filtered_read_seconds': round(filt_s, 4), 'row_count': len(df),
        'notes': 'Plain text, no schema; every read re-parses/re-infers types.',
    })

    # --- JSON Lines ---
    jsonl_path = output_dir / 'sales_order_lines.jsonl'
    t0 = time.perf_counter()
    df.to_json(jsonl_path, orient='records', lines=True, date_format='iso')
    write_s = time.perf_counter() - t0
    full_s = _median_time(lambda: pd.read_json(jsonl_path, lines=True), repeats)
    filt_s = _median_time(lambda: pd.read_json(jsonl_path, lines=True).pipe(lambda d: d[d['status'] == filter_status]), repeats)
    results.append({
        'storage_type': 'JSON Lines', 'file_size_bytes': jsonl_path.stat().st_size,
        'write_seconds': round(write_s, 4), 'full_read_seconds': round(full_s, 4),
        'filtered_read_seconds': round(filt_s, 4), 'row_count': len(df),
        'notes': 'One JSON object per line; verbose (repeated key names every row).',
    })

    # --- Parquet ---
    parquet_path = output_dir / 'sales_order_lines.parquet'
    t0 = time.perf_counter()
    df.to_parquet(parquet_path, index=False, compression='snappy')
    write_s = time.perf_counter() - t0
    full_s = _median_time(lambda: pd.read_parquet(parquet_path), repeats)
    filt_s = _median_time(lambda: pd.read_parquet(parquet_path).pipe(lambda d: d[d['status'] == filter_status]), repeats)
    results.append({
        'storage_type': 'Parquet', 'file_size_bytes': parquet_path.stat().st_size,
        'write_seconds': round(write_s, 4), 'full_read_seconds': round(full_s, 4),
        'filtered_read_seconds': round(filt_s, 4), 'row_count': len(df),
        'notes': 'Columnar + compressed; schema stored in file, no type guessing.',
    })

    # --- PostgreSQL ---
    with psycopg.connect(host=DB['host'], port=DB['port'], dbname=DB['dbname'],
                          user=DB['user'], password=DB['password']) as conn:
        def full_query():
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM curated.sales_order_lines")
                cur.fetchall()

        def filtered_query():
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM curated.sales_order_lines WHERE status = %s", (filter_status,))
                cur.fetchall()

        full_s = _median_time(full_query, repeats)
        filt_s = _median_time(filtered_query, repeats)

        with conn.cursor() as cur:
            cur.execute("SELECT pg_total_relation_size('curated.sales_order_lines')")
            table_size = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM curated.sales_order_lines")
            row_count = cur.fetchone()[0]

    results.append({
        'storage_type': 'PostgreSQL', 'file_size_bytes': table_size,
        'write_seconds': '', 'full_read_seconds': round(full_s, 4),
        'filtered_read_seconds': round(filt_s, 4), 'row_count': row_count,
        'notes': 'Server table size via pg_total_relation_size (includes indexes); already loaded, not re-written here.',
    })

    results_df = pd.DataFrame(results)
    results_df.to_csv(output_dir / 'benchmark_results.csv', index=False)
    return results_df


def write_partitioned_parquet(df, output_dir):
    """Write Parquet partitioned by order_year/order_month."""
    output_dir = Path(output_dir)
    df = df.copy()
    df['order_year'] = pd.to_datetime(df['order_timestamp']).dt.year
    df['order_month'] = pd.to_datetime(df['order_timestamp']).dt.month
    df.to_parquet(output_dir, partition_cols=['order_year', 'order_month'], index=False)
    return output_dir