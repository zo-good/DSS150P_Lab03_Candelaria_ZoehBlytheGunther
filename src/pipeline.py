"""Stage functions shared by the CLI, the Airflow DAG (through the CLI) and tests.

Each function takes an explicit ``run_id`` and touches only that run's own
paths. None of them reads or writes shared mutable state, which is what makes
overlapping and repeated runs safe. The CLI adds argument parsing, printing
and error reporting on top.
"""
import pandas as pd

from src.benchmark.partitions import (
    add_partition_columns,
    partition_summary,
    select_partition,
    verify_partition,
    write_partitioned_parquet,
)
from src.benchmark.storage import run_benchmark
from src.common.audit import sanitize_run_id
from src.common.errors import PipelineStageError
from src.common.runtime import (
    benchmark_dir_for,
    curated_path_for,
    partition_dir_for,
    raw_dir_for,
    read_manifest,
    record_stage,
    set_latest_run,
)
from src.extract.files import extract_sources
from src.load.postgres import (
    count_partition_rows,
    load_partition,
    upsert_curated,
    verify_loaded,
)
from src.transform.curated import build_curated
from src.transform.staging import build_staging, count_raw_rows
from src.validate.quality import validate_curated


def run_stage(stage_name: str, func, *args, **kwargs):
    """Run a pipeline stage, wrapping any failure with stage context."""
    try:
        return func(*args, **kwargs)
    except Exception as e:
        raise PipelineStageError(stage_name, e) from e


def tracked(run_id: str, stage: str, func, *args, **kwargs):
    """run_stage + record a failure in the run's manifest before re-raising."""
    try:
        return run_stage(stage, func, *args, **kwargs)
    except PipelineStageError as err:
        try:
            record_stage(run_id, stage, 'failed', error=str(err))
        except Exception:  # never let bookkeeping hide the real error
            pass
        raise


def do_extract(run_id: str) -> dict:
    run_id = sanitize_run_id(run_id)
    raw_dir = tracked(run_id, 'extract', extract_sources, run_id)
    raw_rows = run_stage('extract', count_raw_rows, raw_dir)
    record_stage(run_id, 'extract', 'success', raw_dir=str(raw_dir), raw_rows=raw_rows)
    set_latest_run(run_id)
    return {'run_id': run_id, 'raw_dir': raw_dir, 'raw_rows': raw_rows}


def do_transform(run_id: str) -> dict:
    run_id = sanitize_run_id(run_id)
    raw_dir = raw_dir_for(run_id)
    if not raw_dir.is_dir():
        err = PipelineStageError(
            'transform.staging',
            FileNotFoundError(f'no raw snapshot for run {run_id} at {raw_dir}; run extract first'),
        )
        try:
            record_stage(run_id, 'transform', 'failed', error=str(err))
        except Exception:
            pass
        raise err
    staging, staging_q = tracked(run_id, 'transform', _build_staging, raw_dir, run_id)
    curated, curated_q = tracked(run_id, 'transform', _build_curated, staging, run_id)
    counts = {
        'staging_valid': {name: len(df) for name, df in staging.items()},
        'staging_quarantine': len(staging_q),
        'curated': len(curated),
        'curated_quarantine': len(curated_q),
    }
    record_stage(run_id, 'transform', 'success', **counts)
    return {'run_id': run_id, 'staging': staging, 'staging_quarantine': staging_q,
            'curated': curated, 'curated_quarantine': curated_q}


def _build_staging(raw_dir, run_id):
    return run_stage('transform.staging', build_staging, raw_dir, run_id)


def _build_curated(staging, run_id):
    return run_stage('transform.curated', build_curated, staging, run_id)


def read_curated(run_id: str) -> pd.DataFrame:
    path = curated_path_for(run_id)
    if not path.is_file():
        raise FileNotFoundError(f'no curated file for run {run_id} at {path}; run transform first')
    return pd.read_parquet(path)


def do_load(run_id: str, df: pd.DataFrame | None = None) -> dict:
    run_id = sanitize_run_id(run_id)
    if df is None:
        df = tracked(run_id, 'load', read_curated, run_id)
    affected = tracked(run_id, 'load', upsert_curated, df, run_id)
    record_stage(run_id, 'load', 'success', scope='full', rows_in_file=len(df), rows_affected=affected)
    return {'run_id': run_id, 'rows_in_file': len(df), 'affected': affected}


def do_load_partition(run_id: str, year: int, month: int) -> dict:
    run_id = sanitize_run_id(run_id)
    df = tracked(run_id, 'load-partition', read_curated, run_id)
    subset = select_partition(df, year, month)
    affected = tracked(run_id, 'load-partition', load_partition, subset, year, month, run_id)
    in_db = tracked(run_id, 'load-partition', count_partition_rows, year, month)
    key = f'{int(year):04d}-{int(month):02d}'
    record_stage(run_id, 'load', 'success', scope=key, rows_in_file=len(subset),
                 rows_affected=affected, rows_in_database_for_partition=in_db)
    return {'run_id': run_id, 'partition_key': key, 'rows': len(subset),
            'affected': affected, 'rows_in_database': in_db}


def do_validate(run_id: str, check_db: bool = True, df: pd.DataFrame | None = None) -> dict:
    """Validate curated data; optionally confirm the loaded rows are in PostgreSQL.

    The database check is scoped to what ``load`` actually loaded: the whole
    file after a full load, or one month after a partition load (read from the
    run's manifest). If no load has been recorded the check fails, because
    "validated" must not be reported for data that never reached PostgreSQL.
    """
    run_id = sanitize_run_id(run_id)
    if df is None:
        df = tracked(run_id, 'validate', read_curated, run_id)
    errors = list(tracked(run_id, 'validate', validate_curated, df))
    scope = None
    db_rows_checked = 0
    if check_db:
        manifest = read_manifest(run_id) or {}
        load_info = manifest.get('stages', {}).get('load', {})
        if load_info.get('status') != 'success':
            errors.append('no successful load is recorded for this run; nothing to verify in PostgreSQL')
        else:
            scope = load_info.get('scope', 'full')
            to_check = df
            if scope != 'full':
                year, month = (int(x) for x in scope.split('-'))
                to_check = select_partition(df, year, month)
            db_rows_checked = len(to_check)
            errors.extend(tracked(run_id, 'validate', verify_loaded, to_check))
    status = 'failed' if errors else 'success'
    record_stage(run_id, 'validate', status, rows_checked=len(df), errors=errors,
                 database_checked=check_db, database_scope=scope, database_rows_checked=db_rows_checked)
    return {'run_id': run_id, 'rows': len(df), 'errors': errors, 'database_scope': scope,
            'database_rows_checked': db_rows_checked}


def do_partition_write(run_id: str) -> dict:
    run_id = sanitize_run_id(run_id)
    df = tracked(run_id, 'partition-write', read_curated, run_id)
    root = tracked(run_id, 'partition-write', write_partitioned_parquet, df, partition_dir_for(run_id))
    summary = partition_summary(root)
    record_stage(run_id, 'partition-write', 'success', root=str(root),
                 partitions=len(summary), rows=int(summary['rows'].sum()))
    return {'run_id': run_id, 'root': root, 'summary': summary}


def do_partition_read(run_id: str, year: int, month: int) -> dict:
    run_id = sanitize_run_id(run_id)
    df = tracked(run_id, 'partition-read', read_curated, run_id)
    root = partition_dir_for(run_id)
    if not root.is_dir():
        raise PipelineStageError(
            'partition-read',
            FileNotFoundError(f'no partitioned dataset for run {run_id}; run partition-write first'))
    result = tracked(run_id, 'partition-read', verify_partition, root, year, month, df)
    record_stage(run_id, 'partition-read', 'success', **result)
    return {'run_id': run_id, **result}


def do_benchmark(run_id: str, repeats: int, postgres: bool = True, evidence_dir=None):
    run_id = sanitize_run_id(run_id)
    curated_path = curated_path_for(run_id)
    if not curated_path.is_file():
        raise PipelineStageError(
            'benchmark', FileNotFoundError(f'no curated file for run {run_id}; run transform first'))
    out_dir = benchmark_dir_for(run_id)
    results = tracked(run_id, 'benchmark', run_benchmark, curated_path, out_dir, repeats,
                      postgres=postgres, evidence_dir=evidence_dir)
    record_stage(run_id, 'benchmark', 'success', output_dir=str(out_dir), repeats=repeats)
    return results, out_dir
