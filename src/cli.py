import argparse
import pandas as pd
from src.config import PROJECT_ROOT, DB, SETTINGS
from src.common.audit import new_run_id
from src.extract.files import extract_sources
from src.transform.staging import build_staging
from src.transform.curated import build_curated
from src.common.errors import PipelineStageError
from src.load.postgres import upsert_curated
from src.validate.quality import validate_curated


def run_stage(stage_name: str, func, *args, **kwargs):
    """Run a pipeline stage, wrapping any failure with stage context."""
    try:
        return func(*args, **kwargs)
    except Exception as e:
        raise PipelineStageError(stage_name, e) from e
    
def main():
    parser = argparse.ArgumentParser(description='DSS150P modular pipeline')
    sub = parser.add_subparsers(dest='command', required=True)
    sub.add_parser('validate-env')
    sub.add_parser('extract')
    sub.add_parser('transform')
    sub.add_parser('load')
    sub.add_parser('validate')
    b = sub.add_parser('benchmark'); b.add_argument('--repeats', type=int, default=5)
    p = sub.add_parser('load-partition'); p.add_argument('--year', type=int, required=True); p.add_argument('--month', type=int, required=True)
    sub.add_parser('run-all')
    args = parser.parse_args()

    if args.command == 'validate-env':
        print('PROJECT_ROOT=', PROJECT_ROOT)
        print('DB host/database=', DB['host'], DB['dbname'])
        print('Configured source=', SETTINGS['pipeline']['source_dir'])
        return

    if args.command == 'extract':
        run_id = new_run_id()
        raw_dir = run_stage('extract', extract_sources, run_id)
        (PROJECT_ROOT / 'state').mkdir(exist_ok=True)
        (PROJECT_ROOT / 'state' / 'current_run_id.txt').write_text(run_id)
        print(f'run_id={run_id}')
        print(f'Raw snapshot created at: {raw_dir}')
        return
    
    if args.command == 'transform':
        run_id = (PROJECT_ROOT / 'state' / 'current_run_id.txt').read_text().strip()
        raw_dir = PROJECT_ROOT / 'data' / 'raw' / f'run_id={run_id}'
        staging, staging_quarantine = run_stage('transform.staging', build_staging, raw_dir, run_id)
        curated, curated_quarantine = run_stage('transform.curated', build_curated, staging, run_id)
        for name, df in staging.items():
            print(f'{name}: {len(df)} valid rows')
        print(f'staging quarantine: {len(staging_quarantine)} rows')
        print(f'curated: {len(curated)} valid rows')
        print(f'curated quarantine: {len(curated_quarantine)} rows')
        return
    
    if args.command == 'load':
        run_id = (PROJECT_ROOT / 'state' / 'current_run_id.txt').read_text().strip()
        curated_path = PROJECT_ROOT / 'data' / 'curated' / f'sales_order_lines_run_id={run_id}.parquet'
        df = pd.read_parquet(curated_path)
        affected = run_stage('load', upsert_curated, df, run_id)
        print(f'Rows affected (inserted or updated): {affected}')
        return

    if args.command == 'validate':
        run_id = (PROJECT_ROOT / 'state' / 'current_run_id.txt').read_text().strip()
        curated_path = (
        PROJECT_ROOT
            / 'data'
            / 'curated'
            / f'sales_order_lines_run_id={run_id}.parquet'
        )

        df = pd.read_parquet(curated_path)
        errors = run_stage('validate', validate_curated, df)

        if errors:
            print('Validation FAILED:')
            for error in errors:
                print(f'- {error}')
            raise SystemExit(1)

        print(f'Validation PASSED: {len(df)} curated rows checked.')
        return

    if args.command == 'run-all':
        # 1. Extract
        run_id = new_run_id()
        raw_dir = run_stage('extract', extract_sources, run_id)

        (PROJECT_ROOT / 'state').mkdir(exist_ok=True)
        (PROJECT_ROOT / 'state' / 'current_run_id.txt').write_text(run_id)

        print(f'run_id={run_id}')
        print(f'Raw snapshot created at: {raw_dir}')

        # 2. Transform
        staging, staging_quarantine = run_stage(
            'transform.staging',
            build_staging,
            raw_dir,
            run_id
        )

        curated, curated_quarantine = run_stage(
            'transform.curated',
            build_curated,
            staging,
            run_id
        )

        for name, df in staging.items():
            print(f'{name}: {len(df)} valid rows')

        print(f'staging quarantine: {len(staging_quarantine)} rows')
        print(f'curated: {len(curated)} valid rows')
        print(f'curated quarantine: {len(curated_quarantine)} rows')

        # 3. Load
        affected = run_stage(
            'load',
            upsert_curated,
            curated,
            run_id
        )

        print(f'Rows affected (inserted or updated): {affected}')

        # 4. Validate
        errors = run_stage(
            'validate',
            validate_curated,
            curated
        )

        if errors:
            print('Validation FAILED:')
            for error in errors:
                print(f'- {error}')
            raise SystemExit(1)

        print(f'Validation PASSED: {len(curated)} curated rows checked.')
        print('Pipeline completed successfully.')
        return

    raise NotImplementedError(f'Wire command: {args.command}')


if __name__ == '__main__':
    main()
