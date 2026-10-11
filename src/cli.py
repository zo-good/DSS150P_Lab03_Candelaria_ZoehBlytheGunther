import argparse
import sys

from src import config
from src import pipeline
from src.benchmark.storage import MIN_REPEATS
from src.common.audit import new_run_id
from src.common.env_check import format_report, has_failures, run_checks
from src.common.errors import PipelineStageError
from src.common.runtime import RunIdError, resolve_run_id
from src.pipeline import run_stage  # noqa: F401  (kept importable as src.cli.run_stage)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='DSS150P modular pipeline')
    sub = parser.add_subparsers(dest='command', required=True)

    def with_run_id(p):
        p.add_argument(
            '--run-id',
            help='Run to operate on. Defaults to $PIPELINE_RUN_ID, then to the most recent extract '
                 '(a convenience for manual use only; pass --run-id for concurrent or scheduled runs).')
        return p

    v = sub.add_parser('validate-env', help='Check configuration, source files, output folders and the database')
    v.add_argument('--skip-db', action='store_true', help='Skip the PostgreSQL connectivity checks')

    with_run_id(sub.add_parser('extract'))
    with_run_id(sub.add_parser('transform'))
    with_run_id(sub.add_parser('load'))
    val = with_run_id(sub.add_parser('validate'))
    val.add_argument('--skip-db', action='store_true', help='Only validate the curated file; do not check PostgreSQL')

    b = with_run_id(sub.add_parser('benchmark'))
    b.add_argument('--repeats', type=int, default=config.SETTINGS['storage_benchmark']['repeats'])
    b.add_argument('--skip-postgres', action='store_true', help='Benchmark the file formats only')
    b.add_argument('--evidence-dir', help='Also copy the result files to this folder (e.g. docs/evidence/benchmark)')

    p = with_run_id(sub.add_parser('load-partition'))
    p.add_argument('--year', type=int, required=True)
    p.add_argument('--month', type=int, required=True)

    with_run_id(sub.add_parser('partition-write', help='Write the run as Parquet partitioned by order_year/order_month'))
    pr = with_run_id(sub.add_parser('partition-read', help='Read one partition and verify its rows'))
    pr.add_argument('--year', type=int, required=True)
    pr.add_argument('--month', type=int, required=True)

    with_run_id(sub.add_parser('run-all'))
    return parser


def _print_transform(result: dict) -> None:
    for name, df in result['staging'].items():
        print(f'{name}: {len(df)} valid rows')
    print(f'staging quarantine: {len(result["staging_quarantine"])} rows')
    print(f'curated: {len(result["curated"])} valid rows')
    print(f'curated quarantine: {len(result["curated_quarantine"])} rows')


def _print_validation(result: dict) -> int:
    if result['errors']:
        print('Validation FAILED:')
        for error in result['errors']:
            print(f'- {error}')
        return 1
    where = (f'; {result["database_rows_checked"]} of them confirmed present in PostgreSQL '
             f'(scope: {result["database_scope"]})') if result['database_scope'] else ''
    print(f'Validation PASSED: {result["rows"]} curated rows checked{where}.')
    return 0


def _dispatch(args) -> int:
    cmd = args.command

    if cmd == 'validate-env':
        print('PROJECT_ROOT=', config.PROJECT_ROOT)
        print('DB host/database=', config.DB['host'], config.DB['dbname'])
        print('Configured source=', config.SETTINGS['pipeline']['source_dir'])
        checks = run_checks(check_db=not args.skip_db)
        print(format_report(checks))
        return 1 if has_failures(checks) else 0

    if cmd == 'extract':
        run_id = args.run_id or new_run_id()
        result = pipeline.do_extract(run_id)
        print(f'run_id={result["run_id"]}')
        print(f'Raw snapshot created at: {result["raw_dir"]}')
        raw = result['raw_rows']
        print(f'raw rows: customers={raw["customers"]} products={raw["products"]} orders={raw["orders"]}')
        return 0

    if cmd == 'run-all':
        result = pipeline.do_extract(args.run_id or new_run_id())
        run_id = result['run_id']
        print(f'run_id={run_id}')
        print(f'Raw snapshot created at: {result["raw_dir"]}')
        raw = result['raw_rows']
        print(f'raw rows: customers={raw["customers"]} products={raw["products"]} orders={raw["orders"]}')
        transformed = pipeline.do_transform(run_id)
        _print_transform(transformed)
        loaded = pipeline.do_load(run_id, df=transformed['curated'])
        print(f'Rows affected (inserted or updated): {loaded["affected"]}')
        validation = pipeline.do_validate(run_id, check_db=True, df=transformed['curated'])
        code = _print_validation(validation)
        if code == 0:
            print('Pipeline completed successfully.')
        return code

    run_id = resolve_run_id(args.run_id)

    if cmd == 'transform':
        _print_transform(pipeline.do_transform(run_id))
        return 0

    if cmd == 'load':
        result = pipeline.do_load(run_id)
        print(f'Rows affected (inserted or updated): {result["affected"]}')
        return 0

    if cmd == 'validate':
        return _print_validation(pipeline.do_validate(run_id, check_db=not args.skip_db))

    if cmd == 'load-partition':
        r = pipeline.do_load_partition(run_id, args.year, args.month)
        print(f'Partition {r["partition_key"]}: {r["rows"]} rows, {r["affected"]} affected')
        print(f'PostgreSQL now holds {r["rows_in_database"]} rows for {r["partition_key"]}')
        return 0

    if cmd == 'partition-write':
        r = pipeline.do_partition_write(run_id)
        summary = r['summary']
        print(f'Partitioned dataset written to: {r["root"]}')
        print(f'{len(summary)} partitions, {int(summary["rows"].sum())} rows')
        print(summary.to_string(index=False))
        return 0

    if cmd == 'partition-read':
        r = pipeline.do_partition_read(run_id, args.year, args.month)
        print(f'Partition {r["partition_key"]}: read {r["rows_read"]} rows '
              f'(expected {r["rows_expected"]}) from {r["files_read"]} of {r["files_total"]} files '
              f'across {r["partitions_total"]} partitions; pruned={r["pruned"]}')
        print('Verification PASSED.')
        return 0

    if cmd == 'benchmark':
        if args.repeats < MIN_REPEATS:
            print(f'ERROR: --repeats must be at least {MIN_REPEATS} (got {args.repeats})', file=sys.stderr)
            return 2
        results, out_dir = pipeline.do_benchmark(
            run_id, args.repeats, postgres=not args.skip_postgres, evidence_dir=args.evidence_dir)
        print(results.drop(columns=['notes']).to_string(index=False))
        print(f'Results saved in: {out_dir}')
        return 0

    raise NotImplementedError(f'Wire command: {cmd}')


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return _dispatch(args)
    except PipelineStageError as err:
        print(f'ERROR {err}', file=sys.stderr)
        return 1
    except RunIdError as err:
        print(f'ERROR {err}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
