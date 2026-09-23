import argparse
from src.config import PROJECT_ROOT, DB, SETTINGS
from src.common.audit import new_run_id
from src.extract.files import extract_sources
from src.transform.staging import build_staging


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
        raw_dir = extract_sources(run_id)
        (PROJECT_ROOT / 'state').mkdir(exist_ok=True)
        (PROJECT_ROOT / 'state' / 'current_run_id.txt').write_text(run_id)
        print(f'run_id={run_id}')
        print(f'Raw snapshot created at: {raw_dir}')
        return
    
    if args.command == 'transform':
        run_id = (PROJECT_ROOT / 'state' / 'current_run_id.txt').read_text().strip()
        raw_dir = PROJECT_ROOT / 'data' / 'raw' / f'run_id={run_id}'
        staging, quarantine_df = build_staging(raw_dir, run_id)
        for name, df in staging.items():
            print(f'{name}: {len(df)} valid rows')
        print(f'quarantine: {len(quarantine_df)} rows')
        return
    
    # TODO: Wire the modular functions together. Keep orchestration logic thin.
    raise NotImplementedError(f'Wire command: {args.command}')


if __name__ == '__main__':
    main()
