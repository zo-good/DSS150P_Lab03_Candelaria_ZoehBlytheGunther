"""Summarise why rows were quarantined for one run.

    python scripts/quarantine_summary.py --run-id <run id>
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd  # noqa: E402

from src.common.runtime import quarantine_path_for, resolve_run_id  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id')
    run_id = resolve_run_id(parser.parse_args().run_id)
    for kind in ('staging', 'curated'):
        path = quarantine_path_for(kind, run_id)
        if not path.is_file():
            print(f'--- {kind} quarantine: no file (0 rows) ---')
            continue
        df = pd.read_parquet(path)
        print(f'--- {kind} quarantine: {len(df)} rows ---')
        if kind == 'curated':
            # Reasons embed ids ("customer_id C99999 not found ..."); group by kind of problem.
            df = df.assign(reason=df['reason'].str.replace(r'(customer_id|product_id) \S+', r'\1 <id>', regex=True))
        print(df.groupby(['source', 'reason']).size().rename('rows').reset_index().to_string(index=False))
    return 0


if __name__ == '__main__':
    sys.exit(main())
