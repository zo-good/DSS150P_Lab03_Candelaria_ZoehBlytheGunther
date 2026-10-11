"""Fill the numeric tables in docs/evidence/report.md from the SAVED results.

    python -m src.report_tables            # rewrite the generated blocks in report.md
    python -m src.report_tables --check    # exit 1 if report.md is out of date

Every number inside a generated block comes from a saved file:
  * docs/evidence/logs/02_run_all.txt        row counts of the full run
  * docs/evidence/logs/06_quarantine_reasons.txt
  * docs/evidence/logs/09_partition_write.txt, 10_partition_read.txt
  * docs/evidence/logs/11_partition_load.txt, 12_partition_db_verification.txt, 13_partition_audit.txt
  * docs/evidence/benchmark/benchmark_results.csv and benchmark_context.json
so the report cannot drift from what was actually executed. The text between
``<!-- BEGIN:name -->`` and ``<!-- END:name -->`` is machine-written; edit the
prose around it, never inside it.
"""
import argparse
import json
import re
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / 'docs' / 'evidence'
REPORT = EVIDENCE / 'report.md'
ENV_DOC = EVIDENCE / 'environment_verification.md'
LOGS = EVIDENCE / 'logs'
BENCH = EVIDENCE / 'benchmark'


def _n(value) -> str:
    return f'{int(value):,}'


def _grab(pattern: str, text: str, name: str):
    match = re.search(pattern, text, re.MULTILINE)
    if not match:
        raise ValueError(f'could not find {name!r} in the saved log (pattern: {pattern})')
    return match


def parse_run_all(text: str) -> dict:
    raw = _grab(r'raw rows: customers=(\d+) products=(\d+) orders=(\d+)', text, 'raw rows')
    out = {
        'raw': dict(zip(('customers', 'products', 'orders'), map(int, raw.groups()))),
        'staging_valid': {},
        'run_id': _grab(r'^run_id=(\S+)', text, 'run_id').group(1),
        'staging_quarantine': int(_grab(r'^staging quarantine: (\d+) rows', text, 'staging quarantine').group(1)),
        'curated': int(_grab(r'^curated: (\d+) valid rows', text, 'curated').group(1)),
        'curated_quarantine': int(_grab(r'^curated quarantine: (\d+) rows', text, 'curated quarantine').group(1)),
        'first_load_affected': int(_grab(r'^Rows affected \(inserted or updated\): (\d+)', text, 'rows affected').group(1)),
    }
    for name in ('customers', 'products', 'orders'):
        out['staging_valid'][name] = int(_grab(rf'^{name}: (\d+) valid rows', text, name).group(1))
    return out


def render_counts(log_text: str) -> str:
    r = parse_run_all(log_text)
    lines = [
        f'Run `{r["run_id"]}` (from `docs/evidence/logs/02_run_all.txt`):',
        '',
        '| Stage | customers | products | orders |',
        '| :--- | ---: | ---: | ---: |',
        '| Raw rows read from the source files | ' + ' | '.join(_n(r['raw'][k]) for k in ('customers', 'products', 'orders')) + ' |',
        '| Valid rows in staging (after removing superseded duplicates and quarantined rows) | '
        + ' | '.join(_n(r['staging_valid'][k]) for k in ('customers', 'products', 'orders')) + ' |',
        '',
        '| Result | Rows |',
        '| :--- | ---: |',
        f'| Staging quarantine (rejected by cleaning rules) | {_n(r["staging_quarantine"])} |',
        f'| Curated (valid rows after joining orders to customers and products) | {_n(r["curated"])} |',
        f'| Curated quarantine (orders whose customer or product does not exist) | {_n(r["curated_quarantine"])} |',
        f'| First load: rows inserted or updated in PostgreSQL | {_n(r["first_load_affected"])} |',
    ]
    return '\n'.join(lines)


def render_partitions(write_log: str, read_log: str) -> str:
    total = _grab(r'^(\d+) partitions, (\d+) rows', write_log, 'partition totals')
    read = _grab(
        r'^Partition (\S+): read (\d+) rows \(expected (\d+)\) from (\d+) of (\d+) files '
        r'across (\d+) partitions; pruned=(\w+)', read_log, 'partition read')
    key, rows_read, expected, files_read, files_total, partitions, pruned = read.groups()
    return '\n'.join([
        '| Item | Value |',
        '| :--- | ---: |',
        f'| Partitions written (order_year / order_month) | {_n(total.group(1))} |',
        f'| Rows across all partitions | {_n(total.group(2))} |',
        f'| Partition read and verified | {key} |',
        f'| Rows read / rows expected | {_n(rows_read)} / {_n(expected)} |',
        f'| Files opened / files in dataset | {files_read} / {files_total} |',
        f'| Pruning verified (other partitions never opened) | {pruned} |',
    ])


def render_quarantine(text: str) -> str:
    """Table of quarantine reasons from the saved quarantine_summary log."""
    lines = ['| Quarantine file | Source | Reason | Rows |', '| :--- | :--- | :--- | ---: |']
    kind, total = None, {}
    for raw in text.splitlines():
        header = re.match(r'--- (\w+) quarantine: (\d+) rows ---', raw)
        if header:
            kind = header.group(1)
            total[kind] = int(header.group(2))
            continue
        row = re.match(r'^\s*(\w+)\s+(.+?)\s+(\d+)$', raw)
        if row and kind and row.group(1) != 'source':
            lines.append(f'| {kind} ({_n(total[kind])} rows) | {row.group(1)} | {row.group(2)} | {_n(row.group(3))} |')
    if len(lines) == 2:
        raise ValueError('no quarantine reasons found in the saved log')
    return '\n'.join(lines)


def render_partition_load(load_log: str, month_log: str, audit_log: str) -> str:
    loads = re.findall(r'^Partition (\S+): (\d+) rows, (\d+) affected', load_log, re.MULTILINE)
    if len(loads) != 2:
        raise ValueError('expected two load-partition results (first load and rerun) in the saved log')
    in_db = re.findall(r'^\s*(\d{4}-\d{2})\s*\|\s*(\d+)\s*$', month_log, re.MULTILINE)
    audit = _grab(r'^\s*(\d{4}-\d{2})\s*\|\s*(\d+)\s*\|\s*(\S+)\s*$', audit_log, 'audit row')
    validate = _grab(r'^Validation PASSED: (\d+) curated rows checked; (\d+) of them confirmed present in PostgreSQL '
                     r'\(scope: (\S+)\)', load_log, 'scoped validation')
    key = loads[0][0]
    lines = [
        '| Item | Value |', '| :--- | ---: |',
        f'| Partition loaded | {key} |',
        f'| First load: rows in partition / rows inserted or updated | {_n(loads[0][1])} / {_n(loads[0][2])} |',
        f'| Rerun of the same load: rows inserted or updated | {_n(loads[1][2])} |',
        '| Months present in PostgreSQL afterwards (verification query) | '
        + ', '.join(f'{m} ({_n(c)} rows)' for m, c in in_db) + ' |',
        f'| `audit.partition_loads` row | {audit.group(1)}, row_count {_n(audit.group(2))}, run `{audit.group(3)}` |',
        f'| `validate` after the partition load | {_n(validate.group(1))} rows checked; '
        f'{_n(validate.group(2))} confirmed in PostgreSQL (scope {validate.group(3)}) |',
    ]
    return '\n'.join(lines)


def _seconds(value) -> str:
    return '—' if pd.isna(value) or value == '' else f'{float(value):.4f}'


def render_benchmark(results: pd.DataFrame, context: dict) -> str:
    ram = context.get('ram_bytes')
    ram_text = f'{ram / 2**30:.1f} GiB RAM' if ram else 'RAM not recorded'
    full = results[results['scenario'] == 'full_table']
    part = results[results['scenario'].str.startswith('partition_')]
    repeats = int(results['repeats'].iloc[0])
    lines = [
        f'Measured on: {context["os"]}; {context.get("cpu_model") or "CPU not recorded"}, '
        f'{context["cpu_count"]} logical CPUs, {ram_text}; Python {context["python"]}, pandas {context["pandas"]}, '
        f'pyarrow {context["pyarrow"]}, psycopg {context["psycopg"]}'
        + (f', PostgreSQL {context["postgres_server_version"]}' if context.get('postgres_server_version') else '')
        + f'. Each value is the median of {repeats} timed runs after 1 un-timed warm-up '
        f'(measured {context["measured_at_utc"][:19].replace("T", " ")} UTC). '
        'The numbers describe this machine only; see `docs/evidence/benchmark/benchmark_timings.json` for every individual timing.',
        '',
        '| Storage format | Size (bytes) | Size (MB) | Write (s) | Full read (s) | Filtered read (s) | Rows | Rows after filter |',
        '| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |',
    ]
    for _, row in full.iterrows():
        lines.append(
            f'| {row["storage_type"]} | {_n(row["file_size_bytes"])} | {row["file_size_bytes"] / 1e6:.2f} | '
            f'{_seconds(row["write_seconds"])} | {_seconds(row["full_read_seconds"])} | '
            f'{_seconds(row["filtered_read_seconds"])} | {_n(row["row_count"])} | {_n(row["filtered_row_count"])} |')
    lines += ['', 'Reading one month (' + part['scenario'].iloc[0].removeprefix('partition_') + '):', '',
              '| Approach | Size (bytes) | Write (s) | Read one month (s) | Rows returned |',
              '| :--- | ---: | ---: | ---: | ---: |']
    for _, row in part.iterrows():
        lines.append(f'| {row["storage_type"]} | {_n(row["file_size_bytes"])} | {_seconds(row["write_seconds"])} | '
                     f'{_seconds(row["filtered_read_seconds"])} | {_n(row["filtered_row_count"])} |')

    by = full.set_index('storage_type')
    size = by['file_size_bytes']
    pq_vs_csv = (1 - size['Parquet'] / size['CSV']) * 100
    pq_vs_json = (1 - size['Parquet'] / size['JSON Lines']) * 100
    files_only = by.drop(index='PostgreSQL', errors='ignore')
    lines += [
        '',
        f'- Parquet file size is {pq_vs_csv:.1f}% smaller than CSV and {pq_vs_json:.1f}% smaller than JSON Lines '
        f'({_n(size["Parquet"])} vs {_n(size["CSV"])} and {_n(size["JSON Lines"])} bytes).',
        f'- Fastest median full read: {by["full_read_seconds"].astype(float).idxmin()} '
        f'({by["full_read_seconds"].astype(float).min():.4f} s); slowest: '
        f'{by["full_read_seconds"].astype(float).idxmax()} ({by["full_read_seconds"].astype(float).max():.4f} s).',
        f'- Fastest median write among the file formats: {files_only["write_seconds"].astype(float).idxmin()} '
        f'({files_only["write_seconds"].astype(float).min():.4f} s).',
    ]
    base = part[part['storage_type'].str.contains('single file')]['filtered_read_seconds'].astype(float).iloc[0]
    parted = part[part['storage_type'].str.contains('partitioned')]['filtered_read_seconds'].astype(float).iloc[0]
    lines.append(f'- One-month read: {parted:.4f} s from the partitioned dataset vs {base:.4f} s from the single file '
                 f'({base / parted:.1f}x faster).')
    return '\n'.join(lines)


def build_blocks() -> dict:
    results = pd.read_csv(BENCH / 'benchmark_results.csv')
    context = json.loads((BENCH / 'benchmark_context.json').read_text(encoding='utf-8'))
    return {
        'pipeline_counts': render_counts((LOGS / '02_run_all.txt').read_text(encoding='utf-8')),
        'quarantine_reasons': render_quarantine((LOGS / '06_quarantine_reasons.txt').read_text(encoding='utf-8')),
        'partition_load': render_partition_load((LOGS / '11_partition_load.txt').read_text(encoding='utf-8'),
                                                (LOGS / '12_partition_db_verification.txt').read_text(encoding='utf-8'),
                                                (LOGS / '13_partition_audit.txt').read_text(encoding='utf-8')),
        'validate_env_log': '```text\n' + (LOGS / '01_validate_env.txt').read_text(encoding='utf-8').rstrip() + '\n```',
        'partition_facts': render_partitions((LOGS / '09_partition_write.txt').read_text(encoding='utf-8'),
                                             (LOGS / '10_partition_read.txt').read_text(encoding='utf-8')),
        'benchmark': render_benchmark(results, context),
    }


def inject(text: str, blocks: dict) -> str:
    """Replace the body of every ``blocks`` entry whose markers exist in ``text``."""
    for name, body in blocks.items():
        begin, end = f'<!-- BEGIN:{name} -->', f'<!-- END:{name} -->'
        pattern = re.compile(re.escape(begin) + r'.*?' + re.escape(end), re.DOTALL)
        if pattern.search(text):
            text = pattern.sub(lambda m: f'{begin}\n{body}\n{end}', text)
    return text


def _updated(path: Path, blocks: dict, required: list[str]) -> tuple[str, str]:
    current = path.read_text(encoding='utf-8')
    for name in required:
        if f'<!-- BEGIN:{name} -->' not in current or f'<!-- END:{name} -->' not in current:
            raise ValueError(f'{path.name} has no <!-- BEGIN:{name} --> ... <!-- END:{name} --> block')
    return current, inject(current, blocks)


TARGETS = {
    REPORT: ['pipeline_counts', 'quarantine_reasons', 'partition_load', 'partition_facts', 'benchmark'],
    ENV_DOC: ['validate_env_log'],
}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--check', action='store_true', help='do not write; fail if a document is out of date')
    args = parser.parse_args(argv)
    blocks = build_blocks()
    stale = []
    for path, required in TARGETS.items():
        current, updated = _updated(path, blocks, required)
        if updated != current:
            stale.append(path.name)
            if not args.check:
                path.write_text(updated, encoding='utf-8')
    if args.check:
        if stale:
            print(f'{", ".join(stale)} do not match the saved results; run: python -m src.report_tables', file=sys.stderr)
            return 1
        print('report.md and environment_verification.md match the saved results.')
        return 0
    print('Updated: ' + (', '.join(stale) if stale else 'nothing to change (already current)'))
    return 0


if __name__ == '__main__':
    sys.exit(main())
