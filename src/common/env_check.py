"""Real environment checks behind ``python -m src.cli validate-env``.

Each check returns a Check(name, status, detail). status is 'ok', 'warn' or
'fail'; only 'fail' makes validate-env exit non-zero. Secrets are never printed.
"""
import json
import os
import uuid
from dataclasses import dataclass
from pathlib import Path

from src import config
from src.common.db import connect
from src.extract.files import EXPECTED_CSV_COLUMNS, SOURCE_FILES

REQUIRED_SETTINGS = {
    'pipeline': ['source_dir', 'raw_dir', 'staging_dir', 'curated_dir',
                 'quarantine_dir', 'benchmark_dir', 'partition_dir'],
    'quality': ['allowed_order_statuses', 'min_quantity', 'max_quantity'],
    'storage_benchmark': ['repeats', 'filter_status'],
}
REQUIRED_TABLES = ['curated.sales_order_lines', 'audit.partition_loads', 'audit.pipeline_runs']
PLACEHOLDER_PASSWORD = 'change_me'


@dataclass
class Check:
    name: str
    status: str  # 'ok' | 'warn' | 'fail'
    detail: str


def check_dotenv() -> list[Check]:
    if (config.PROJECT_ROOT / '.env').is_file():
        return [Check('.env file', 'ok', 'found')]
    return [Check('.env file', 'warn',
                  'not found; relying on environment variables (copy .env.example to .env for local runs)')]


def check_settings() -> list[Check]:
    out = []
    settings = config.SETTINGS if isinstance(config.SETTINGS, dict) else {}
    for section, keys in REQUIRED_SETTINGS.items():
        missing = [k for k in keys if k not in (settings.get(section) or {})]
        if missing:
            out.append(Check(f'settings.yml [{section}]', 'fail', f'missing key(s): {", ".join(missing)}'))
        else:
            out.append(Check(f'settings.yml [{section}]', 'ok', 'all required keys present'))
    quality = settings.get('quality') or {}
    if all(k in quality for k in REQUIRED_SETTINGS['quality']):
        lo, hi = quality['min_quantity'], quality['max_quantity']
        statuses = quality['allowed_order_statuses']
        if not (isinstance(lo, int) and isinstance(hi, int) and lo <= hi):
            out.append(Check('quality thresholds', 'fail', f'min_quantity ({lo}) must be <= max_quantity ({hi})'))
        elif not (isinstance(statuses, list) and statuses):
            out.append(Check('quality thresholds', 'fail', 'allowed_order_statuses must be a non-empty list'))
        else:
            out.append(Check('quality thresholds', 'ok', f'quantity {lo}..{hi}, {len(statuses)} allowed statuses'))
    bench = settings.get('storage_benchmark') or {}
    if 'repeats' in bench and 'filter_status' in bench:
        statuses = (quality or {}).get('allowed_order_statuses') or []
        if not isinstance(bench['repeats'], int) or bench['repeats'] < 5:
            out.append(Check('benchmark settings', 'warn', f'repeats={bench["repeats"]!r}; the lab requires at least 5'))
        elif statuses and bench['filter_status'] not in statuses:
            out.append(Check('benchmark settings', 'fail',
                             f'filter_status {bench["filter_status"]!r} is not an allowed order status'))
        else:
            out.append(Check('benchmark settings', 'ok', f'repeats={bench["repeats"]}, filter_status={bench["filter_status"]}'))
    return out


def check_env_vars() -> list[Check]:
    out = []
    missing = [v for v in config.REQUIRED_ENV_VARS if not os.environ.get(v, '').strip()]
    if missing:
        out.append(Check('environment variables', 'fail', f'missing or empty: {", ".join(missing)}'))
    else:
        out.append(Check('environment variables', 'ok', 'all required variables are set'))
    port = os.environ.get('POSTGRES_PORT', '')
    if port.strip():
        if not (port.strip().isdigit() and 1 <= int(port) <= 65535):
            out.append(Check('POSTGRES_PORT', 'fail', f'{port!r} is not a valid port (1-65535)'))
    if os.environ.get('POSTGRES_PASSWORD') == PLACEHOLDER_PASSWORD:
        out.append(Check('POSTGRES_PASSWORD', 'warn', f'still the placeholder {PLACEHOLDER_PASSWORD!r}; fine for a local lab only'))
    return out


def _safe_path(key: str):
    """config.path_for, but None (instead of KeyError) when settings.yml lacks the key."""
    try:
        return config.path_for(key)
    except (KeyError, TypeError):
        return None


def check_source_files() -> list[Check]:
    out = []
    source_dir = _safe_path('source_dir')
    if source_dir is None:
        return [Check('source files', 'fail', 'settings.yml has no pipeline.source_dir')]
    for name in SOURCE_FILES:
        path = source_dir / name
        if not path.is_file():
            out.append(Check(f'source {name}', 'fail', f'missing: {path}'))
            continue
        if path.stat().st_size == 0:
            out.append(Check(f'source {name}', 'fail', 'file is empty'))
            continue
        try:
            if name.endswith('.json'):
                data = json.loads(path.read_text(encoding='utf-8'))
                if not isinstance(data, list) or not data:
                    raise ValueError('expected a non-empty JSON list')
                detail = f'{len(data)} records'
            else:
                header = path.read_text(encoding='utf-8').splitlines()[0].split(',')
                expected = EXPECTED_CSV_COLUMNS[name]
                absent = [c for c in expected if c not in header]
                if absent:
                    raise ValueError(f'missing column(s): {", ".join(absent)}')
                detail = f'header has all {len(expected)} expected columns'
            out.append(Check(f'source {name}', 'ok', detail))
        except Exception as e:  # unreadable / malformed
            out.append(Check(f'source {name}', 'fail', f'{type(e).__name__}: {e}'))
    return out


def check_output_dirs() -> list[Check]:
    out = []
    targets = {}
    for key in ['raw_dir', 'staging_dir', 'curated_dir', 'quarantine_dir', 'benchmark_dir', 'partition_dir']:
        path = _safe_path(key)
        if path is None:
            out.append(Check(f'writable {key}', 'fail', f'settings.yml has no pipeline.{key}'))
        else:
            targets[key] = path
    targets['state'] = config.PROJECT_ROOT / 'state'
    for key, path in targets.items():
        probe = path / f'.write_test_{uuid.uuid4().hex[:8]}'
        try:
            path.mkdir(parents=True, exist_ok=True)
            probe.write_text('ok', encoding='utf-8')
            probe.unlink()
            out.append(Check(f'writable {key}', 'ok', str(path)))
        except OSError as e:
            out.append(Check(f'writable {key}', 'fail', f'{path}: {e}'))
    return out


def check_database() -> list[Check]:
    host, port, name = config.DB['host'], config.DB['port'], config.DB['dbname']
    try:
        with connect(connect_timeout=5) as conn, conn.cursor() as cur:
            cur.execute('SELECT 1')
            out = [Check('database connection', 'ok', f'{host}:{port}/{name}')]
            missing = []
            for table in REQUIRED_TABLES:
                cur.execute('SELECT to_regclass(%s)', (table,))
                if cur.fetchone()[0] is None:
                    missing.append(table)
    except Exception as e:
        return [Check('database connection', 'fail', f'{host}:{port}/{name}: {type(e).__name__}: {e}'.strip())]
    if missing:
        out.append(Check('database tables', 'fail',
                         f'missing: {", ".join(missing)} (run sql/init/01_warehouse_schema.sql)'))
    else:
        out.append(Check('database tables', 'ok', ', '.join(REQUIRED_TABLES)))
    return out


def run_checks(check_db: bool = True) -> list[Check]:
    checks = check_dotenv() + check_settings() + check_env_vars() + check_source_files() + check_output_dirs()
    if check_db:
        checks += check_database()
    return checks


def format_report(checks: list[Check]) -> str:
    tag = {'ok': '[ OK ]', 'warn': '[WARN]', 'fail': '[FAIL]'}
    lines = [f'{tag[c.status]} {c.name}: {c.detail}' for c in checks]
    failed = sum(c.status == 'fail' for c in checks)
    warned = sum(c.status == 'warn' for c in checks)
    lines.append(f'validate-env: {len(checks) - failed - warned} ok, {warned} warning(s), {failed} failure(s)')
    return '\n'.join(lines)


def has_failures(checks: list[Check]) -> bool:
    return any(c.status == 'fail' for c in checks)
