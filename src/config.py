from pathlib import Path
import os
import yaml
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / '.env')

with (PROJECT_ROOT / 'config' / 'settings.yml').open(encoding='utf-8') as f:
    SETTINGS = yaml.safe_load(f)

# Environment variables the pipeline needs. `validate-env` checks each one.
REQUIRED_ENV_VARS = [
    'POSTGRES_HOST',
    'POSTGRES_PORT',
    'POSTGRES_DB',
    'POSTGRES_USER',
    'POSTGRES_PASSWORD',
]

def _port_from_env(raw: str):
    # A non-numeric port must not crash the import: `validate-env` has to be able to
    # load this module in order to report the problem. The bad text is kept as-is so
    # that actually connecting fails loudly (psycopg rejects it) instead of silently
    # falling back to the default port.
    raw = raw.strip()
    return int(raw) if raw.isdigit() else raw


DB = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': _port_from_env(os.getenv('POSTGRES_PORT', '5432')),
    'dbname': os.getenv('POSTGRES_DB', 'dss150p'),
    'user': os.getenv('POSTGRES_USER', 'dss150p'),
    'password': os.getenv('POSTGRES_PASSWORD', 'change_me'),
}


def path_for(key: str) -> Path:
    # PROJECT_ROOT is looked up at call time (not import time) so tests can
    # point the whole pipeline at a temporary project directory.
    return PROJECT_ROOT / SETTINGS['pipeline'][key]
