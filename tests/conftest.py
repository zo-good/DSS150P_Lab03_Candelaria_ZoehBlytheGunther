import uuid
from pathlib import Path

import psycopg
import pytest

from src import config, pipeline
from src.common.db import connect
from factories import default_dataset, write_source

REPO = Path(__file__).resolve().parents[1]


@pytest.fixture
def project(tmp_path, monkeypatch):
    """An isolated project root: its own data/ and state/ folders, real settings."""
    monkeypatch.setattr(config, 'PROJECT_ROOT', tmp_path)
    monkeypatch.delenv('PIPELINE_RUN_ID', raising=False)
    (tmp_path / 'data' / 'source').mkdir(parents=True)
    return tmp_path


@pytest.fixture
def make_curated(project):
    """Run extract + transform on synthetic data and return the curated DataFrame."""
    def _make(run_id='run_a', customers=None, products=None, orders=None):
        default_c, default_p, default_o = default_dataset()
        write_source(project,
                     default_c if customers is None else customers,
                     default_p if products is None else products,
                     default_o if orders is None else orders)
        pipeline.do_extract(run_id)
        return pipeline.do_transform(run_id)['curated']
    return _make


@pytest.fixture(scope='session')
def pg_database():
    """A throwaway database built from the repo's own schema SQL (skips if no server)."""
    creds = dict(config.DB)
    try:
        admin = psycopg.connect(host=creds['host'], port=creds['port'], dbname='postgres',
                                user=creds['user'], password=creds['password'],
                                autocommit=True, connect_timeout=3)
    except psycopg.OperationalError as e:
        pytest.skip(f'PostgreSQL not reachable: {e}')
    name = f'dss150p_test_{uuid.uuid4().hex[:8]}'
    admin.execute(f'CREATE DATABASE {name}')
    schema = '\n'.join(line for line in (REPO / 'sql' / 'init' / '01_warehouse_schema.sql')
                       .read_text(encoding='utf-8').splitlines() if not line.startswith('\\connect'))
    with psycopg.connect(host=creds['host'], port=creds['port'], dbname=name,
                         user=creds['user'], password=creds['password']) as conn:
        conn.execute(schema)
    yield name
    admin.execute(f'DROP DATABASE IF EXISTS {name} WITH (FORCE)')
    admin.close()


@pytest.fixture
def pg(pg_database, monkeypatch):
    """Point the pipeline at the throwaway database with empty tables."""
    monkeypatch.setitem(config.DB, 'dbname', pg_database)
    with connect() as conn:
        conn.execute('TRUNCATE curated.sales_order_lines, audit.partition_loads, audit.pipeline_runs')
    return pg_database
