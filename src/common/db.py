import psycopg

from src import config

CONNECT_TIMEOUT_SECONDS = 10


def connect(**overrides):
    """Open a PostgreSQL connection from the current config.DB settings.

    config.DB is read on every call (not copied at import time) so tests and
    tools can switch database name/port without reloading modules.
    """
    params = dict(
        host=config.DB['host'],
        port=config.DB['port'],
        dbname=config.DB['dbname'],
        user=config.DB['user'],
        password=config.DB['password'],
        connect_timeout=CONNECT_TIMEOUT_SECONDS,
    )
    params.update(overrides)
    return psycopg.connect(**params)
