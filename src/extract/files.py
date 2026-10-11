from pathlib import Path

from src.common.audit import sanitize_run_id
from src.common.runtime import atomic_copy, raw_dir_for
from src.config import path_for

SOURCE_FILES = ['customers.csv', 'products.json', 'orders.csv']

EXPECTED_CSV_COLUMNS = {
    'customers.csv': [
        'customer_id', 'first_name', 'last_name', 'email', 'city',
        'customer_tier', 'created_at', 'updated_at',
    ],
    'orders.csv': [
        'order_id', 'customer_id', 'product_id', 'order_timestamp', 'quantity',
        'unit_price', 'discount_pct', 'status', 'updated_at',
    ],
}


def extract_sources(run_id: str) -> Path:
    """Copy immutable source snapshots into a run-specific raw directory.

    Each file is copied to a temporary name and moved into place atomically, so
    a retry of the same run id (Airflow retries reuse the id) can never leave a
    half-copied file behind, and different run ids never share a directory.
    """
    run_id = sanitize_run_id(run_id)
    source_dir = path_for('source_dir')

    missing = [name for name in SOURCE_FILES if not (source_dir / name).is_file()]
    if missing:
        raise FileNotFoundError(
            f'Missing source file(s) in {source_dir}: {", ".join(missing)}'
        )

    raw_dir = raw_dir_for(run_id)
    raw_dir.mkdir(parents=True, exist_ok=True)
    for name in SOURCE_FILES:
        atomic_copy(source_dir / name, raw_dir / name)
    return raw_dir
