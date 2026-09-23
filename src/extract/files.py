from pathlib import Path
import shutil
from src.config import path_for


def extract_sources(run_id: str) -> Path:
    """Copy immutable source snapshots into a run-specific raw directory."""
    raw_dir = path_for('raw_dir') / f'run_id={run_id}'
    raw_dir.mkdir(parents=True, exist_ok=True)

    source_dir = path_for('source_dir')
    for filename in ['customers.csv', 'products.json', 'orders.csv']:
        shutil.copy2(source_dir / filename, raw_dir / filename)

    return raw_dir