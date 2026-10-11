"""Run-scoped paths, atomic file writes, a portable lock, and per-run state.

Why this module exists
----------------------
Earlier versions shared mutable files between runs (a single "current run id"
file in state/, fixed ``data/staging/*.parquet`` names, fixed benchmark file names). Two overlapping runs
could read each other's run id or overwrite each other's outputs, and a crash
in the middle of a write left half-written Parquet files behind.

The rules now are:
  * every run writes only under directories named after its own run id;
  * every file is written to a temporary name and moved into place with
    ``os.replace`` (atomic on the same filesystem), so readers never see a
    partial file;
  * the few files that several runs *do* share (the "latest run" pointer and a
    run's manifest) are updated under a lock;
  * later stages find their run id from ``--run-id`` / ``PIPELINE_RUN_ID``
    first and only fall back to the "latest run" pointer for manual use.
"""
import contextlib
import json
import os
import shutil
import time
import uuid
from pathlib import Path

from src import config
from src.common.audit import sanitize_run_id, utc_now_iso


class RunIdError(RuntimeError):
    """No usable run id could be determined."""


# --------------------------------------------------------------------------
# Per-run paths. Everything a run writes lives under its own run id.
# --------------------------------------------------------------------------
def raw_dir_for(run_id: str) -> Path:
    return config.path_for('raw_dir') / f'run_id={sanitize_run_id(run_id)}'


def staging_dir_for(run_id: str) -> Path:
    return config.path_for('staging_dir') / f'run_id={sanitize_run_id(run_id)}'


def curated_path_for(run_id: str) -> Path:
    run_id = sanitize_run_id(run_id)
    return config.path_for('curated_dir') / f'sales_order_lines_run_id={run_id}.parquet'


def quarantine_path_for(kind: str, run_id: str) -> Path:
    run_id = sanitize_run_id(run_id)
    return config.path_for('quarantine_dir') / f'{kind}_run_id={run_id}.parquet'


def benchmark_dir_for(run_id: str) -> Path:
    return config.path_for('benchmark_dir') / f'run_id={sanitize_run_id(run_id)}'


def partition_dir_for(run_id: str) -> Path:
    return config.path_for('partition_dir') / f'run_id={sanitize_run_id(run_id)}'


def state_dir() -> Path:
    return config.PROJECT_ROOT / 'state'


# --------------------------------------------------------------------------
# Atomic writes
# --------------------------------------------------------------------------
def _temp_sibling(path: Path) -> Path:
    # Leading "." keeps the half-written file invisible to Parquet dataset
    # readers and glob patterns that skip hidden files.
    return path.with_name(f'.{path.name}.{os.getpid()}.{uuid.uuid4().hex[:8]}.tmp')


def atomic_write_text(path, text: str) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = _temp_sibling(path)
    try:
        tmp.write_text(text, encoding='utf-8')
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)
    return path


def atomic_write_parquet(df, path, **kwargs) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = _temp_sibling(path)
    try:
        df.to_parquet(tmp, index=False, **kwargs)
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)
    return path


def atomic_copy(src, dst) -> Path:
    dst = Path(dst)
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = _temp_sibling(dst)
    try:
        shutil.copy2(src, tmp)
        os.replace(tmp, dst)
    finally:
        tmp.unlink(missing_ok=True)
    return dst


def replace_directory(new_dir, final_dir) -> Path:
    """Swap a fully written directory into place, removing the old one."""
    new_dir, final_dir = Path(new_dir), Path(final_dir)
    final_dir.parent.mkdir(parents=True, exist_ok=True)
    if final_dir.exists():
        old = final_dir.with_name(f'.{final_dir.name}.{uuid.uuid4().hex[:8]}.old')
        os.replace(final_dir, old)
        os.replace(new_dir, final_dir)
        shutil.rmtree(old, ignore_errors=True)
    else:
        os.replace(new_dir, final_dir)
    return final_dir


# --------------------------------------------------------------------------
# Portable lock (works the same on Windows, Linux and Docker bind mounts)
# --------------------------------------------------------------------------
@contextlib.contextmanager
def file_lock(path, timeout: float = 30.0, stale_after: float = 300.0, poll: float = 0.02):
    """Hold an exclusive lock by creating ``path`` with O_CREAT|O_EXCL.

    Creating a file that must not already exist is atomic on every OS, so only
    one holder can succeed. Critical sections here last milliseconds; a lock
    older than ``stale_after`` seconds is assumed to belong to a crashed
    process and is removed. (Known limit: if a holder crashes AND two waiters
    break the stale lock in the same instant, both may proceed. That needs a
    crash plus a sub-millisecond coincidence and only affects bookkeeping files.)
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + timeout
    while True:
        try:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            try:
                if time.time() - path.stat().st_mtime > stale_after:
                    path.unlink(missing_ok=True)
                    continue
            except FileNotFoundError:
                continue
            if time.monotonic() >= deadline:
                raise TimeoutError(f'could not acquire lock {path} within {timeout}s')
            time.sleep(poll)
            continue
        try:
            os.write(fd, str(os.getpid()).encode())
        finally:
            os.close(fd)
        break
    try:
        yield
    finally:
        path.unlink(missing_ok=True)


# --------------------------------------------------------------------------
# Run id resolution and per-run manifest
# --------------------------------------------------------------------------
def _latest_pointer() -> Path:
    return state_dir() / 'latest_run.txt'


def set_latest_run(run_id: str) -> None:
    """Remember the most recent run for manual use (convenience only)."""
    with file_lock(state_dir() / 'locks' / 'latest.lock'):
        atomic_write_text(_latest_pointer(), sanitize_run_id(run_id))


def resolve_run_id(explicit: str | None = None) -> str:
    """Pick the run id for a stage: --run-id, then PIPELINE_RUN_ID, then latest.

    Orchestrated/concurrent runs must pass the first two. The "latest" pointer
    is a convenience for typing commands by hand and is refused when empty.
    """
    for candidate in (explicit, os.getenv('PIPELINE_RUN_ID')):
        if candidate:
            return sanitize_run_id(candidate)
    pointer = _latest_pointer()
    if pointer.is_file():
        value = pointer.read_text(encoding='utf-8').strip()
        if value:
            return sanitize_run_id(value)
    raise RunIdError(
        'No run id given. Pass --run-id, set PIPELINE_RUN_ID, or run `extract` first.'
    )


def manifest_path(run_id: str) -> Path:
    return state_dir() / 'runs' / f'{sanitize_run_id(run_id)}.json'


def read_manifest(run_id: str) -> dict | None:
    path = manifest_path(run_id)
    if not path.is_file():
        return None
    return json.loads(path.read_text(encoding='utf-8'))


def record_stage(run_id: str, stage: str, status: str, **details) -> dict:
    """Record one stage's outcome in this run's own manifest (under a lock)."""
    run_id = sanitize_run_id(run_id)
    with file_lock(state_dir() / 'locks' / f'{run_id}.lock'):
        manifest = read_manifest(run_id) or {
            'run_id': run_id,
            'created_at_utc': utc_now_iso(),
            'stages': {},
        }
        manifest['stages'][stage] = {
            'status': status,
            'recorded_at_utc': utc_now_iso(),
            **details,
        }
        atomic_write_text(
            manifest_path(run_id),
            json.dumps(manifest, indent=2, sort_keys=True, default=str),
        )
    return manifest
