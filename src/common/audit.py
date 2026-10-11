from datetime import datetime, timezone
import os
import re
import hashlib
import json
import uuid

_UNSAFE_RUN_ID_CHARS = re.compile(r'[^A-Za-z0-9_.=-]')
MAX_RUN_ID_LENGTH = 120


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sanitize_run_id(run_id: str) -> str:
    """Make a run id safe to use in file/directory names on every OS.

    Airflow run ids look like ``manual__2026-09-23T12:53:45+00:00``; ``:`` and
    ``+`` are not valid in Windows paths and a ``/`` or ``..`` could escape the
    run directory. Every character outside ``[A-Za-z0-9_.=-]`` becomes ``_``.
    The same function is applied everywhere a run id is turned into a path or
    stored in PostgreSQL, so the on-disk id and the database id always match.
    """
    cleaned = _UNSAFE_RUN_ID_CHARS.sub('_', str(run_id).strip()).strip('.')
    if not cleaned:
        raise ValueError(f'run id {run_id!r} is empty after sanitising')
    return cleaned[:MAX_RUN_ID_LENGTH]


def new_run_id() -> str:
    env_value = os.getenv('PIPELINE_RUN_ID')
    if env_value:
        return sanitize_run_id(env_value)
    return f"run_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}_{uuid.uuid4().hex[:8]}"


def record_hash(record: dict, keys: list[str]) -> str:
    payload = {k: record.get(k) for k in keys}
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()
