"""Safe concurrent / repeated runs: run ids, atomic writes, locking, isolated outputs."""
import os
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd
import pytest

from factories import default_dataset, write_source
from src import config, pipeline
from src.common.audit import new_run_id, sanitize_run_id
from src.common.runtime import (
    RunIdError, atomic_copy, atomic_write_parquet, atomic_write_text, curated_path_for,
    file_lock, manifest_path, raw_dir_for, read_manifest, record_stage, resolve_run_id,
    set_latest_run, staging_dir_for,
)

REPO = Path(__file__).resolve().parents[1]


def leftovers(folder):
    return [p for p in Path(folder).rglob('*') if p.name.endswith('.tmp') or p.name.endswith('.old')]


# --------------------------------------------------------------- run ids
@pytest.mark.parametrize('raw', [
    'manual__2026-09-23T12:53:45+00:00',          # a real Airflow run id (see screenshots)
    'scheduled__2026-09-23T02:00:00+00:00',
    '../../etc/passwd', 'a/b\\c', 'run id with spaces', 'CON:',
])
def test_sanitised_run_ids_are_safe_path_components(raw):
    clean = sanitize_run_id(raw)
    assert clean and not set(clean) & set(':+/\\ ') and not clean.startswith('.')
    assert '..' not in clean.replace('...', '') or clean.strip('.') == clean
    assert sanitize_run_id(clean) == clean         # idempotent


def test_airflow_style_id_maps_to_a_windows_safe_name():
    assert sanitize_run_id('manual__2026-09-23T12:53:45+00:00') == 'manual__2026-09-23T12_53_45_00_00'


@pytest.mark.parametrize('bad', ['', '   ', '...', '/'.replace('/', '.')])
def test_empty_run_ids_are_rejected(bad):
    with pytest.raises(ValueError):
        sanitize_run_id(bad)


def test_every_path_for_a_run_stays_inside_its_folder(project):
    evil = '../../escape'
    for path in (raw_dir_for(evil), staging_dir_for(evil), curated_path_for(evil), manifest_path(evil)):
        assert project in path.resolve().parents


def test_new_run_id_sanitises_the_environment_value(monkeypatch):
    monkeypatch.setenv('PIPELINE_RUN_ID', 'manual__2026-09-23T12:53:45+00:00')
    assert new_run_id() == 'manual__2026-09-23T12_53_45_00_00'


def test_generated_run_ids_are_unique():
    assert len({new_run_id() for _ in range(200)}) == 200


def test_run_id_precedence_explicit_then_env_then_latest(project, monkeypatch):
    with pytest.raises(RunIdError, match='No run id given'):
        resolve_run_id()
    set_latest_run('from_latest')
    assert resolve_run_id() == 'from_latest'
    monkeypatch.setenv('PIPELINE_RUN_ID', 'from_env')
    assert resolve_run_id() == 'from_env'
    assert resolve_run_id('explicit') == 'explicit'


def test_no_code_depends_on_the_shared_current_run_id_file():
    offenders = [str(p) for p in list((REPO / 'src').rglob('*.py')) + [REPO / 'dags' / 'dss150p_pipeline.py']
                 if 'current_run_id' in p.read_text(encoding='utf-8')]
    assert offenders == []
    ignore = (REPO / '.gitignore').read_text(encoding='utf-8')
    assert 'state/runs/' in ignore and 'state/latest_run.txt' in ignore


# ---------------------------------------------------------- atomic writes
def test_atomic_writes_leave_no_temp_files(tmp_path):
    atomic_write_text(tmp_path / 'a.txt', 'hello')
    atomic_write_parquet(pd.DataFrame({'x': [1, 2]}), tmp_path / 'b.parquet')
    atomic_copy(tmp_path / 'a.txt', tmp_path / 'c.txt')
    assert (tmp_path / 'c.txt').read_text() == 'hello' and len(pd.read_parquet(tmp_path / 'b.parquet')) == 2
    assert leftovers(tmp_path) == [] and sorted(os.listdir(tmp_path)) == ['a.txt', 'b.parquet', 'c.txt']


def test_a_crash_before_the_final_move_keeps_the_old_file_and_cleans_up(tmp_path, monkeypatch):
    """Fail at the last step (the atomic move). A non-atomic writer would already have
    overwritten the target by then; an atomic one has only touched a temp file."""
    target = tmp_path / 'data.parquet'
    atomic_write_parquet(pd.DataFrame({'x': [1, 2, 3]}), target)

    def crash(src, dst):
        raise OSError('power cut')

    monkeypatch.setattr('src.common.runtime.os.replace', crash)
    with pytest.raises(OSError):
        atomic_write_parquet(pd.DataFrame({'x': [9]}), target)
    monkeypatch.undo()
    assert list(pd.read_parquet(target)['x']) == [1, 2, 3] and leftovers(tmp_path) == []


def test_a_failure_while_writing_keeps_the_old_file_and_cleans_up(tmp_path, monkeypatch):
    target = tmp_path / 'data.parquet'
    atomic_write_parquet(pd.DataFrame({'x': [1, 2, 3]}), target)
    real = pd.DataFrame.to_parquet

    def half_write_then_fail(self, path, *a, **k):
        real(self, path, *a, **k)                 # the temp file now exists...
        raise OSError('disk full')                # ...then the write "fails"

    monkeypatch.setattr(pd.DataFrame, 'to_parquet', half_write_then_fail)
    with pytest.raises(OSError):
        atomic_write_parquet(pd.DataFrame({'x': [9]}), target)
    monkeypatch.undo()
    assert list(pd.read_parquet(target)['x']) == [1, 2, 3] and leftovers(tmp_path) == []


# ------------------------------------------------------------------ lock
def test_lock_serialises_threads(tmp_path):
    counter, lock_path = tmp_path / 'n.txt', tmp_path / 'x.lock'
    counter.write_text('0')

    def work():
        for _ in range(25):
            with file_lock(lock_path):
                counter.write_text(str(int(counter.read_text()) + 1))

    threads = [threading.Thread(target=work) for _ in range(8)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert counter.read_text() == '200' and not lock_path.exists()


def test_lock_serialises_separate_processes(tmp_path):
    counter, lock_path = tmp_path / 'n.txt', tmp_path / 'x.lock'
    counter.write_text('0')
    script = (
        'import sys\nfrom src.common.runtime import file_lock\n'
        'lock, counter, n = sys.argv[1:4]\n'
        'for _ in range(int(n)):\n'
        '    with file_lock(lock):\n'
        '        v = int(open(counter).read()); open(counter, "w").write(str(v + 1))\n'
    )
    env = {**os.environ, 'PYTHONPATH': str(REPO)}
    procs = [subprocess.Popen([sys.executable, '-c', script, str(lock_path), str(counter), '50'],
                              cwd=REPO, env=env) for _ in range(4)]
    assert [p.wait(timeout=120) for p in procs] == [0, 0, 0, 0]
    assert counter.read_text() == '200'


def test_lock_times_out_when_held_and_recovers_a_stale_lock(tmp_path):
    lock_path = tmp_path / 'held.lock'
    with file_lock(lock_path):
        with pytest.raises(TimeoutError):
            with file_lock(lock_path, timeout=0.2):
                pass
    lock_path.write_text('99999')                       # orphan from a crashed process
    old = lock_path.stat().st_mtime - 1000
    os.utime(lock_path, (old, old))
    with file_lock(lock_path, timeout=1, stale_after=300):
        pass
    assert not lock_path.exists()


def test_lock_is_released_when_the_body_raises(tmp_path):
    lock_path = tmp_path / 'x.lock'
    with pytest.raises(RuntimeError):
        with file_lock(lock_path):
            raise RuntimeError('boom')
    assert not lock_path.exists()


# ------------------------------------------------------------- manifests
def test_manifest_records_stages_per_run_without_mixing_runs(project):
    record_stage('run_a', 'extract', 'success', rows=1)
    record_stage('run_a', 'load', 'failed', error='x')
    record_stage('run_b', 'extract', 'success', rows=2)
    a, b = read_manifest('run_a'), read_manifest('run_b')
    assert set(a['stages']) == {'extract', 'load'} and a['stages']['load']['status'] == 'failed'
    assert set(b['stages']) == {'extract'} and b['stages']['extract']['rows'] == 2
    assert read_manifest('missing') is None


def test_concurrent_manifest_updates_for_one_run_lose_nothing(project):
    stages = [f's{i}' for i in range(20)]
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda s: record_stage('run_a', s, 'success'), stages))
    assert set(read_manifest('run_a')['stages']) == set(stages)


# ----------------------------------------------- overlapping pipeline runs
def _full_prep(project):
    write_source(project, *default_dataset())


def test_overlapping_runs_with_different_ids_stay_isolated(project):
    _full_prep(project)
    ids = [f'run_{i}' for i in range(6)]

    def go(run_id):
        pipeline.do_extract(run_id)
        return pipeline.do_transform(run_id)['curated']

    with ThreadPoolExecutor(max_workers=6) as pool:
        list(pool.map(go, ids))

    for run_id in ids:
        curated = pd.read_parquet(curated_path_for(run_id))
        assert len(curated) == 5 and set(curated['pipeline_run_id']) == {run_id}
        for name in ('customers', 'products', 'orders'):
            staged = pd.read_parquet(staging_dir_for(run_id) / f'{name}.parquet')
            assert set(staged['pipeline_run_id']) == {run_id}
        manifest = read_manifest(run_id)
        assert manifest['run_id'] == run_id
        assert manifest['stages']['extract']['status'] == manifest['stages']['transform']['status'] == 'success'
    assert (project / 'state' / 'latest_run.txt').read_text() in ids
    assert leftovers(project) == [] and not list((project / 'state' / 'locks').glob('*'))


def test_overlapping_runs_with_the_SAME_id_never_produce_a_corrupt_file(project):
    """An Airflow retry or a double-click can run one id twice at once."""
    _full_prep(project)

    def go(_):
        pipeline.do_extract('shared_id')
        return pipeline.do_transform('shared_id')['curated']

    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(go, range(4)))
    curated = pd.read_parquet(curated_path_for('shared_id'))      # readable, complete
    assert len(curated) == 5 and curated['order_id'].is_unique
    assert len(pd.read_parquet(staging_dir_for('shared_id') / 'orders.parquet')) == 5
    assert leftovers(project) == []


def test_repeating_a_run_gives_identical_business_data(project):
    _full_prep(project)
    pipeline.do_extract('r1'); pipeline.do_transform('r1')
    first = pd.read_parquet(curated_path_for('r1')).sort_values('order_id').reset_index(drop=True)
    pipeline.do_extract('r1'); pipeline.do_transform('r1')
    second = pd.read_parquet(curated_path_for('r1')).sort_values('order_id').reset_index(drop=True)
    cols = [c for c in first.columns if c != 'processed_at_utc']
    pd.testing.assert_frame_equal(first[cols], second[cols])
    assert leftovers(project) == []


def test_a_later_run_does_not_change_an_earlier_runs_pointer_based_commands(project):
    """Stages given an explicit id ignore the 'latest' pointer entirely."""
    _full_prep(project)
    pipeline.do_extract('first')
    pipeline.do_extract('second')                      # latest now points at 'second'
    assert resolve_run_id() == 'second'
    assert resolve_run_id('first') == 'first'
    pipeline.do_transform('first')
    assert curated_path_for('first').is_file() and not curated_path_for('second').exists()
