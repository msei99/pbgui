"""Recover dead task owners using isolated queue files and mocked processes."""

import ast
import json
import os
from pathlib import Path
from unittest.mock import Mock

import pytest

import task_queue
import task_worker as worker


@pytest.fixture
def queue(tmp_path, monkeypatch):
    """Redirect all queue operations and diagnostics away from runtime data."""
    monkeypatch.setattr(task_queue, 'get_tasks_root_dir', lambda: tmp_path)
    monkeypatch.setattr(worker, '_job_log', Mock())
    monkeypatch.setattr(worker, 'is_pid_running', lambda pid: pid == 100)
    task_queue.ensure_task_dirs()
    return tmp_path


def job(queue, name='job', pid=200, age=120, cancelled=False, state='running'):
    """Create a temporary job with an explicitly controlled ownership age."""
    path = queue / state / (name + '.json')
    path.write_text(json.dumps({'id':name, 'type':'hl_best_1m', 'status':state,
        'worker_pid':pid, 'cancel_requested':cancelled, 'manual_parallel':True,
        'run_requested':True, 'run_requested_ts':10, 'run_started_ts':20}))
    timestamp = worker.time.time() - age
    os.utime(path, (timestamp, timestamp))
    return path


@pytest.mark.parametrize('cancelled', [False, True])
def test_dead_owner_recovered_and_flags_reset(queue, cancelled):
    """A cancelled orphan fails; an interrupted orphan becomes runnable again."""
    source = job(queue, cancelled=cancelled)
    worker._requeue_stale_running_jobs()
    assert not source.exists()
    state = 'failed' if cancelled else 'pending'
    data = json.loads((queue / state / source.name).read_text())
    assert data['status'] == state
    assert data['worker_pid'] == data['run_started_ts'] == data['run_requested_ts'] == 0
    assert data['manual_parallel'] is data['run_requested'] is False
    assert data['error'] == ('cancelled' if cancelled else 'requeued after worker exit')
    worker._requeue_stale_running_jobs()
    assert len(list((queue / state).glob('*.json'))) == 1


@pytest.mark.parametrize('pid,age', [(100,10000),(200,0),(0,0)])
def test_live_and_launching_jobs_untouched(queue, pid, age):
    """Even startup recovery must preserve live owners and fresh claims."""
    source = job(queue, pid=pid, age=age)
    original = source.read_bytes()
    worker._requeue_stale_running_jobs(max_age_s=0)
    assert source.read_bytes() == original


def test_pidless_orphan_recovered_after_grace(queue):
    """A launcher that died before publishing its PID cannot block forever."""
    source = job(queue, pid=0)
    worker._requeue_stale_running_jobs(max_age_s=60)
    assert (queue / 'pending' / source.name).exists()


def test_manual_run_reclaims_dead_manual_slot(queue, monkeypatch):
    """The API Run path frees an abandoned manual slot before counting limits."""
    job(queue, name='dead')
    job(queue, name='next', pid=0, state='pending')
    spawn = Mock()
    monkeypatch.setattr(worker.subprocess, 'Popen', spawn)
    ok, detail = worker.start_pending_job('next')
    assert ok, detail
    spawn.assert_called_once()
    assert (queue / 'pending/dead.json').exists()
    assert (queue / 'running/next.json').exists()


def test_recovery_holds_queue_lock_across_read_modify_move(queue, monkeypatch):
    """Recovery joins the same reentrant transaction used by cancel and queue writes."""
    job(queue)
    real_lock = worker._task_queue_lock
    held = []
    real_move = worker.move_job_file

    class Lease:
        """Observe the outer recovery lock without changing its behavior."""
        def __enter__(self):
            """Acquire the actual cross-process queue lock."""
            self.context = real_lock()
            self.context.__enter__()
            held.append(True)
        def __exit__(self, *args):
            """Release after all persistence is complete."""
            held.pop()
            return self.context.__exit__(*args)

    def move(*args):
        """The whole mutation and state move must remain within the outer lease."""
        assert held
        return real_move(*args)

    monkeypatch.setattr(worker, '_task_queue_lock', Lease)
    monkeypatch.setattr(worker, 'move_job_file', move)
    worker._requeue_stale_running_jobs()
    assert not held


def test_daemon_recovers_before_counting_running_slots():
    """The scheduling loop must run recovery each cycle, not only at startup."""
    tree = ast.parse(Path(worker.__file__).read_text())
    main = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'main')
    loop = next(n for n in ast.walk(main) if isinstance(n, ast.While) and ast.unparse(n.test) == 'not _STOP')
    body = ast.unparse(loop)
    assert body.index('_requeue_stale_running_jobs(max_age_s=60)') < body.index('running_counts:')
