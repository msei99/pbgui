"""DB Sync recovery does not strand pending restarts or touch live workers."""

from contextlib import nullcontext
from unittest.mock import Mock
import task_queue
import pytest
from api import db_tools
import task_worker


@pytest.mark.parametrize('state', ['pending', 'running'])
@pytest.mark.parametrize('result', ['ok', 'failed', 'exception'])
def test_recovery_restart_outcome(monkeypatch, state, result):
    """Both orphan states become terminal on failed launch and remain retryable."""
    fail = Mock(return_value=True)
    retry = Mock(return_value=True)
    start = Mock(return_value=(result == 'ok', 'launch failed'))
    if result == 'exception':
        start.side_effect = OSError('spawn failed')
    monkeypatch.setattr(db_tools, 'force_fail_job', fail)
    monkeypatch.setattr(db_tools, 'retry_failed_job', retry)
    monkeypatch.setattr(task_worker, 'start_pending_job', start)
    monkeypatch.setattr(db_tools, 'is_pid_running', lambda pid: False)
    task = {'id':'task','status':state,'created_ts':1,'worker_pid':123}
    monkeypatch.setattr(task_queue, '_task_queue_lock', nullcontext)
    monkeypatch.setattr(db_tools, '_task_jobs_by_id', lambda: {'task':task})
    db_tools._recover_db_sync_task(task)
    start.assert_called_once_with('task')
    assert retry.call_count == int(state == 'running')
    assert fail.call_count == int(state == 'running') + int(result != 'ok')
    if result != 'ok':
        assert 'Failed to restart DB Sync' in fail.call_args.kwargs['error']


@pytest.mark.parametrize('task', [
    {'status':'running','worker_pid':123,'created_ts':1},
    {'status':'pending','worker_pid':123,'created_ts':1},
    {'status':'done','created_ts':1},
    {'status':'failed','created_ts':1},
])
def test_live_and_terminal_tasks_untouched(monkeypatch, task):
    """A live PID or terminal state is never requeued by reconciliation."""
    monkeypatch.setattr(db_tools, 'is_pid_running', lambda pid: True)
    fail = Mock()
    monkeypatch.setattr(db_tools, 'force_fail_job', fail)
    monkeypatch.setattr(task_queue, '_task_queue_lock', nullcontext)
    monkeypatch.setattr(db_tools, '_task_jobs_by_id', lambda: {'task':dict(task,id='task')})
    db_tools._recover_db_sync_task(dict(task,id='task'))
    fail.assert_not_called()


def test_failed_restart_clears_sync_running_state(monkeypatch):
    """Reconciliation publishes launch failure and releases the UI's running flag."""
    task = {'id':'task','type':'db_sync','status':'pending','created_ts':1}
    job = {'id':'sync','worker_job_id':'task','running':True,'enabled':False}
    monkeypatch.setattr(db_tools, '_sync_jobs', {'sync':job})
    monkeypatch.setattr(db_tools, '_task_jobs_by_id', lambda: {'task':task})
    monkeypatch.setattr(task_queue, '_task_queue_lock', nullcontext)
    monkeypatch.setattr(task_worker, 'start_pending_job', lambda job_id: (False, 'slots full'))
    save = Mock()
    monkeypatch.setattr(db_tools, '_save_sync_jobs', save)

    def fail(job_id, *, error):
        """Model durable failure publication without accessing task files."""
        task.update(status='failed',error=error)
        return True

    monkeypatch.setattr(db_tools, 'force_fail_job', fail)
    db_tools._reconcile_sync_worker_jobs()
    assert job['running'] is False
    assert job['worker_job_id'] == ''
    assert 'slots full' in job['last_error']
    save.assert_called_once()
