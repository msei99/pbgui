"""Isolated process tests for legacy deadline guard handover and rollback."""
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

import pytest

from setup.vast_gpu_benchmark import guard_migration as migration

pytestmark = pytest.mark.skipif(not hasattr(os, 'pidfd_open'), reason='Linux pidfds required')


@pytest.fixture
def rental_guard(tmp_path):
    """Own fake old guard/optimizer processes and clean up every temporary child."""
    old_source = '''import json,os,time
from pathlib import Path
root=Path(os.environ['PBGUI_WORKDIR'])
state=dict(job_id=os.environ['PBGUI_JOB_ID'],instance_id=int(os.environ['CONTAINER_ID']),
 deadline=float(os.environ['PBGUI_DEADLINE']),max_deadline=float(os.environ['PBGUI_MAX_DEADLINE']),deadline_protocol=1)
(root/'guard.json').write_text(json.dumps(state))
while True: time.sleep(.1)
'''
    worker = tmp_path / 'worker.py'
    worker.write_text(old_source)
    deadline = time.time() + 1800
    env = dict(os.environ, PBGUI_WORKDIR=str(tmp_path), PBGUI_DEADLINE=str(deadline),
               PBGUI_MAX_DEADLINE=str(deadline + 3600), PBGUI_JOB_ID='a'*32,
               CONTAINER_ID='123', CONTAINER_API_KEY='isolated-test-credential')
    old = subprocess.Popen([sys.executable, str(worker), 'guard'], env=env)
    optimizer = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])
    until = time.monotonic() + 5
    while not (tmp_path / 'guard.json').exists() and time.monotonic() < until:
        time.sleep(.01)
    source = Path('setup/vast_gpu_benchmark/cloud_worker.py').read_text()
    request = dict(job_id='a'*32, instance_id=123, expected=deadline, hard_deadline=deadline+7200,
                   source=source, sha256=hashlib.sha256(source.encode()).hexdigest())
    new_pids = []
    try:
        assert (tmp_path / 'guard.json').is_file()
        yield tmp_path, request, old, optimizer, new_pids
    finally:
        for pid in new_pids:
            try:
                os.kill(pid, signal.SIGKILL)
                os.waitpid(pid, 0)
            except (ProcessLookupError, ChildProcessError):
                pass
        for process in (old, optimizer):
            if process.poll() is None:
                process.kill()
            process.wait(timeout=5)


def test_live_handover_preserves_deadline_and_optimizer(rental_guard):
    """Only the identified guard is replaced, after the new guard publishes readiness."""
    root, request, old, optimizer, new_pids = rental_guard
    original_worker = (root / 'worker.py').read_bytes()
    state = migration.migrate(root, request)
    new_pids.append(state['guard_pid'])
    assert state['deadline_protocol'] == 2 and state['deadline'] == request['expected']
    assert state['max_deadline'] == request['expected'] + 3600
    old.wait(timeout=5)
    assert optimizer.poll() is None
    assert (root / 'worker.py').read_bytes() == original_worker
    assert (root / 'deadline-guard-v2.py').stat().st_mode & 0o777 == 0o600
    again = migration.migrate(root, request)
    assert again['guard_pid'] == state['guard_pid']
    # Exercise a real arbitrary-minute request in the migrated guard.
    change = dict(id='b'*32, job_id='a'*32, expected=state['deadline'], deadline=state['deadline']+7*60)
    migration.publish(root / 'deadline-request.json', change)
    until = time.monotonic()+5
    while time.monotonic() < until:
        confirmed = json.loads((root / 'guard.json').read_text())
        if confirmed.get('request_id') == change['id']:
            break
        time.sleep(.05)
    assert confirmed['deadline'] == change['deadline']
    assert optimizer.poll() is None


@pytest.mark.parametrize('failure', ['exit', 'timeout', 'syntax'])
def test_failed_replacement_resumes_original_guard(rental_guard, monkeypatch, failure):
    """A failed child must not leave the old guard stopped or change its deadline."""
    root, request, old, optimizer, _ = rental_guard
    before = json.loads((root / 'guard.json').read_text())
    request['source'] = {'exit': 'raise SystemExit(1)', 'timeout': 'import time; time.sleep(60)', 'syntax': 'invalid syntax!'}[failure]
    request['sha256'] = hashlib.sha256(request['source'].encode()).hexdigest()
    monkeypatch.setattr(migration, 'READY_TIMEOUT', .1)
    with pytest.raises((ValueError, SyntaxError)):
        migration.migrate(root, request)
    assert old.poll() is None and optimizer.poll() is None
    assert json.loads((root / 'guard.json').read_text()) == before
    until = time.monotonic() + 2
    while time.monotonic() < until:
        status = (Path('/proc')/str(old.pid)/'stat').read_text().rpartition(')')[2].split()[0]
        if status not in ('T', 't'):
            break
        time.sleep(.01)
    assert status not in ('T', 't')


@pytest.mark.parametrize('change', [dict(instance_id=124), dict(expected=1), dict(job_id='b'*32), dict(sha256='bad')])
def test_handover_rejects_wrong_identity_or_deadline(rental_guard, change):
    """An unrelated rental, stale request or damaged source never signals processes."""
    root, request, old, optimizer, _ = rental_guard
    before = (root / 'guard.json').read_bytes()
    request.update(change)
    with pytest.raises(ValueError):
        migration.migrate(root, request)
    assert old.poll() is None and optimizer.poll() is None
    assert (root / 'guard.json').read_bytes() == before


@pytest.mark.parametrize('fail', [False, True])
def test_handover_quarantines_stale_deadline_requests(rental_guard, monkeypatch, fail):
    """Old rejected requests cannot become accepted by the new protocol at startup."""
    root, request, old, optimizer, new_pids = rental_guard
    stale = dict(id='e'*32, job_id=request['job_id'], expected=request['expected'],
                 deadline=request['expected']+7*60)
    migration.publish(root / 'deadline-request.json', stale)
    if fail:
        request['source'] = 'raise SystemExit(1)'
        request['sha256'] = hashlib.sha256(request['source'].encode()).hexdigest()
        with pytest.raises(ValueError):
            migration.migrate(root, request)
        assert json.loads((root / 'deadline-request.json').read_text()) == stale
        assert old.poll() is None
    else:
        state = migration.migrate(root, request)
        new_pids.append(state['guard_pid'])
        assert state['deadline'] == request['expected']
        assert state.get('request_id') != stale['id']
        assert not (root / 'deadline-request.json').exists()
    assert not (root / '.deadline-request-before-upgrade.json').exists()
    assert optimizer.poll() is None
