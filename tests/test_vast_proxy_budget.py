"""Offline remote-budget enforcement and exact Loop ownership regressions."""

import json
from pathlib import Path
from types import SimpleNamespace
import threading

import pytest

from setup.vast_gpu_benchmark import proxy_budget_worker as worker
from vast_provider import VastError
from vast_transfer import WorkerConnection


@pytest.mark.parametrize('limit', [None, True, 0, -1, 1.5, '100000'])
def test_invalid_budget_cannot_launch(tmp_path, limit):
    """Only the positive integer controller limit may enable the wrapper."""
    with pytest.raises(ValueError, match='proxy limit'):
        worker.run(tmp_path, limit)
    assert not (tmp_path / 'proxy-budget-state.json').exists()


def setup_job(root):
    """Create isolated running-job and rental metadata."""
    (root / 'output').mkdir()
    (root / 'started.json').write_text('{}')
    deadline = root / 'guard.json'
    deadline.write_text(json.dumps({'deadline': worker.time.time() + 3600}))
    return deadline


def test_network_independent_first_crossing_stops_existing_wrapper(tmp_path):
    """No SSH/provider/client exists while the local first completed batch stops."""
    deadline = setup_job(tmp_path)
    log = tmp_path / 'output/optimizer.log'
    log.write_text('GPU optimizer progress | gen=13 phase=generation_complete | '
                   'evolution_proxy_completed_run=106496 evolution_exact=768/10000000\n')
    worker.run(tmp_path, 100000, attach=True, deadline_path=deadline)
    state = json.loads((tmp_path / 'proxy-budget-state.json').read_text())
    assert state['observed'] == 106496
    assert state['limit'] == 100000
    assert state['reason'] == 'proxy_limit_reached'
    assert (tmp_path / 'stop').exists()
    assert (tmp_path / 'stop').stat().st_mode & 0o777 == 0o600
    assert (tmp_path / 'proxy-budget-state.json').stat().st_mode & 0o777 == 0o600


def test_below_limit_waits_for_next_local_batch(tmp_path):
    """Seed/exact/dispatch counts cannot consume the proxy budget."""
    deadline = setup_job(tmp_path)
    log = tmp_path / 'output/optimizer.log'
    log.write_text('GPU optimizer progress | gen=12 phase=generation_complete | '
                   'evolution_proxy_completed_run=98304 seed_proxy=999999 evolution_exact=999999\n'
                   'GPU proxy dispatch progress | candidates=999999/999999\n')

    class Advance:
        """Advance the remote log without wall-clock sleeps or network access."""
        def is_set(self):
            """Keep watching until the budget is reached."""
            return False

        def wait(self, seconds):
            """Publish the next completed generation after checking no early stop."""
            assert not (tmp_path / 'stop').exists()
            with log.open('a') as stream:
                stream.write('GPU optimizer progress | gen=13 phase=generation_complete | '
                             'evolution_proxy_completed_run=106496 evolution_exact=768\n')

    worker.watch(tmp_path, 100000, Advance(), deadline_path=deadline)
    assert json.loads((tmp_path / 'proxy-budget-state.json').read_text())['observed'] == 106496


def test_wrapper_joins_monitor_and_preserves_native_shutdown(tmp_path, monkeypatch):
    """The guard owns its monitor thread and the original wrapper owns shutdown."""
    deadline = setup_job(tmp_path)
    monkeypatch.setenv('PBGUI_WORKDIR', str(tmp_path))
    (tmp_path / 'output/optimizer.log').write_text('GPU optimize | gen=13 proxy=106496 (42/s) exact=10\n')
    native = tmp_path / 'native.py'
    native.write_text('''import json,time
from pathlib import Path
import os
root=Path(os.environ['PBGUI_WORKDIR'])
def write_record(path,value):
    path.write_text(json.dumps(value))
def run():
    until=time.monotonic()+3
    while not (root/'stop').exists() and time.monotonic()<until:
        time.sleep(.01)
    assert (root/'stop').exists()
    write_record(root/'finished.json', {'cancelled':True})
''')
    before = set(threading.enumerate())
    worker.run(tmp_path, 100000, worker_path=native, deadline_path=deadline)
    assert set(threading.enumerate()) == before
    assert json.loads((tmp_path / 'finished.json').read_text())['cancelled']


def test_watchdog_failure_requests_stop_instead_of_running_unbounded(tmp_path):
    """Bad local guard state fails closed without changing optimizer inputs."""
    deadline = setup_job(tmp_path)
    deadline.write_text('invalid')
    worker.run(tmp_path, 100000, attach=True, deadline_path=deadline)
    assert (tmp_path / 'stop').exists()
    assert json.loads((tmp_path / 'proxy-budget-state.json').read_text())['reason'] == 'watchdog_error'


def test_finished_job_never_restarts(tmp_path):
    """A duplicate launch cannot reset a completed physical attempt."""
    (tmp_path / 'finished.json').write_text('{}')
    worker.run(tmp_path, 100000, worker_path=tmp_path / 'missing.py')
    assert not (tmp_path / 'proxy-budget-state.json').exists()


def test_symlink_log_fails_closed(tmp_path):
    """The guard never follows a log symlink outside its job."""
    deadline = setup_job(tmp_path)
    (tmp_path / 'output/optimizer.log').symlink_to(tmp_path / 'guard.json')
    worker.run(tmp_path, 100000, attach=True, deadline_path=deadline)
    assert (tmp_path / 'stop').exists()
    assert json.loads((tmp_path / 'proxy-budget-state.json').read_text())['reason'] == 'watchdog_error'


@pytest.fixture
def connection(tmp_path, monkeypatch):
    """Bind a fake owned cloud job to a fake Loop, without runtime reads."""
    import pb8_loop_store
    state = dict(loop_id='a' * 32, loop_owner='b' * 32, loop_operation='operation')
    record = dict(settings={'run_limit_mode': 'proxy', 'run_proxy': 100000}, jobs=[
        dict(operation='operation', kind='optimizer', backend={'id': 'c' * 32, 'execution': 'vast'},
             research={'budget': {'mode': 'proxy', 'value': 100000}})])
    monkeypatch.setattr(pb8_loop_store, 'LoopStore', lambda root: SimpleNamespace(read=lambda *a, **kw: record))
    conn = object.__new__(WorkerConnection)
    conn.identifier = 'c' * 32
    conn.store = SimpleNamespace(root=tmp_path / 'vast', read=lambda *a: state)
    return conn, state, record


def test_budget_is_user_owned_not_ai_or_worker_config(connection):
    """The exact bound operation uses the fixed Loop setting."""
    conn, _, _ = connection
    assert conn.proxy_budget_limit() == 100000


@pytest.mark.parametrize('change', ['backend', 'operation', 'research_budget', 'invalid_limit'])
def test_wrong_binding_or_changed_budget_refused(connection, change):
    """No job may borrow another operation's budget or an AI fraction."""
    conn, _, record = connection
    if change == 'backend':
        record['jobs'][0]['backend']['id'] = 'd' * 32
    elif change == 'operation':
        record['jobs'][0]['operation'] = 'other'
    elif change == 'research_budget':
        record['jobs'][0]['research']['budget']['value'] = 30000
    else:
        record['settings']['run_proxy'] = True
    with pytest.raises(VastError):
        conn.proxy_budget_limit()


@pytest.mark.parametrize('kind', ['standalone', 'calibration', 'hours'])
def test_other_job_modes_keep_native_start(connection, kind):
    """Non-proxy work receives no new guard or settings."""
    conn, state, record = connection
    if kind == 'standalone':
        state.pop('loop_id')
    elif kind == 'calibration':
        state['kind'] = 'calibration'
    else:
        record['settings']['run_limit_mode'] = 'hours'
    assert conn.proxy_budget_limit() is None


def test_installer_transfers_only_hashed_private_guard(tmp_path, monkeypatch):
    """Execute the transport's installer locally against an isolated remote root."""
    import shlex
    import subprocess
    import sys
    import hashlib
    conn = object.__new__(WorkerConnection)
    conn.remote_root = str(tmp_path / 'remote')
    conn.identifier = 'c' * 32
    updates = []
    conn.store = SimpleNamespace(update=lambda *a, **kw: updates.append(kw))
    conn.proxy_budget_limit = lambda: 100000

    def command(value, *, stdin, **kwargs):
        """Run the fixed Python installer without SSH or production data."""
        args = shlex.split(value)
        args[0] = sys.executable
        return subprocess.run(args, input=stdin.read(), capture_output=True, check=True).stdout

    conn.command = command
    launch = conn.proxy_budget_command()
    guard = updates[0]['proxy_budget_guard']
    target = Path(conn.remote_root) / ('proxy-budget-' + guard['sha256'] + '.py')
    assert hashlib.sha256(target.read_bytes()).hexdigest() == guard['sha256']
    assert target.stat().st_mode & 0o777 == 0o600
    assert str(target) in launch and launch.endswith(' 100000')
    assert list(target.parent.iterdir()) == [target]


def test_duplicate_guard_does_not_replace_live_job_state(tmp_path):
    """A second launch cannot overwrite the owner or change the active quota."""
    import fcntl
    lock = (tmp_path / 'proxy-budget.lock').open('a')
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        worker.run(tmp_path, 100000, attach=True)
        assert not (tmp_path / 'proxy-budget-state.json').exists()
    finally:
        lock.close()
