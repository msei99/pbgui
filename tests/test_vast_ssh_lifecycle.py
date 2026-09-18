"""Exercise SSH cancellation and pipe lifetimes using only local subprocesses."""

import subprocess
import sys
import time
from threading import Event

import pytest

from vast_provider import VastError
from vast_transfer import WorkerConnection


@pytest.fixture
def local_command(tmp_path, monkeypatch):
    """Replace the SSH executable with an owned local Python child."""
    connection = object.__new__(WorkerConnection)
    connection.lease_id = 'a' * 32
    connection.identity_directory = tmp_path
    connection.host = '8.8.8.8'
    connection.port = 22
    real_popen = subprocess.Popen
    children = []

    def spawn(args, **kwargs):
        """Interpret only this fixture's fixed Python snippets locally."""
        child = real_popen([sys.executable, '-c', args[-1].split(' && ', 1)[1]], **kwargs)
        children.append(child)
        return child

    monkeypatch.setattr('vast_transfer.subprocess.Popen', spawn)
    yield connection, children
    for child in children:
        if child.poll() is None:
            child.kill()
        child.wait()


@pytest.mark.parametrize('closed', ['os.close(1)', 'os.close(1); os.close(2)'])
@pytest.mark.parametrize('cancel_kind', ['progress', 'event'])
def test_stop_remains_responsive_after_output_eof(local_command, closed, cancel_kind):
    """Closing output must never enter an uninterruptible process.wait()."""
    connection, children = local_command
    cancellation = Event()
    callbacks = []

    def progress():
        """Simulate a persisted stop arriving during the pending SSH operation."""
        callbacks.append(time.monotonic())
        if len(callbacks) >= 2:
            if cancel_kind == 'progress':
                raise VastError('Requested stop', 422)
            cancellation.set()

    started = time.monotonic()
    with pytest.raises(VastError) as error:
        connection.command('import os,time; ' + closed + '; time.sleep(15)',
                           timeout=5, progress=progress, cancel_event=cancellation)
    assert error.value.status == 422
    assert time.monotonic() - started < 4
    assert children[0].poll() is not None
    assert children[0].stdout.closed and children[0].stderr.closed


def test_stderr_is_drained_after_stdout_eof(local_command):
    """Diagnostics larger than a pipe buffer must not deadlock process exit."""
    connection, children = local_command
    result = connection.command(
        'import os; os.write(1,b"result"); os.close(1); os.write(2,b"x" * 1048576)',
        timeout=5)
    assert result == b'result'
    assert children[0].returncode == 0


def test_output_eof_does_not_hide_failed_exit(local_command):
    """Preserve sanitized errors after draining both pipes."""
    connection, _ = local_command
    with pytest.raises(VastError, match='remote disk full'):
        connection.command('import os; os.close(1); os.write(2,b"no space left"); os._exit(1)', timeout=5)


def test_rsync_stop_remains_responsive_after_output_eof():
    """The rsync process wait also continues polling persisted controls."""
    from types import SimpleNamespace
    from vast_rsync import _attempt

    reports = []
    connection = SimpleNamespace(identifier='a' * 32, lease_id='b' * 32,
        store=SimpleNamespace(read=lambda *args: {'stop': len(reports) >= 1}))
    started = time.monotonic()
    with pytest.raises(VastError, match='cancelled'):
        _attempt(connection, [sys.executable, '-c',
            'import os,time; os.close(1); os.close(2); time.sleep(15)'],
            started + 5, 100, 0, lambda *args: reports.append(args))
    assert time.monotonic() - started < 3
