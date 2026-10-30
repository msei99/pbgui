"""Offline cross-process locking, legacy imports and non-LIFO ownership contracts."""
from contextlib import ExitStack
import importlib.util
import subprocess
import sys
import threading
import time
import types
from pathlib import Path

import pytest

from file_lock import advisory_file_lock
from vast_snapshot import bounded_lock, work_boundary
from vast_provider import VastError


@pytest.mark.parametrize('first,second', [(advisory_file_lock, bounded_lock), (bounded_lock, advisory_file_lock)])
def test_reentrancy_uses_the_legacy_registry(tmp_path, first, second):
    """Both entry orders share one descriptor, even after releasing an outer key."""
    queue, inner = first(tmp_path / 'queue'), bounded_lock(tmp_path / 'snapshot')
    queue.__enter__()
    inner.__enter__()
    try:
        with second(tmp_path / 'queue'):
            pass
        queue.__exit__(None, None, None)
        with second(tmp_path / 'snapshot'):
            pass
    finally:
        inner.__exit__(None, None, None)


@pytest.mark.parametrize('ancestor', ['data', 'vast'])
@pytest.mark.parametrize('first,second', [(advisory_file_lock, bounded_lock), (bounded_lock, advisory_file_lock)])
def test_symlink_ancestors_share_legacy_identity(tmp_path, ancestor, first, second):
    """Supported data links use the same descriptor through logical and real paths."""
    from file_lock import _HELD_LOCKS
    real = tmp_path / 'volume/vast'
    real.mkdir(parents=True)
    if ancestor == 'data':
        (tmp_path / 'data').symlink_to(real.parent, target_is_directory=True)
    else:
        (tmp_path / 'data').mkdir()
        (tmp_path / 'data/vast').symlink_to(real, target_is_directory=True)
    logical = tmp_path / 'data/vast/queue'
    with first(logical):
        entry = _HELD_LOCKS.entries[str(real / 'queue')]
        with second(real / 'queue'):
            assert _HELD_LOCKS.entries[str(real / 'queue')] is entry
            assert entry['depth'] == 2
    assert str(real / 'queue') not in _HELD_LOCKS.entries
    assert (real / 'queue.lock').is_file()


@pytest.mark.parametrize('change', ['lock_symlink', 'parent_replaced', 'ancestor_retargeted', 'traversal'])
def test_bounded_lock_retains_path_safety(tmp_path, change):
    """Resolving supported ancestors does not permit lock links or replaced parents."""
    parent = tmp_path / 'job'
    parent.mkdir()
    target = parent / '.snapshot-lock'
    def replace():
        """Change the directory only after the initial identity has been captured."""
        if change == 'parent_replaced':
            parent.rename(tmp_path / 'old-job')
            parent.mkdir()
        elif change == 'ancestor_retargeted':
            alias.unlink()
            alias.symlink_to(other, target_is_directory=True)
    if change == 'lock_symlink':
        untouched = tmp_path / 'unrelated'
        untouched.write_text('unchanged')
        target.with_name(target.name + '.lock').symlink_to(untouched)
    elif change == 'ancestor_retargeted':
        other = tmp_path / 'other'
        other.mkdir()
        alias = tmp_path / 'alias'
        alias.symlink_to(parent, target_is_directory=True)
        target = alias / '.snapshot-lock'
    elif change == 'traversal':
        target = parent / '..' / 'job' / '.snapshot-lock'
    with pytest.raises(VastError):
        with bounded_lock(target, check=replace):
            pytest.fail('unsafe lock accepted')
    if change == 'lock_symlink':
        assert untouched.read_text() == 'unchanged'


def test_bounded_thread_wait_and_missing_parent(tmp_path):
    """A waiter times out and never recreates a deleted job directory."""
    entered, release = threading.Event(), threading.Event()
    def owner():
        """Hold an ordinary legacy lock in another thread."""
        with advisory_file_lock(tmp_path / 'job'):
            entered.set()
            release.wait(2)
    thread = threading.Thread(target=owner)
    thread.start()
    assert entered.wait(1)
    try:
        with pytest.raises(VastError, match='busy'):
            with bounded_lock(tmp_path / 'job', timeout=.05):
                pytest.fail('lock not held')
    finally:
        release.set()
        thread.join(2)
    missing = tmp_path / 'removed'
    with pytest.raises(VastError, match='no longer available'):
        with bounded_lock(missing / '.snapshot-lock'):
            pass
    assert not missing.exists()


def test_bounded_process_wait(tmp_path):
    """An independent Python process observes the same flock identity."""
    code = '''from pathlib import Path
from vast_snapshot import bounded_lock
from vast_provider import VastError
import sys
try:
    with bounded_lock(Path(sys.argv[1]), timeout=.1):
        sys.exit(3)
except VastError as exc:
    sys.exit(0 if exc.status == 409 else 4)
'''
    with advisory_file_lock(tmp_path / 'job'):
        child = subprocess.run([sys.executable, '-c', code, str(tmp_path / 'job')],
                               capture_output=True, timeout=5, check=False)
    assert child.returncode == 0, child.stderr.decode()


def test_manual_work_bounds_global_import_lock(tmp_path):
    """A different job's import cannot leave a manual request waiting forever."""
    entered, release = threading.Event(), threading.Event()
    def owner():
        """Simulate another job importing under the shared result-root lock."""
        with advisory_file_lock(tmp_path / '.vast-import'):
            entered.set()
            release.wait(2)
    thread = threading.Thread(target=owner)
    thread.start()
    assert entered.wait(1)
    deadline = time.monotonic() + .05
    def remaining():
        """Use one cooperative budget for all nested locks."""
        value = deadline - time.monotonic()
        if value <= 0:
            raise VastError('isolated collection timeout', 409)
        return value
    try:
        with work_boundary(remaining), pytest.raises(VastError, match='timeout'):
            with bounded_lock(tmp_path / '.vast-import'):
                pass
    finally:
        release.set()
        thread.join(2)


def test_queue_import_does_not_require_new_loaded_module_symbols(monkeypatch):
    """A legacy guard can lazy-import the new queue with its old file_lock API."""
    import file_lock
    import vast_jobs
    import vast_queue
    legacy = types.ModuleType('file_lock')
    # These are the symbols present in the approved base, not a new API.
    for name in ('advisory_file_lock', '_thread_lock', '_HELD_LOCKS'):
        setattr(legacy, name, getattr(file_lock, name))
    monkeypatch.setitem(sys.modules, 'file_lock', legacy)
    spec = importlib.util.spec_from_file_location('isolated_legacy_queue', vast_queue.__file__)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.CloudQueue is not None


def test_provider_reads_shorten_socket_timeout(monkeypatch):
    """Each manual provider read uses remaining time and a short cancellation bound."""
    from types import SimpleNamespace
    from unittest.mock import Mock, MagicMock
    from vast_provider import VastClient
    socket = Mock()
    response = MagicMock()
    response.__enter__.return_value = response
    response.fp = SimpleNamespace(raw=SimpleNamespace(_sock=socket))
    response.isclosed.return_value = False
    response.read1.side_effect = [b'{}', b'']
    opener = Mock()
    opener.open.return_value = response
    monkeypatch.setattr('urllib.request.build_opener', lambda *_args: opener)
    with work_boundary(lambda: .2):
        assert VastClient('isolated')._request('GET', '/users/current/') == {}
    assert opener.open.call_args.kwargs['timeout'] == .2
    assert socket.settimeout.call_count == 2
    assert all(call.args[0] <= .2 for call in socket.settimeout.call_args_list)


def test_stop_finalizer_lock_wait_uses_persisted_budget(tmp_path):
    """A blocked Finalize cannot outlive the Stop caller's existing deadline."""
    from vast_jobs import JobStore, write_json
    from secure_files import ensure_private_directory
    from vast_job_runner import finalize_collected_results
    store = JobStore(tmp_path / 'cloud')
    identifier = 'a' * 32
    directory = ensure_private_directory(store.root / 'jobs' / identifier)
    write_json(directory / 'state.json', {'id': identifier})
    entered, release = threading.Event(), threading.Event()
    def owner():
        """Simulate a different finalizer, without ever loading real results."""
        with advisory_file_lock(directory / '.finalize-lock'):
            entered.set()
            release.wait(2)
    thread = threading.Thread(target=owner)
    thread.start()
    assert entered.wait(1)
    deadline = time.monotonic() + .05
    def remaining():
        """Represent the caller's persisted, nonrenewable Stop allowance."""
        value = deadline - time.monotonic()
        if value <= 0:
            raise VastError('Final collection deadline reached', 409)
        return value
    try:
        with pytest.raises(VastError, match='deadline'):
            finalize_collected_results(store, identifier, check=remaining)
    finally:
        release.set()
        thread.join(2)



def test_manual_provider_content_length_eof(monkeypatch):
    """Real HTTPResponse closes fp at Content-Length EOF; the loop still returns JSON."""
    import socket
    from http.client import HTTPResponse
    from unittest.mock import Mock
    from vast_provider import VastClient
    client, server = socket.socketpair()
    try:
        server.sendall(b'HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}')
        response = HTTPResponse(client)
        response.begin()
        opener = Mock()
        opener.open.return_value = response
        monkeypatch.setattr('urllib.request.build_opener', lambda *_args: opener)
        with work_boundary(lambda: 1):
            assert VastClient('isolated')._request('GET', '/users/current/') == {}
        assert response.isclosed()
    finally:
        client.close()
        server.close()
