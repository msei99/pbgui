"""Regression coverage for verified Vast result-snapshot accounting."""

import hashlib
import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import io
import json
from pathlib import Path
import tarfile
import threading
from types import SimpleNamespace

import pytest

import vast_transfer
from vast_provider import VastError


@pytest.mark.parametrize('outcome,payload', [
    ('interrupted', b'go'),
    ('checksum', b'bad!'),
    ('verified', b'good'),
])
def test_collect_counts_only_verified_downloads(tmp_path, monkeypatch, outcome, payload):
    """Failed and corrupt snapshots preserve the prior result and quota."""
    identifier = 'a' * 32
    state = {'downloaded_bytes': 5}

    def update(_identifier, **changes):
        """Track snapshot quota and completion updates in memory."""
        state.update(changes)

    connection = object.__new__(vast_transfer.WorkerConnection)
    connection.identifier = identifier
    connection.directory = tmp_path
    connection.remote_root = '/work/pbgui/jobs/' + identifier
    connection.store = SimpleNamespace(read=lambda _identifier: dict(state), update=update)
    connection.operation = lambda _action, **_kwargs: {
        'bytes': 4, 'sha256': hashlib.sha256(b'good').hexdigest()}

    def command(_command, *, stdout, **_kwargs):
        """Simulate one complete, corrupt or interrupted network download."""
        stdout.write(payload)
        if outcome == 'interrupted':
            raise VastError('SSH connection interrupted')

    connection.command = command
    extracted = tmp_path / 'extracted'
    def extract(*_args, check):
        """Verify extraction receives the same remaining collection deadline."""
        assert 0 < check() <= 30
        return extracted
    monkeypatch.setattr(vast_transfer, 'extract_results', extract)
    destination = tmp_path / 'partial.tar.gz'
    destination.write_bytes(b'old')

    if outcome == 'verified':
        assert connection.collect(final=False, timeout=30) == extracted
        assert destination.read_bytes() == b'good'
        assert state['downloaded_bytes'] == 9
        assert state['last_backup_at'] > 0
    else:
        with pytest.raises(VastError):
            connection.collect(final=False, timeout=30)


@pytest.mark.parametrize('available,allowed', [(4, True), (3, False), (0, False)])
def test_manual_snapshot_volume_preserves_final_transfer_reserve(tmp_path, monkeypatch, available, allowed):
    """The entire new archive must fit below the cumulative partial allowance."""
    from vast_snapshot import work_boundary
    state = {'downloaded_bytes': 1024**3 - available}
    connection = object.__new__(vast_transfer.WorkerConnection)
    connection.identifier, connection.directory = 'a'*32, tmp_path
    connection.remote_root = '/work/pbgui/jobs/' + connection.identifier
    connection.store = SimpleNamespace(read=lambda _id: dict(state),
        update=lambda _id, **changes: state.update(changes))
    connection.operation = lambda *_args, **_kwargs: {'bytes': 4,
        'sha256': hashlib.sha256(b'good').hexdigest()}
    downloads = []
    def download(_command, *, stdout, **_kwargs):
        """Download only the synthetic archive payload."""
        downloads.append(True)
        stdout.write(b'good')
    connection.command = download
    monkeypatch.setattr(vast_transfer, 'extract_results', lambda *_args, **_kwargs: tmp_path / 'extracted')
    with work_boundary(lambda: 30):
        if allowed:
            connection.collect(False, timeout=30)
            assert state['downloaded_bytes'] == 1024**3
        else:
            with pytest.raises(VastError, match='allowance'):
                connection.collect(False, timeout=30)
            assert not downloads


@pytest.mark.parametrize('phase', ['quota', 'backup'])
@pytest.mark.parametrize('interruption', ['deadline', 'shutdown'])
def test_manual_transfer_state_wait_obeys_budget_and_shutdown(tmp_path, monkeypatch, phase, interruption):
    """Neither transport metadata write may strand a manual owner on .state-lock."""
    from file_lock import advisory_file_lock
    from vast_jobs import JobStore, write_json
    from vast_snapshot import work_boundary
    import vast_collect
    identifier = 'a' * 32
    directory = tmp_path / 'cloud/jobs' / identifier
    directory.mkdir(parents=True)
    store = JobStore(tmp_path / 'cloud')
    write_json(directory / 'state.json', dict(id=identifier, status='running', downloaded_bytes=0))
    write_json(directory / 'control.json', dict(stop=False, cleanup=False))
    archive = io.BytesIO()
    manifest = json.dumps({'files': []}).encode()
    with tarfile.open(fileobj=archive, mode='w:gz') as writer:
        entry = tarfile.TarInfo('manifest.json')
        entry.size = len(manifest)
        writer.addfile(entry, io.BytesIO(manifest))
    payload = archive.getvalue()
    destination = directory / 'partial.tar.gz'
    destination.write_bytes(b'previous verified archive')
    begin, held, release, attempted, expired = (threading.Event() for _ in range(5))
    def holder():
        """Keep the real legacy state lock until the test explicitly releases it."""
        if begin.wait(3):
            with advisory_file_lock(directory / '.state-lock'):
                held.set()
                release.wait(5)
    def block_state():
        """Install contention at the chosen metadata boundary."""
        begin.set()
        assert held.wait(2)
    original_lock = vast_transfer.bounded_lock
    @contextmanager
    def tracked_lock(target, **kwargs):
        """Observe the production bounded state acquisition, not an implementation mock."""
        if Path(target).name == '.state-lock':
            attempted.set()
        with original_lock(target, **kwargs):
            yield
    monkeypatch.setattr(vast_transfer, 'bounded_lock', tracked_lock)
    connection = object.__new__(vast_transfer.WorkerConnection)
    connection.store, connection.identifier, connection.directory = store, identifier, directory
    connection.remote_root = '/work/pbgui/jobs/' + identifier
    connection.operation = lambda *_args, **_kwargs: dict(bytes=len(payload), sha256=hashlib.sha256(payload).hexdigest())
    def download(_command, *, stdout, **_kwargs):
        """Write only the valid in-memory archive; never start SSH."""
        stdout.write(payload)
        if phase == 'quota':
            block_state()
    connection.command = download
    original_extract = vast_transfer.extract_results
    def extract(*args, **kwargs):
        """Reach the second state write only after real local verification/extraction."""
        result = original_extract(*args, **kwargs)
        if phase == 'backup':
            block_state()
        return result
    monkeypatch.setattr(vast_transfer, 'extract_results', extract)
    def collect():
        """Use the real API owner and a controllably expiring cooperative allowance."""
        with vast_collect.owned_work() as cancel:
            def remaining():
                """Represent the same cancellation and deadline contract as manual Collect."""
                if cancel.is_set():
                    raise VastError('Pareto collection cancelled during API shutdown', 409)
                if expired.is_set():
                    raise VastError('Pareto collection timed out', 409)
                return 30
            with work_boundary(remaining):
                return connection.collect(False, timeout=30)
    thread = threading.Thread(target=holder)
    vast_collect.startup()
    thread.start()
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            future = pool.submit(collect)
            try:
                assert held.wait(2) and attempted.wait(2)
                if interruption == 'deadline':
                    expired.set()
                    shutdown = None
                else:
                    shutdown = pool.submit(asyncio.run, vast_collect.shutdown())
                with pytest.raises(VastError, match='timed out|shutdown') as error:
                    future.result(timeout=2)
                assert error.value.status == 409
                if shutdown is not None:
                    shutdown.result(timeout=2)
                assert thread.is_alive() and not release.is_set()
                assert not vast_collect._ACTIVE
                state = store.read(identifier)
                assert state['status'] == 'running'
                assert state['downloaded_bytes'] == (0 if phase == 'quota' else len(payload))
                assert 'last_backup_at' not in state and not state.get('final_collected')
                assert store.read(identifier, 'control.json') == dict(stop=False, cleanup=False)
                assert destination.read_bytes() == (b'previous verified archive' if phase == 'quota' else payload)
            finally:
                release.set()
    finally:
        release.set()
        thread.join(3)
        vast_collect.startup()
