"""Exercise direct rsync, immutable reuse and worker publication without a host."""

import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
from types import SimpleNamespace

import pytest

import vast_rsync
from vast_provider import VastError
from vast_transfer import WorkerConnection


@pytest.fixture
def sync_transfer(tmp_path, monkeypatch):
    """Run a real local rsync receiver and the shipped worker helper in temp paths."""
    if not shutil.which('rsync'):
        pytest.skip('rsync required')
    remote = tmp_path / 'remote'
    remote.mkdir()
    worker = remote / 'worker.py'
    original = Path('setup/vast_gpu_benchmark/cloud_worker.py').read_text()
    worker.write_text(original.replace('GUARD_ROOT = Path("/work/pbgui")', 'GUARD_ROOT = Path(' + repr(str(remote)) + ')'))
    revision = remote / 'revision'
    revision.write_text('revision')
    source = tmp_path / 'input'
    source.mkdir()
    fake_ssh = tmp_path / 'ssh.py'
    fake_ssh.write_text("import os,sys\na=sys.argv[sys.argv.index('rsync'):]\nos.execvp(a[0],a)\n")
    reports, commands = [], []
    controls = {'stop': False}
    connection = SimpleNamespace(identifier='a'*32, lease_id='b'*32, directory=tmp_path,
        remote_root='/work/pbgui/jobs/' + 'a'*32, host='8.8.8.8', port=22,
        identity_directory=tmp_path)
    connection.store = SimpleNamespace(read=lambda *args: controls,
        update=lambda identifier, **values: reports.append(values))

    def command(value, *, stdin=None, **kwargs):
        """Execute the actual compatibility helper, translating only remote paths."""
        commands.append(value)
        args = shlex.split(value)
        assignment = args.pop(0).replace('/work/pbgui', str(remote))
        args[0] = sys.executable
        args[2] = args[2].replace('/work/pbgui/worker.py', str(worker)).replace('/opt/pb8-revision', str(revision))
        name, val = assignment.split('=', 1)
        result = subprocess.run(args, stdin=stdin, capture_output=True, env={**os.environ, name: val})
        if result.returncode:
            raise VastError('Worker helper failed: ' + result.stderr.decode(), 422)
        return result.stdout

    connection.command = command
    monkeypatch.setattr(vast_rsync, 'ssh_arguments', lambda connection: [sys.executable, str(fake_ssh)])
    real_attempt = vast_rsync._attempt
    throttle = [False]

    def attempt(connection, args, *rest, **kwargs):
        """Run the real rsync process with its destination mapped below tmp_path."""
        args = list(args)
        args[-1] = args[-1].replace('/work/pbgui', str(remote))
        if throttle[0]:
            args.insert(1, '--bwlimit=256')
        return real_attempt(connection, args, *rest, **kwargs)

    monkeypatch.setattr(vast_rsync, '_attempt', attempt)
    return connection, source, remote, reports, controls, throttle, commands


def prepare_input(source, contents, config='{"cpu":1}'):
    """Create an immutable job manifest like PBGui's queue preparation."""
    files = []
    for index, content in enumerate(contents):
        name = 'ohlcv/coin/' + str(index) + '.npy'
        path = source / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        files.append({'path': name, 'bytes': len(content), 'sha256': hashlib.sha256(content).hexdigest()})
    manifest = {'pb8_revision': 'revision', 'config_sha256': hashlib.sha256(config.encode()).hexdigest(), 'files': files}
    (source / 'optimize.json').write_text(config)
    (source / 'manifest.json').write_text(json.dumps(manifest))
    return manifest


def test_direct_sync_reuses_data_across_jobs_and_updates_same_size_changes(sync_transfer):
    """Unchanged content sends zero bytes despite newer timestamps; changed hashes send."""
    connection, source, remote, reports, *_ = sync_transfer
    content = [os.urandom(100000), os.urandom(80000)]
    manifest = prepare_input(source, content)
    vast_rsync.sync_files(connection, source, timeout=20)
    published = remote / 'jobs' / connection.identifier / 'input'
    assert (published / manifest['files'][0]['path']).read_bytes() == content[0]
    assert (published / manifest['files'][0]['path']).stat().st_ino == (remote / 'rsync-data' / manifest['files'][0]['sha256']).stat().st_ino
    assert reports[-1]['sync_statistics']['changed_bytes'] == 180000
    assert not list(connection.directory.glob('upload.tar.gz'))
    connection.identifier = 'c'*32
    connection.remote_root = '/work/pbgui/jobs/' + connection.identifier
    prepare_input(source, content, config='{"cpu":2}')
    vast_rsync.sync_files(connection, source, timeout=20)
    assert reports[-1]['sync_statistics']['changed_bytes'] == 0
    assert reports[-1]['sync_statistics']['reused_bytes'] == 180000
    assert json.loads((remote / 'jobs' / connection.identifier / 'input/optimize.json').read_text()) == {'cpu': 2}
    # Same file length but different content must get a new identity and transfer.
    connection.identifier = 'd'*32
    connection.remote_root = '/work/pbgui/jobs/' + connection.identifier
    new = b'x'*100000
    prepare_input(source, [new, content[1]])
    vast_rsync.sync_files(connection, source, timeout=20)
    assert reports[-1]['sync_statistics']['changed_bytes'] == 100000
    assert (published / manifest['files'][0]['path']).read_bytes() == content[0]


def test_direct_sync_retries_transient_worker_prepare_without_spending_upload_attempt(sync_transfer):
    """A short SSH loss before rsync is retried inside the one upload operation."""
    connection, source, _, reports, *_ = sync_transfer
    prepare_input(source, [b'data'])
    command = connection.command
    attempts = [0]

    def flaky(value, **kwargs):
        """Drop exactly the first remote preparation command."""
        if value.endswith(' prepare') and attempts[0] == 0:
            attempts[0] += 1
            raise VastError('SSH worker operation failed: connection timed out')
        return command(value, **kwargs)

    connection.command = flaky
    vast_rsync.sync_files(connection, source, timeout=20)
    assert attempts == [1]
    assert any(row.get('upload_progress', {}).get('stage') == 'reconnecting' for row in reports)


def test_direct_sync_resumes_without_publishing_partial_files(sync_transfer):
    """A cancelled transfer leaves only partial data; resuming publishes complete input."""
    connection, source, remote, reports, controls, throttle, _ = sync_transfer
    content = os.urandom(2 * 1024**2)
    manifest = prepare_input(source, [content])
    update = connection.store.update

    def cancel(identifier, **values):
        """Cancel after real sender progress has reached the destination."""
        update(identifier, **values)
        if throttle[0] and values.get('upload_progress', {}).get('transferred_bytes', 0) > 128 * 1024:
            controls['stop'] = True

    connection.store.update = cancel
    throttle[0] = True
    with pytest.raises(VastError, match='cancelled'):
        vast_rsync.sync_files(connection, source, timeout=20)
    assert not (remote / 'jobs' / connection.identifier / 'input-ready.json').exists()
    assert not (remote / 'rsync-data' / manifest['files'][0]['sha256']).exists()
    assert list((remote / 'rsync-data/.rsync-partial').iterdir())
    controls['stop'] = False
    throttle[0] = False
    vast_rsync.sync_files(connection, source, timeout=20)
    assert (remote / 'jobs' / connection.identifier / 'input/ohlcv/coin/0.npy').read_bytes() == content
    assert reports[-1]['sync_statistics']['literal_bytes'] < len(content)


def test_direct_sync_skips_legacy_cache_query_and_archive(tmp_path, monkeypatch):
    """The primary upload path must never enter legacy cache paging or packaging."""
    import vast_transfer
    connection = object.__new__(WorkerConnection)
    connection.identifier = 'a'*32
    connection.store = SimpleNamespace()
    monkeypatch.setattr(vast_transfer, 'execution_input', lambda *args: tmp_path)
    monkeypatch.setattr(vast_rsync, 'available', lambda *args: True)
    calls = []
    monkeypatch.setattr(vast_rsync, 'sync_files', lambda *args, **kwargs: calls.append(args))
    connection.upload(30)
    assert calls == [(connection, tmp_path)]


def test_direct_sync_does_not_publish_when_receiver_fails(sync_transfer, monkeypatch):
    """An unsuccessful rsync cannot create a ready marker even after some data arrived."""
    connection, source, remote, *_ = sync_transfer
    prepare_input(source, [b'data'])
    monkeypatch.setattr(vast_rsync, '_attempt', lambda *args, **kwargs: (23, 'disk error'))
    with pytest.raises(VastError, match='synchronization failed'):
        vast_rsync.sync_files(connection, source, timeout=20)
    assert not (remote / 'jobs' / connection.identifier / 'input-ready.json').exists()


def test_direct_sync_recovers_publication_before_ready_marker(sync_transfer):
    """Retry finishes a interrupted publish without copying or hashing data again."""
    connection, source, remote, reports, *_ = sync_transfer
    prepare_input(source, [b'data'])
    vast_rsync.sync_files(connection, source, timeout=20)
    job = remote / 'jobs' / connection.identifier
    (job / 'input-ready.json').unlink()
    original = (job / 'input/ohlcv/coin/0.npy').stat().st_ino
    vast_rsync.sync_files(connection, source, timeout=20)
    assert (job / 'input-ready.json').exists()
    assert (job / 'input/ohlcv/coin/0.npy').stat().st_ino == original
    assert reports[-1]['sync_statistics']['changed_bytes'] == 0


def test_direct_sync_rejects_changed_manifest_for_ready_job(sync_transfer):
    """An existing job identity cannot silently continue with different data."""
    connection, source, remote, *_ = sync_transfer
    prepare_input(source, [b'old'])
    vast_rsync.sync_files(connection, source, timeout=20)
    prepare_input(source, [b'new'])
    with pytest.raises(VastError, match='configuration changed'):
        vast_rsync.sync_files(connection, source, timeout=20)
    assert (remote / 'jobs' / connection.identifier / 'input/ohlcv/coin/0.npy').read_bytes() == b'old'


def test_direct_sync_uses_original_data_with_cpu_adjusted_execution_copy(sync_transfer):
    """Automatic CPU selection keeps data in input and only overrides small metadata."""
    connection, source, remote, *_ = sync_transfer
    manifest = prepare_input(source, [b'original'])
    execution = connection.directory / 'execution-input'
    execution.mkdir()
    config = '{"cpu":21}'
    manifest['config_sha256'] = hashlib.sha256(config.encode()).hexdigest()
    (execution / 'optimize.json').write_text(config)
    (execution / 'manifest.json').write_text(json.dumps(manifest))
    vast_rsync.sync_files(connection, execution, timeout=20)
    published = remote / 'jobs' / connection.identifier / 'input'
    assert (published / 'ohlcv/coin/0.npy').read_bytes() == b'original'
    assert (published / 'optimize.json').read_text() == config


def test_direct_sync_repairs_corrupt_partial_file(sync_transfer):
    """Rsync's delta checks repair an interrupted file with an incorrect prefix."""
    connection, source, remote, *_ = sync_transfer
    content = os.urandom(100000)
    manifest = prepare_input(source, [content])
    partial = remote / 'rsync-data/.rsync-partial'
    partial.mkdir(parents=True)
    (partial / manifest['files'][0]['sha256']).write_bytes(b'incorrect'*2000)
    vast_rsync.sync_files(connection, source, timeout=20)
    assert (remote / 'jobs' / connection.identifier / 'input/ohlcv/coin/0.npy').read_bytes() == content


def test_direct_sync_rejects_redirected_remote_data_directory(sync_transfer):
    """The helper must reject a destination symlink before starting rsync."""
    connection, source, remote, *_ = sync_transfer
    prepare_input(source, [b'data'])
    elsewhere = remote / 'unrelated'
    elsewhere.mkdir()
    (remote / 'rsync-data').symlink_to(elsewhere, target_is_directory=True)
    with pytest.raises(VastError, match='Symlink'):
        vast_rsync.sync_files(connection, source, timeout=20)
    assert not list(elsewhere.iterdir())


@pytest.mark.parametrize('path', ['../escape', '/escape', 'ohlcv/../../escape', 'manifest.json'])
def test_direct_sync_rejects_manifest_traversal(sync_transfer, path):
    """Persisted data paths must not escape the prepared input or replace metadata."""
    connection, source, *_ = sync_transfer
    manifest = prepare_input(source, [b'data'])
    manifest['files'][0]['path'] = path
    (source / 'manifest.json').write_text(json.dumps(manifest))
    with pytest.raises((VastError, ValueError)):
        vast_rsync.sync_files(connection, source, timeout=20)
