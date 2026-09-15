"""Exercise real rsync resume and verification without network or runtime data."""

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


@pytest.fixture
def transfer(tmp_path, monkeypatch):
    """Replace only the SSH boundary with a local rsync receiver."""
    if not shutil.which('rsync'):
        pytest.skip('rsync executable required for isolated transport integration')
    remote = tmp_path / 'remote'
    reports = []
    controls = {'stop': False}
    source = tmp_path / 'archive.tar.gz'
    source.write_bytes(os.urandom(2 * 1024**2))
    fake_ssh = tmp_path / 'ssh.py'
    fake_ssh.write_text("import os,sys\nargs=sys.argv[sys.argv.index('rsync'):]\nos.execvp(args[0],args)\n")
    connection = SimpleNamespace(identifier='a'*32, lease_id='b'*32,
        remote_root='/work/pbgui/jobs/' + 'a'*32, directory=tmp_path,
        identity_directory=tmp_path, host='8.8.8.8', port=2222)

    def command(value, **kwargs):
        """Run fixed verifier scripts within the isolated destination."""
        value = value.replace(connection.remote_root, str(remote))
        result = subprocess.run(shlex.split(value), capture_output=True)
        if result.returncode:
            raise VastError('Remote verification failed', 422)
        return result.stdout

    def update(identifier, **values):
        """Record durable progress without touching production storage."""
        reports.append(values['upload_progress'])

    connection.command = command
    connection.store = SimpleNamespace(read=lambda *args: controls, update=update)
    monkeypatch.setattr(vast_rsync, 'ssh_arguments', lambda connection: [sys.executable, str(fake_ssh)])
    real_attempt = vast_rsync._attempt
    throttle = [False]

    def attempt(connection, args, *rest):
        """Route the real transfer to a local server and optionally slow it for cancellation."""
        args = list(args)
        args[-1] = args[-1].replace(connection.remote_root, str(remote))
        if throttle[0]:
            args.insert(1, '--bwlimit=256')
        return real_attempt(connection, args, *rest)

    monkeypatch.setattr(vast_rsync, '_attempt', attempt)
    return connection, source, remote, reports, controls, throttle


def test_rsync_resumes_cancelled_transfer(transfer):
    """Cancellation reaps the sender; the next call reuses its retained partial file."""
    connection, source, remote, reports, controls, throttle = transfer
    original_update = connection.store.update

    def update(identifier, **values):
        """Cancel after actual byte progress, before the full archive arrives."""
        original_update(identifier, **values)
        if throttle[0] and values['upload_progress']['transferred_bytes'] >= 128 * 1024:
            controls['stop'] = True

    connection.store.update = update
    throttle[0] = True
    with pytest.raises(VastError, match='cancelled'):
        vast_rsync.upload(connection, source, timeout=20)
    partial = next(remote.glob('rsync-*.tar.gz'))
    offset = partial.stat().st_size
    assert 0 < offset < source.stat().st_size
    assert not (remote / 'input.tar.gz').exists()
    reports.clear()
    throttle[0] = False
    controls['stop'] = False
    vast_rsync.upload(connection, source, timeout=20)
    assert reports[0]['transferred_bytes'] == offset
    assert (remote / 'input.tar.gz').read_bytes() == source.read_bytes()
    assert reports[-1]['bytes'] == source.stat().st_size
    assert reports[-1]['stage'] == 'verified'
    assert all(row['bytes'] == 0 for row in reports[:-1])


def test_rsync_repairs_corrupt_partial_prefix(transfer):
    """Append verification detects an incorrect retained prefix before final publication."""
    connection, source, remote, reports, controls, throttle = transfer
    remote.mkdir()
    (remote / ('rsync-' + vast_rsync.digest(source) + '.tar.gz')).write_bytes(b'bad-prefix'*4096)
    vast_rsync.upload(connection, source, timeout=20)
    assert (remote / 'input.tar.gz').read_bytes() == source.read_bytes()


def test_rsync_never_installs_failed_verification(transfer, monkeypatch):
    """A lying or failed sender cannot publish corrupt input."""
    connection, source, remote, reports, controls, throttle = transfer

    def corrupt(connection, args, *rest):
        """Simulate a successful exit with incorrect remote bytes."""
        (remote / ('rsync-' + vast_rsync.digest(source) + '.tar.gz')).write_bytes(b'x'*source.stat().st_size)
        return 0, ''

    monkeypatch.setattr(vast_rsync, '_attempt', corrupt)
    with pytest.raises(VastError, match='verification'):
        vast_rsync.upload(connection, source, timeout=20)
    assert not (remote / 'input.tar.gz').exists()
    assert all(row['bytes'] == 0 for row in reports)


def test_missing_remote_rsync_uses_compatibility_transport(monkeypatch):
    """Only a missing capability selects fallback; connection failures remain errors."""
    monkeypatch.setattr(vast_rsync.shutil, 'which', lambda name: '/usr/bin/rsync')
    connection = SimpleNamespace(command=lambda *args, **kwargs: b'')
    assert not vast_rsync.available(connection, 20)

    def failed(*args, **kwargs):
        """Simulate an authentication failure during the capability probe."""
        raise VastError('authentication failed', 422)

    connection.command = failed
    with pytest.raises(VastError, match='authentication'):
        vast_rsync.available(connection, 20)


def test_rsync_repairs_full_sized_corrupt_partial(transfer):
    """A full-sized invalid file must not be skipped forever by append mode."""
    connection, source, remote, reports, controls, throttle = transfer
    remote.mkdir()
    (remote / ('rsync-' + vast_rsync.digest(source) + '.tar.gz')).write_bytes(b'x'*source.stat().st_size)
    vast_rsync.upload(connection, source, timeout=20)
    assert reports[0]['transferred_bytes'] == 0
    assert (remote / 'input.tar.gz').read_bytes() == source.read_bytes()


def test_rsync_retries_transient_disconnect(transfer, monkeypatch):
    """A disconnected attempt resumes its partial file without transport fallback."""
    connection, source, remote, reports, controls, throttle = transfer
    real_attempt = vast_rsync._attempt
    calls = []

    def attempt(connection, args, *rest):
        """Leave a valid prefix on the first simulated broken connection."""
        calls.append(True)
        if len(calls) == 1:
            (remote / ('rsync-' + vast_rsync.digest(source) + '.tar.gz')).write_bytes(source.read_bytes()[:65536])
            return 12, 'connection unexpectedly closed'
        return real_attempt(connection, args, *rest)

    monkeypatch.setattr(vast_rsync, '_attempt', attempt)
    vast_rsync.upload(connection, source, timeout=20)
    assert len(calls) == 2
    assert any(row['stage'] == 'reconnecting' for row in reports)
    assert any(row['transferred_bytes'] == 65536 for row in reports)
    assert (remote / 'input.tar.gz').read_bytes() == source.read_bytes()
