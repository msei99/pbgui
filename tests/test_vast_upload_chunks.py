"""Offline resumable SSH uploads using only temporary local files."""
import subprocess
from types import SimpleNamespace
import pytest
from vast_transfer import WorkerConnection
from vast_provider import VastError


def test_chunk_upload_resumes_and_repairs_corrupt_chunks(tmp_path):
    """A dropped connection retains verified chunks and publishes only complete input."""
    remote = tmp_path / 'remote'
    archive = tmp_path / 'archive'
    archive.write_bytes(b'aaaabbbbccccdddd')
    reports, sends = [], []
    connection = object.__new__(WorkerConnection)
    connection.remote_root = str(remote)
    connection.identifier = 'a'*32
    connection.store = SimpleNamespace(update=lambda identifier, **values: reports.append(values))
    fail = [True]
    def command(cmd, *, stdin=None, stdout=None, progress=None, **kwargs):
        """Execute the fixed transport scripts inside the test's private directory."""
        if stdin is not None:
            sends.append(stdin.read()); stdin.seek(0)
            if fail[0] and sends[-1] == b'cccc':
                raise VastError('Simulated SSH disconnect')
        result = subprocess.run(['bash', '-c', cmd], stdin=stdin, capture_output=True, timeout=5, check=True)
        if progress:
            progress()
        return result.stdout
    connection.command = command
    with pytest.raises(VastError, match='Simulated'):
        connection.upload_archive(archive, timeout=30, chunk_bytes=4)
    assert not (remote / 'input.tar.gz').exists()
    # Corruption is detected even when the stored piece has the expected length.
    next((remote / 'upload-chunks').iterdir()).write_bytes(b'xxxx')
    fail[0] = False
    sends.clear()
    connection.upload_archive(archive, timeout=30, chunk_bytes=4)
    assert len(sends) == 3
    assert (remote / 'input.tar.gz').read_bytes() == archive.read_bytes()
    assert reports[-1]['upload_progress']['stage'] == 'verifying'
    sends.clear()
    connection.upload_archive(archive, timeout=30, chunk_bytes=4)
    assert sends == []


def test_upload_archive_is_stable_across_retries(tmp_path, monkeypatch):
    """Repeated packaging keeps identical bytes despite a changed wall clock."""
    import vast_transfer
    root = tmp_path / 'input'
    root.mkdir()
    (root / 'manifest.json').write_text('{"files": []}')
    (root / 'optimize.json').write_text('{}')
    connection = object.__new__(WorkerConnection)
    connection.directory = tmp_path
    connection.remote_root = '/unused'
    connection.identifier = 'a'*32
    connection.store = SimpleNamespace(update=lambda *a, **kw: None)
    connection.command = lambda *a, **kw: b''
    connection.operation = lambda *a, **kw: {'missing': []}
    captured = []
    connection.upload_archive = lambda path, **kw: captured.append(path.read_bytes())
    monkeypatch.setattr(vast_transfer, 'execution_input', lambda *a: root)
    connection.upload(timeout=30)
    monkeypatch.setattr(vast_transfer.gzip.time, 'time', lambda: 9999999999)
    connection.upload(timeout=30)
    assert captured[0] == captured[1]


def test_receiver_progress_is_separate_from_verified_bytes(tmp_path):
    """Remote acknowledgements never count an unverified block as committed."""
    archive = tmp_path / 'archive'
    archive.write_bytes(b'a'*100000)
    connection = object.__new__(WorkerConnection)
    connection.remote_root = str(tmp_path / 'remote')
    connection.identifier = 'a'*32
    reports = []
    connection.store = SimpleNamespace(update=lambda *a, **kw: reports.append(kw['upload_progress']))
    def command(cmd, *, stdin=None, stdout=None, **kwargs):
        """Run the real receiver and verifier against isolated local files."""
        result = subprocess.run(['bash', '-c', cmd], stdin=stdin, capture_output=True, check=True)
        if hasattr(stdout, 'write'):
            stdout.write(result.stdout)
        return result.stdout
    connection.command = command
    connection.upload_archive(archive, timeout=30)
    assert any(row['bytes'] == 0 and row['in_flight_bytes'] > 0 for row in reports)
    assert reports[-1]['bytes'] == archive.stat().st_size
    assert reports[-1]['in_flight_bytes'] == 0
    assert reports[-1]['bytes_per_second'] > 0


@pytest.mark.parametrize('active', [False, True])
def test_ssh_receiver_activity_controls_idle_timeout(tmp_path, monkeypatch, active):
    """A silent receiver times out; periodic acknowledgements keep it alive."""
    import sys
    import vast_transfer
    connection = object.__new__(WorkerConnection)
    connection.lease_id = 'a'*32
    connection.identity_directory = tmp_path
    connection.port = 22
    connection.host = '127.0.0.1'
    real_popen = subprocess.Popen
    children = []
    def spawn(args, **kwargs):
        """Replace SSH with a deterministic local producer, without networking."""
        code = ('import time\nfor i in range(8):\n print(i,flush=True); time.sleep(.1)' if active
                else 'import time; time.sleep(3)')
        child = real_popen([sys.executable, '-c', code], **kwargs)
        children.append(child)
        return child
    monkeypatch.setattr(vast_transfer.subprocess, 'Popen', spawn)
    if active:
        assert connection.command('unused', timeout=3, idle_timeout=.5)
    else:
        with pytest.raises(VastError, match='stalled'):
            connection.command('unused', timeout=3, idle_timeout=.5)
    assert all(child.poll() is not None for child in children)


def test_ssh_hard_deadline_and_sanitized_error(tmp_path, monkeypatch):
    """Receiver activity cannot override the hard deadline or expose raw diagnostics."""
    import sys
    import vast_transfer
    connection = object.__new__(WorkerConnection)
    connection.lease_id = 'a'*32
    connection.identity_directory = tmp_path
    connection.port = 22
    connection.host = '127.0.0.1'
    real_popen = subprocess.Popen
    script = ['import time\nfor i in range(50):\n print(i,flush=True); time.sleep(.1)']
    def spawn(args, **kwargs):
        """Execute only local synthetic processes."""
        return real_popen([sys.executable, '-c', script[0]], **kwargs)
    monkeypatch.setattr(vast_transfer.subprocess, 'Popen', spawn)
    with pytest.raises(VastError, match='timed out'):
        connection.command('unused', timeout=1, idle_timeout=.5)
    script[0] = 'import sys,time; sys.stderr.write("Connection reset secret-test-value\\n"); sys.stderr.flush(); time.sleep(.1); sys.exit(1)'
    with pytest.raises(VastError, match='connection reset by peer') as error:
        connection.command('unused', timeout=3)
    assert 'secret-test-value' not in str(error.value)
