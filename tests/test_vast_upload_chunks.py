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
    import hashlib
    expected = {block for block in (b'aaaa', b'bbbb', b'cccc', b'dddd')
                if not (remote / 'upload-chunks' / hashlib.sha256(block).hexdigest()).exists()
                or (remote / 'upload-chunks' / hashlib.sha256(block).hexdigest()).read_bytes() != block}
    sends.clear()
    connection.upload_archive(archive, timeout=30, chunk_bytes=4)
    assert set(sends) == expected
    assert len(sends) == len(expected)
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


@pytest.mark.parametrize('count', [21845, 110000])
def test_large_cache_inventory_is_paged(tmp_path, count):
    """Large multi-coin manifests never require an oversized worker response."""
    import json
    connection = object.__new__(WorkerConnection)
    connection.remote_root = '/unused'
    files = [{'sha256': f'{i:064x}', 'bytes': 10} for i in range(count)]
    pages = []
    def command(cmd, *, stdin, **kwargs):
        """Capture bounded page requests without contacting a remote server."""
        page = json.load(stdin)
        assert len(page['files']) <= 1024
        pages.append(page)
    def operation(action, **kwargs):
        """Model a cold cache with every blob missing."""
        assert action == 'cache-missing'
        response = {'missing': [item['sha256'] for item in pages[-1]['files']]}
        assert len(json.dumps(response)) < 70000
        return response
    connection.command = command
    connection.operation = operation
    assert connection.missing_cache_files(files, lambda: 30) == {item['sha256'] for item in files}
    assert len(pages) == (count + 1023) // 1024


def test_cache_pages_use_existing_worker_and_reject_foreign_hashes(tmp_path, monkeypatch):
    """The deployed worker protocol handles pages and results cannot cross pages."""
    import json
    from setup.vast_gpu_benchmark import cloud_worker
    monkeypatch.setattr(cloud_worker, 'ROOT', tmp_path)
    monkeypatch.setattr(cloud_worker, 'GUARD_ROOT', tmp_path)
    cache = tmp_path / 'cache'; cache.mkdir()
    blob = cache / 'blob'; blob.write_bytes(b'data')
    checksum = cloud_worker.file_hash(blob); blob.rename(cache / checksum)
    files = [{'sha256': checksum, 'bytes': 4}] + [{'sha256': f'{i:064x}', 'bytes': 4} for i in range(1024)]
    connection = object.__new__(WorkerConnection)
    connection.remote_root = '/unused'
    def command(cmd, *, stdin, **kwargs):
        """Persist the same per-page request read by the current image worker."""
        (tmp_path / 'cache-request.json').write_bytes(stdin.read())
    connection.command = command
    connection.operation = lambda *a, **kw: cloud_worker.cache_missing()
    missing = connection.missing_cache_files(files, lambda: 30)
    assert checksum not in missing
    assert len(missing) == 1024
    connection.operation = lambda *a, **kw: {'missing': ['f' * 64]}
    with pytest.raises(VastError, match='Invalid remote cache'):
        connection.missing_cache_files(files, lambda: 30)


def test_parallel_chunks_are_bounded_deduplicated_and_joined(tmp_path):
    """Two transfers overlap, equal chunks share one transfer, and output stays exact."""
    import threading
    archive = tmp_path / 'archive'; archive.write_bytes(b'aaaabbbbccccaaaa')
    connection = object.__new__(WorkerConnection)
    connection.remote_root = str(tmp_path / 'remote')
    connection.identifier = 'a' * 32
    reports = []
    connection.store = SimpleNamespace(update=lambda *a, **kw: reports.append(kw['upload_progress']))
    barrier = threading.Barrier(2)
    lock = threading.Lock()
    active = peak = 0
    sends = []
    def command(cmd, *, stdin=None, stdout=None, **kwargs):
        """Use actual receiver/checksum scripts with synchronized first transfers."""
        nonlocal active, peak
        if stdin is not None:
            data = stdin.read(); stdin.seek(0)
            with lock:
                sends.append(data); active += 1; peak = max(peak, active)
                first_pair = len(sends) <= 2
            if first_pair:
                barrier.wait(timeout=5)
        try:
            result = subprocess.run(['bash', '-c', cmd], stdin=stdin, capture_output=True, timeout=5, check=True)
            if hasattr(stdout, 'write'):
                stdout.write(result.stdout)
            return result.stdout
        finally:
            if stdin is not None:
                with lock:
                    active -= 1
    connection.command = command
    connection.upload_archive(archive, timeout=30, chunk_bytes=4)
    assert peak == 2 and active == 0
    assert sorted(sends) == [b'aaaa', b'bbbb', b'cccc']
    assert (tmp_path / 'remote/input.tar.gz').read_bytes() == archive.read_bytes()
    assert [r['bytes'] for r in reports] == sorted(r['bytes'] for r in reports)
    assert reports[-1]['bytes'] == 16 and reports[-1]['in_flight_bytes'] == 0
    assert not any(t.name.startswith('vast-upload') for t in threading.enumerate())


def test_parallel_failure_cancels_other_owner(tmp_path):
    """A permanent failure cancels the sibling and never publishes partial input."""
    import threading
    archive = tmp_path / 'archive'; archive.write_bytes(b'aaaabbbbcccc')
    connection = object.__new__(WorkerConnection)
    connection.remote_root = '/unused'; connection.identifier = 'a' * 32
    connection.store = SimpleNamespace(update=lambda *a, **kw: None)
    barrier = threading.Barrier(2)
    stopped = threading.Event()
    def command(cmd, *, stdin=None, cancel_event=None, **kwargs):
        """Fail one transfer while the sibling waits for cancellation."""
        if stdin is None:
            return b'[]'
        data = stdin.read()
        barrier.wait(timeout=5)
        if data == b'aaaa':
            raise VastError('permanent test failure', 422)
        assert cancel_event.wait(5)
        stopped.set()
        raise VastError('cancelled', 422)
    connection.command = command
    with pytest.raises(VastError, match='permanent test failure'):
        connection.upload_archive(archive, timeout=30, chunk_bytes=4)
    assert stopped.is_set()
    assert not any(t.name.startswith('vast-upload') for t in threading.enumerate())


def test_cancelled_ssh_command_kills_and_joins_child(tmp_path, monkeypatch):
    """Cancellation releases the real owned subprocess and both output pipes."""
    import sys
    import threading
    import vast_transfer
    connection = object.__new__(WorkerConnection)
    connection.lease_id = 'a' * 32
    connection.identity_directory = tmp_path
    connection.port = 22
    connection.host = '127.0.0.1'
    real_popen = subprocess.Popen
    children = []
    def spawn(args, **kwargs):
        """Substitute a local sleeper for SSH; production paths are never accessed."""
        child = real_popen([sys.executable, '-c', 'import time; time.sleep(30)'], **kwargs)
        children.append(child)
        return child
    monkeypatch.setattr(vast_transfer.subprocess, 'Popen', spawn)
    cancelled = threading.Event(); cancelled.set()
    with pytest.raises(VastError, match='cancelled'):
        connection.command('unused', timeout=10, cancel_event=cancelled)
    assert len(children) == 1
    assert children[0].poll() is not None
    assert children[0].stdout.closed and children[0].stderr.closed


@pytest.mark.parametrize('fail_second_page', [False, True])
def test_cache_progress_counts_only_acknowledged_files(fail_second_page):
    """Pending/invalid pages cannot advance the visible cache-check counter."""
    files = [{'sha256': f'{i:064x}', 'bytes': 4} for i in range(2050)]
    connection = object.__new__(WorkerConnection)
    connection.remote_root = '/unused'
    reports, pages = [], []
    def command(cmd, *, stdin, **kwargs):
        """Record page payloads locally without SSH."""
        import json
        pages.append(json.loads(stdin.read()))
        assert reports[-1] == ((len(pages) - 1) * 1024, len(files))
    def operation(*args, **kwargs):
        """Return valid hashes except the deliberately unconfirmed second page."""
        if fail_second_page and len(pages) == 2:
            return {'missing': ['f' * 64]}
        return {'missing': [item['sha256'] for item in pages[-1]['files']]}
    connection.command, connection.operation = command, operation
    if fail_second_page:
        with pytest.raises(VastError, match='Invalid remote cache'):
            connection.missing_cache_files(files, lambda: 30, progress=lambda *values: reports.append(values))
        assert reports == [(0, 2050), (1024, 2050)]
    else:
        connection.missing_cache_files(files, lambda: 30, progress=lambda *values: reports.append(values))
        assert reports == [(0, 2050), (1024, 2050), (2048, 2050), (2050, 2050)]
