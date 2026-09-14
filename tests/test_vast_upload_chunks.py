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
