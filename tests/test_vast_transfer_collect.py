"""Regression coverage for verified Vast result-snapshot accounting."""

import hashlib
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
    monkeypatch.setattr(vast_transfer, 'extract_results',
                        lambda *_args: extracted)
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
