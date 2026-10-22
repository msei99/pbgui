"""Bounded snapshot retention scans preserve recovery data and filesystem boundaries."""
import json
import asyncio
import threading
import time
from pathlib import Path

import pytest

from pb8_loop_evidence import EvidenceStore
from secure_files import read_regular_file_prefix_nofollow
from tests.test_pb8_loop_optimizer import OWNER, FakeBackend, record


def snapshot(store, row, number, *, age=0, broken=False):
    """Write only isolated snapshots, with realistic large evidence after the header."""
    identifier = f'{number:032x}'
    value = {'schema': 1, 'id': identifier, 'owner': OWNER, 'loop': row['id'],
             'round': number, 'phase': 'evaluate', 'created_at': time.time() - age,
             'required': {}, 'data': {'payload': 'x' * 100_000}, 'history': {}}
    directory = store.owner_dir(OWNER) / 'snapshots'
    directory.mkdir(exist_ok=True)
    path = directory / (identifier + '.json')
    text = json.dumps(value, indent=4)
    path.write_text(text[:-1] if broken else text)
    return path, value


def test_retention_never_decodes_bodies_of_retained_snapshots(record, monkeypatch):
    """Repeated cleanup reads at most 4 KiB per current snapshot, regardless of body size."""
    import pb8_loop_evidence as module
    store, row = record
    paths = [snapshot(store, row, number)[0] for number in range(8)]
    original = module.read_regular_file_nofollow

    def bounded(path, root):
        """Any full snapshot body read would recreate the progressive slowdown."""
        assert Path(path).parent.name != 'snapshots'
        return original(path, root)

    monkeypatch.setattr(module, 'read_regular_file_nofollow', bounded)
    EvidenceStore(store).cleanup(row)
    assert all(path.exists() for path in paths)


def test_retention_verifies_expired_candidates_and_keeps_corrupt_or_protected_files(record):
    """Only validated expired data may be removed; recovery references always survive."""
    store, row = record
    row = store.update(OWNER, row['id'], lambda value: value.update(status='stopped'))
    expired, _ = snapshot(store, row, 1, age=15 * 86400)
    corrupt, _ = snapshot(store, row, 2, age=15 * 86400, broken=True)
    protected, _ = snapshot(store, row, 3, age=15 * 86400)
    recent, _ = snapshot(store, row, 4)
    EvidenceStore(store).cleanup(row, {protected.stem})
    assert not expired.exists()
    assert corrupt.exists() and protected.exists() and recent.exists()


@pytest.mark.parametrize('boundary', ['file', 'parent', 'outside', 'directory'])
def test_bounded_prefix_reader_rejects_unsafe_sources(tmp_path, boundary):
    """Prefix reads retain the same root, no-follow and regular-file checks as full reads."""
    root = tmp_path / 'root'
    root.mkdir()
    outside = tmp_path / 'outside'
    outside.write_text('private')
    path = root / 'candidate'
    if boundary == 'file':
        path.symlink_to(outside)
    elif boundary == 'parent':
        path.symlink_to(tmp_path, target_is_directory=True)
        path = path / 'outside'
    elif boundary == 'outside':
        path = outside
    else:
        path.mkdir()
    with pytest.raises((OSError, RuntimeError)):
        read_regular_file_prefix_nofollow(path, root)


def test_bounded_prefix_reader_reads_only_requested_bytes(tmp_path, monkeypatch):
    """Limit actual descriptor reads, rather than slicing an already expanded body."""
    import secure_files
    path = tmp_path / 'large'
    path.write_bytes(b'x' * 1_000_000)
    original = secure_files.os.read
    sizes = []

    def measured(descriptor, size):
        """Record the requested byte budget for this isolated file."""
        sizes.append(size)
        return original(descriptor, size)

    monkeypatch.setattr(secure_files.os, 'read', measured)
    assert read_regular_file_prefix_nofollow(path, tmp_path) == b'x' * 4096
    assert sum(sizes) == 4096


@pytest.mark.parametrize('limit', [0, -1, 65537, True, 1.5])
def test_bounded_prefix_reader_rejects_invalid_limits(tmp_path, limit):
    """Callers cannot turn a header probe into an unbounded allocation."""
    with pytest.raises(ValueError):
        read_regular_file_prefix_nofollow(tmp_path / 'unused', tmp_path, limit)


def test_controller_schedules_loop_work_before_startup_retention(record, tmp_path, monkeypatch):
    """Maintenance must yield after scheduling work, including persisted Stop cleanup."""
    from pb8_loop_controller import LoopController
    store, row = record

    async def scenario():
        """Hold maintenance to prove that the controller has already started a tick."""
        loop = asyncio.get_running_loop()
        started, maintenance = asyncio.Event(), asyncio.Event()
        release = threading.Event()
        controller = LoopController(tmp_path, store=store, backend=FakeBackend())

        async def step(_row):
            """Represent ordinary processing without any native or provider operation."""
            started.set()

        def cleanup(*_args):
            """A slow maintenance worker cannot precede admission of normal work."""
            loop.call_soon_threadsafe(maintenance.set)
            assert release.wait(3)

        monkeypatch.setattr(controller, '_tick', step)
        monkeypatch.setattr(EvidenceStore, 'cleanup', cleanup)
        controller.start()
        try:
            await asyncio.wait_for(maintenance.wait(), 3)
            await asyncio.wait_for(started.wait(), 1)
        finally:
            release.set()
            await controller.close()

    asyncio.run(scenario())
