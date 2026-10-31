"""Isolated regressions for advisory lock directory setup and ownership."""

from pathlib import Path
import stat
import threading
from unittest.mock import patch

import pytest

from file_lock import advisory_file_lock


def test_existing_parent_skips_mkdir_for_repeated_and_nested_locks(tmp_path):
    """Existing parents require no mkdir, including the reentrant path."""
    target = tmp_path / "state.json"
    with patch.object(Path, "mkdir", autospec=True, side_effect=Path.mkdir) as mkdir:
        for _ in range(3):
            with advisory_file_lock(target):
                with advisory_file_lock(target):
                    pass
        mkdir.assert_not_called()
    assert stat.S_IMODE((tmp_path / "state.json.lock").stat().st_mode) == 0o600


def test_missing_parent_is_created_and_recreated(tmp_path):
    """Do not cache directory existence after an earlier transaction."""
    target = tmp_path / "nested" / "state" / "data.json"
    lock_path = target.with_name(target.name + ".lock")
    for _ in range(2):
        with advisory_file_lock(target):
            assert target.parent.is_dir()
            assert lock_path.is_file()
        lock_path.unlink()
        target.parent.rmdir()
        target.parent.parent.rmdir()


def test_concurrent_missing_parent_creation(tmp_path, monkeypatch):
    """Two threads observing a missing parent can both finish transactions."""
    target = tmp_path / "new" / "nested" / "state.json"
    barrier = threading.Barrier(2, timeout=3)
    original_is_dir = Path.is_dir
    errors = []
    completed = []

    def observe_missing(path):
        """Synchronize the two initial negative existence observations."""
        exists = original_is_dir(path)
        if path == target.parent and not exists:
            barrier.wait()
        return exists

    def transact():
        """Capture worker exceptions so the parent test cannot miss failures."""
        try:
            with advisory_file_lock(target):
                completed.append(threading.get_ident())
        except BaseException as exc:
            errors.append(exc)

    monkeypatch.setattr(Path, "is_dir", observe_missing)
    workers = [threading.Thread(target=transact, daemon=True) for _ in range(2)]
    try:
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(5)
    finally:
        barrier.abort()
        for worker in workers:
            if worker.ident is not None:
                worker.join(5)
    assert not any(worker.is_alive() for worker in workers)
    assert not errors
    assert len(completed) == 2
    assert target.parent.is_dir()


@pytest.mark.parametrize("raise_inside", [False, True])
def test_thread_serialization_and_release(tmp_path, raise_inside):
    """A competing thread enters only after normal or exceptional release."""
    target = tmp_path / "state.json"
    attempted, acquired = threading.Event(), threading.Event()
    errors = []

    def compete():
        """Attempt a real transaction from a different thread."""
        try:
            attempted.set()
            with advisory_file_lock(target):
                acquired.set()
        except BaseException as exc:
            errors.append(exc)

    worker = threading.Thread(target=compete, daemon=True)
    try:
        try:
            with advisory_file_lock(target):
                with advisory_file_lock(target):
                    worker.start()
                    assert attempted.wait(3)
                    assert not acquired.wait(0.05), "competitor entered an owned transaction"
                if raise_inside:
                    raise ValueError("transaction failed")
        except ValueError as exc:
            assert raise_inside and str(exc) == "transaction failed"
        assert acquired.wait(3), "lock was not released"
    finally:
        if worker.ident is not None:
            worker.join(5)
    assert not worker.is_alive()
    assert not errors


def test_parent_file_error_is_propagated(tmp_path):
    """An invalid parent must fail without changing the existing file."""
    parent = tmp_path / "parent"
    parent.write_text("unchanged", encoding="utf-8")
    with pytest.raises((FileExistsError, NotADirectoryError)):
        with advisory_file_lock(parent / "state.json"):
            pytest.fail("file parent accepted")
    assert parent.read_text(encoding="utf-8") == "unchanged"
