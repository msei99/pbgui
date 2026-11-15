"""Isolated regressions for advisory lock directory setup and ownership."""

from pathlib import Path
import multiprocessing
import os
import stat
import threading
from unittest.mock import patch

import pytest

from file_lock import advisory_directory_lock, advisory_file_lock


def _directory_lock_competitor(root, sender):
    """Signal a real cross-process attempt without accessing any runtime data."""
    descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        sender.send("attempted")
        with advisory_directory_lock(descriptor):
            sender.send("acquired")
    finally:
        os.close(descriptor)
        sender.close()


@pytest.mark.parametrize("raise_inside", [False, True])
def test_directory_lock_serializes_shared_fd_and_nested_alias(tmp_path, raise_inside):
    """Shared caller FDs serialize threads; nested aliases do not release the outer lock."""
    first = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    alias = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    attempted, acquired = threading.Event(), threading.Event()
    errors = []

    def compete():
        """Reuse the exact caller FD from another thread to exercise OFD isolation."""
        try:
            attempted.set()
            with advisory_directory_lock(first):
                acquired.set()
        except BaseException as exc:
            errors.append(exc)

    worker = threading.Thread(target=compete, daemon=True)
    try:
        try:
            with advisory_directory_lock(first):
                with advisory_directory_lock(alias):
                    worker.start()
                    assert attempted.wait(3)
                    assert not acquired.wait(0.05)
                assert not acquired.wait(0.05), "inner release unlocked the outer owner"
                if raise_inside:
                    raise ValueError("fixture transaction aborted")
        except ValueError:
            assert raise_inside
        assert acquired.wait(3)
    finally:
        if worker.ident is not None:
            worker.join(5)
        os.close(first)
        os.close(alias)
    assert not worker.is_alive()
    assert not errors
    assert not list(tmp_path.iterdir()), "directory locks must not create lock files"


def test_directory_lock_blocks_an_independent_process(tmp_path):
    """The inode lock spans processes and is released with its owner transaction."""
    descriptor = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    context = multiprocessing.get_context("spawn")
    receiver, sender = context.Pipe(duplex=False)
    process = context.Process(target=_directory_lock_competitor, args=(tmp_path, sender))
    try:
        with advisory_directory_lock(descriptor):
            process.start()
            sender.close()
            assert receiver.poll(10)
            assert receiver.recv() == "attempted"
            assert not receiver.poll(0.1)
        assert receiver.poll(5)
        assert receiver.recv() == "acquired"
        process.join(5)
        assert process.exitcode == 0
    finally:
        if process.is_alive():
            process.terminate()
            process.join(5)
        process.close()
        receiver.close()
        sender.close()
        os.close(descriptor)


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


@pytest.mark.parametrize("cancel", [False, True])
def test_directory_lock_wait_can_timeout_or_stop(tmp_path, cancel):
    """An independently opened description cannot trap an owned API worker indefinitely."""
    root = tmp_path / "locked"
    root.mkdir()
    descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    requested = threading.Event()
    results = []

    def contender():
        """Wait on the actual flock held by the main thread, with bounded ownership."""
        try:
            with advisory_directory_lock(descriptor, timeout=0.1, cancel_requested=requested.is_set):
                results.append("unexpected acquisition")
        except (InterruptedError, TimeoutError) as exc:
            results.append(type(exc))

    try:
        with advisory_directory_lock(descriptor):
            worker = threading.Thread(target=contender)
            worker.start()
            if cancel:
                requested.set()
            worker.join(2)
            assert not worker.is_alive()
        assert results == [InterruptedError if cancel else TimeoutError]
        with advisory_directory_lock(descriptor, timeout=0.1):
            pass
    finally:
        os.close(descriptor)


def _file_lock_competitor(target, sender):
    """Attempt nonblocking admission from a fresh process with no inherited owner."""
    try:
        with advisory_file_lock(Path(target), blocking=False):
            sender.send('acquired')
    except BlockingIOError:
        sender.send('busy')
    finally:
        sender.close()


def test_nonblocking_file_lock_preserves_nested_owner_and_rejects_other_threads(tmp_path):
    """A refused optional admission never unlocks or weakens the blocking owner."""
    target = tmp_path / 'transaction'
    outcomes = []
    def compete():
        """Try the same lock on another thread, without waiting for the owner."""
        try:
            with advisory_file_lock(target, blocking=False):
                outcomes.append('acquired')
        except BlockingIOError:
            outcomes.append('busy')
    with advisory_file_lock(target):
        with advisory_file_lock(target, blocking=False):
            worker = threading.Thread(target=compete); worker.start(); worker.join(2)
            assert not worker.is_alive()
        worker = threading.Thread(target=compete); worker.start(); worker.join(2)
        assert not worker.is_alive()
    assert outcomes == ['busy', 'busy']
    with advisory_file_lock(target, blocking=False):
        pass


def test_nonblocking_file_lock_rejects_other_process_without_releasing_owner(tmp_path):
    """LOCK_NB is cross-process and a failed flock leaves the existing transaction held."""
    context = multiprocessing.get_context('spawn')
    target = tmp_path / 'transaction'
    with advisory_file_lock(target):
        for _ in range(2):
            receiver, sender = context.Pipe(duplex=False)
            process = context.Process(target=_file_lock_competitor, args=(target, sender))
            process.start(); sender.close()
            try:
                assert receiver.poll(5)
                assert receiver.recv() == 'busy'
            finally:
                process.join(5); receiver.close()
                if process.is_alive():
                    process.kill(); process.join()
            assert process.exitcode == 0
    with advisory_file_lock(target, blocking=False):
        pass
