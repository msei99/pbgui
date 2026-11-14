"""Descriptor-bound private creation tests using isolated temporary directories."""

from contextlib import contextmanager
import multiprocessing
import os
from pathlib import Path
import stat

import pytest

import secure_files


pytestmark = pytest.mark.skipif(os.name != "posix", reason="Private descriptor creation requires POSIX")


@contextmanager
def private_directory(root):
    """Create and own one private directory descriptor for a test."""
    root.mkdir(mode=0o700)
    root.chmod(0o700)
    descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        yield descriptor
    finally:
        os.close(descriptor)


def _competing_creator(root, barrier, sender, content):
    """Attempt the same exclusive publication from an independent process."""
    descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        barrier.wait(timeout=10)
        try:
            secure_files.create_private_file_at(descriptor, "example.json", content)
        except FileExistsError:
            sender.send("exists")
        else:
            sender.send("created")
    finally:
        os.close(descriptor)
        sender.close()


def test_private_creation_is_complete_and_keeps_caller_descriptor(tmp_path):
    """A successful creation leaves only the complete owner-only target."""
    root = tmp_path / "private"
    with private_directory(root) as descriptor:
        secure_files.create_private_file_at(descriptor, "example.json", b'{"complete": true}\n')
        assert stat.S_ISDIR(os.fstat(descriptor).st_mode)
    target = root / "example.json"
    assert target.read_bytes() == b'{"complete": true}\n'
    assert stat.S_IMODE(target.stat().st_mode) == 0o600
    assert list(root.iterdir()) == [target]


@pytest.mark.parametrize("name", ["../outside", "/outside", "sub/name", "sub\\name", ".", "..", "bad\x00", "bad\n"])
def test_invalid_names_never_create_a_temporary_file(tmp_path, monkeypatch, name):
    """The shared helper validates basename inputs before opening anything."""
    root = tmp_path / "private"
    with private_directory(root) as descriptor:
        def forbidden(*args, **kwargs):
            """No temporary descriptor may be opened for a path input."""
            pytest.fail("invalid basename opened a temporary file")

        with monkeypatch.context() as patch:
            patch.setattr(secure_files.os, "open", forbidden)
            with pytest.raises(ValueError):
                secure_files.create_private_file_at(descriptor, name, b"{}")
    assert not list(root.iterdir())


def test_nonprivate_directory_is_rejected_without_permission_changes(tmp_path):
    """Private creation does not silently harden or write a shared directory."""
    root = tmp_path / "private"
    with private_directory(root) as descriptor:
        root.chmod(0o755)
        with pytest.raises(ValueError):
            secure_files.create_private_file_at(descriptor, "example.json", b"{}")
    assert stat.S_IMODE(root.stat().st_mode) == 0o755
    assert not list(root.iterdir())


@pytest.mark.parametrize("kind", ["regular", "symlink", "dangling", "fifo", "directory"])
def test_existing_entries_are_never_replaced(tmp_path, kind):
    """Exclusive publication refuses every existing directory entry without following it."""
    root = tmp_path / "private"
    outside = tmp_path / "outside.json"
    outside.write_bytes(b"outside original")
    with private_directory(root) as descriptor:
        target = root / "example.json"
        if kind == "regular":
            target.write_bytes(b"original")
        elif kind == "symlink":
            target.symlink_to(outside)
        elif kind == "dangling":
            target.symlink_to(tmp_path / "missing.json")
        elif kind == "fifo":
            os.mkfifo(target)
        else:
            target.mkdir()
        original_inode = target.lstat().st_ino
        with pytest.raises(FileExistsError):
            secure_files.create_private_file_at(descriptor, target.name, b"replacement")
        assert target.lstat().st_ino == original_inode
        assert list(root.iterdir()) == [target]
        if kind == "regular":
            assert target.read_bytes() == b"original"
    assert outside.read_bytes() == b"outside original"


@pytest.mark.parametrize("failure", ["link", "file_sync"])
def test_prepublication_errors_remove_the_owned_temporary(tmp_path, monkeypatch, failure):
    """A failed write or bind never leaves a target or an owned temporary file."""
    root = tmp_path / "private"
    with private_directory(root) as descriptor:
        def fail(*args, **kwargs):
            """Represent a file-system error before the publication commit point."""
            raise OSError("fixture-only-prepublication-error")

        with monkeypatch.context() as patch:
            patch.setattr(secure_files.os, "link" if failure == "link" else "fsync", fail)
            with pytest.raises(OSError) as caught:
                secure_files.create_private_file_at(descriptor, "example.json", b"complete")
            assert not isinstance(caught.value, secure_files.PrivateFilePublishedError)
        assert stat.S_ISDIR(os.fstat(descriptor).st_mode)
    assert not list(root.iterdir())


def test_directory_sync_failure_reports_a_published_file(tmp_path, monkeypatch):
    """A failure after publication is distinguishable and never rolls the target back."""
    root = tmp_path / "private"
    actual_sync = os.fsync
    with private_directory(root) as descriptor:
        def fail_directory_sync(current):
            """Allow data sync, then reject only the directory acknowledgement."""
            if current == descriptor:
                raise OSError("fixture-only-directory-sync-error")
            return actual_sync(current)

        with monkeypatch.context() as patch:
            patch.setattr(secure_files.os, "fsync", fail_directory_sync)
            with pytest.raises(secure_files.PrivateFilePublishedError):
                secure_files.create_private_file_at(descriptor, "example.json", b"complete")
    assert (root / "example.json").read_bytes() == b"complete"
    assert list(root.iterdir()) == [root / "example.json"]


def test_temporary_cleanup_failure_reports_publication_without_rollback(tmp_path, monkeypatch):
    """A failed unlink preserves the complete target and reports the remaining private link."""
    root = tmp_path / "private"
    actual_unlink = os.unlink
    with private_directory(root) as descriptor:
        def fail_cleanup(*args, **kwargs):
            """Represent a directory failure after the target name was committed."""
            raise OSError("fixture-only-cleanup-error")

        with monkeypatch.context() as patch:
            patch.setattr(secure_files.os, "unlink", fail_cleanup)
            with pytest.raises(secure_files.PrivateFilePublishedError):
                secure_files.create_private_file_at(descriptor, "example.json", b"complete")
        target = root / "example.json"
        assert target.read_bytes() == b"complete"
        temporaries = list(root.glob(".pbgui-create-*.tmp"))
        assert len(temporaries) == 1
        assert temporaries[0].stat().st_ino == target.stat().st_ino
        assert stat.S_IMODE(temporaries[0].stat().st_mode) == 0o600
        # The real filesystem is writable; remove only this test's owned artifact.
        actual_unlink(temporaries[0].name, dir_fd=descriptor)


@pytest.mark.parametrize("zero_write", [False, True])
def test_raw_descriptor_writes_handle_partial_or_zero_progress(tmp_path, monkeypatch, zero_write):
    """Short writes finish the source; zero progress fails before publication."""
    root = tmp_path / "private"
    actual_write = os.write
    content = b"complete bytes after partial writes"
    with private_directory(root) as descriptor:
        def short_write(current, remaining):
            """Limit each real write to three bytes, or report no progress."""
            return 0 if zero_write else actual_write(current, remaining[:3])

        with monkeypatch.context() as patch:
            patch.setattr(secure_files.os, "write", short_write)
            if zero_write:
                with pytest.raises(OSError):
                    secure_files.create_private_file_at(descriptor, "example.json", content)
            else:
                secure_files.create_private_file_at(descriptor, "example.json", content)
    if zero_write:
        assert not list(root.iterdir())
    else:
        assert (root / "example.json").read_bytes() == content
        assert list(root.iterdir()) == [root / "example.json"]


def test_creation_remains_bound_to_the_open_root(tmp_path, monkeypatch):
    """Replacing the root pathname cannot redirect publication or cleanup elsewhere."""
    root = tmp_path / "private"
    moved = tmp_path / "moved"
    outside = tmp_path / "outside"
    outside.mkdir()
    actual_link = os.link
    with private_directory(root) as descriptor:
        def move_root_before_link(*args, **kwargs):
            """Swap the path after the helper has written its private temporary."""
            root.rename(moved)
            root.symlink_to(outside, target_is_directory=True)
            return actual_link(*args, **kwargs)

        with monkeypatch.context() as patch:
            patch.setattr(secure_files.os, "link", move_root_before_link)
            secure_files.create_private_file_at(descriptor, "example.json", b"complete")
    assert (moved / "example.json").read_bytes() == b"complete"
    assert list(moved.iterdir()) == [moved / "example.json"]
    assert not list(outside.iterdir())


def test_two_processes_publish_exactly_one_complete_file(tmp_path):
    """Kernel no-replace publication gives concurrent creators exactly one winner."""
    root = tmp_path / "private"
    root.mkdir(mode=0o700)
    root.chmod(0o700)
    context = multiprocessing.get_context("spawn")
    barrier = context.Barrier(2)
    processes, receivers = [], []
    try:
        for content in (b"first complete candidate", b"second complete candidate"):
            receiver, sender = context.Pipe(duplex=False)
            process = context.Process(target=_competing_creator, args=(root, barrier, sender, content))
            process.start()
            sender.close()
            processes.append(process)
            receivers.append(receiver)
        for process in processes:
            process.join(timeout=15)
            assert not process.is_alive()
            assert process.exitcode == 0
        assert all(receiver.poll(1) for receiver in receivers)
        assert sorted(receiver.recv() for receiver in receivers) == ["created", "exists"]
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
            process.close()
        for receiver in receivers:
            receiver.close()
    assert (root / "example.json").read_bytes() in (b"first complete candidate", b"second complete candidate")
    assert list(root.iterdir()) == [root / "example.json"]
