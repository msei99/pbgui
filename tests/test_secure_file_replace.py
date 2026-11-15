"""Descriptor-bound private replacement tests with held source inode identities."""

from contextlib import contextmanager
import os
import stat

import pytest

import secure_files
from tests.test_secure_file_create import private_directory


@contextmanager
def existing_source(root):
    """Retain the compared source inode through a complete replacement attempt."""
    with private_directory(root) as directory_fd:
        target = root / "example.json"
        target.write_bytes(b"original bytes")
        source_fd = os.open(target.name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory_fd)
        try:
            info = os.fstat(source_fd)
            yield directory_fd, source_fd, target, (info.st_dev, info.st_ino)
        finally:
            os.close(source_fd)


def test_replace_is_atomic_private_and_retains_old_reader(tmp_path):
    """An old open FD sees old bytes while the new entry is a complete private inode."""
    root = tmp_path / "private"
    with existing_source(root) as (directory_fd, source_fd, target, identity):
        secure_files.replace_private_file_at(directory_fd, target.name, b"updated bytes", expected_identity=identity)
        assert os.read(source_fd, 100) == b"original bytes"
        assert target.read_bytes() == b"updated bytes"
        assert target.stat().st_ino != identity[1]
        assert stat.S_IMODE(target.stat().st_mode) == 0o600
        assert list(root.iterdir()) == [target]


@pytest.mark.parametrize("failure", ["write", "file_sync", "replace"])
def test_prepublication_failure_preserves_the_old_entry(tmp_path, monkeypatch, failure):
    """Failed preparation or rename cannot alter the existing source or leave a temp."""
    root = tmp_path / "private"
    with existing_source(root) as (directory_fd, source_fd, target, identity):
        before_mode = target.stat().st_mode

        def fail(*args, **kwargs):
            """Represent an I/O failure before the replacement commit point."""
            raise OSError("fixture-only-replacement-error")

        operation = {"write": "write", "file_sync": "fsync", "replace": "replace"}[failure]
        with monkeypatch.context() as patch:
            patch.setattr(secure_files.os, operation, fail)
            with pytest.raises(OSError) as caught:
                secure_files.replace_private_file_at(directory_fd, target.name, b"updated", expected_identity=identity)
            assert not isinstance(caught.value, secure_files.PrivateFilePublishedError)
        assert target.read_bytes() == b"original bytes"
        assert target.stat().st_ino == identity[1]
        assert target.stat().st_mode == before_mode
        assert list(root.iterdir()) == [target]


@pytest.mark.parametrize("kind", ["symlink", "fifo", "directory", "missing", "other_regular"])
def test_changed_or_nonregular_destination_is_not_replaced(tmp_path, kind):
    """A caller's retained inode identity never authorizes following a new path target."""
    root = tmp_path / "private"
    outside = tmp_path / "outside.json"
    outside.write_bytes(b"outside bytes")
    with existing_source(root) as (directory_fd, source_fd, target, identity):
        target.unlink()
        if kind == "symlink":
            target.symlink_to(outside)
        elif kind == "fifo":
            os.mkfifo(target)
        elif kind == "directory":
            target.mkdir()
        elif kind == "other_regular":
            target.write_bytes(b"another complete source")
        expected = (FileNotFoundError if kind == "missing" else
                    secure_files.PrivateFileChangedError if kind == "other_regular" else ValueError)
        with pytest.raises(expected):
            secure_files.replace_private_file_at(directory_fd, target.name, b"updated", expected_identity=identity)
        assert os.read(source_fd, 100) == b"original bytes"
        assert list(root.iterdir()) == ([] if kind == "missing" else [target])
        if kind == "other_regular":
            assert target.read_bytes() == b"another complete source"
    assert outside.read_bytes() == b"outside bytes"


def test_inode_changed_during_preparation_is_checked_again(tmp_path, monkeypatch):
    """An external rename during data sync is detected before replacing its new inode."""
    root = tmp_path / "private"
    actual_sync = os.fsync
    with existing_source(root) as (directory_fd, source_fd, target, identity):
        def change_target_after_sync(descriptor):
            """Simulate a same-UID writer ignoring the directory lock."""
            actual_sync(descriptor)
            target.unlink()
            target.write_bytes(b"external winner")

        with monkeypatch.context() as patch:
            patch.setattr(secure_files.os, "fsync", change_target_after_sync)
            with pytest.raises(secure_files.PrivateFileChangedError):
                secure_files.replace_private_file_at(directory_fd, target.name, b"updated", expected_identity=identity)
        assert target.read_bytes() == b"external winner"
        assert list(root.iterdir()) == [target]


def test_directory_sync_failure_preserves_the_published_update(tmp_path, monkeypatch):
    """A committed rename is reported distinctly and is never rolled back."""
    root = tmp_path / "private"
    actual_sync = os.fsync
    with existing_source(root) as (directory_fd, source_fd, target, identity):
        def fail_directory_ack(descriptor):
            """Allow file data sync, then fail the directory acknowledgement."""
            if descriptor == directory_fd:
                raise OSError("fixture-only-directory-ack-error")
            actual_sync(descriptor)

        with monkeypatch.context() as patch:
            patch.setattr(secure_files.os, "fsync", fail_directory_ack)
            with pytest.raises(secure_files.PrivateFilePublishedError):
                secure_files.replace_private_file_at(directory_fd, target.name, b"updated", expected_identity=identity)
        assert target.read_bytes() == b"updated"
        assert os.read(source_fd, 100) == b"original bytes"
        assert list(root.iterdir()) == [target]
