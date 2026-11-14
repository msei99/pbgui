"""Owner-only filesystem helpers for PBGui credentials and session data."""

from __future__ import annotations

import os
import errno
import stat
import uuid
from pathlib import Path


PRIVATE_DIR_MODE = 0o700
PRIVATE_FILE_MODE = 0o600


class PrivateFilePublishedError(OSError):
    """A new file was published, but cleanup/durability acknowledgement failed."""


def create_private_file_at(directory_fd: int, name: str, content: bytes) -> None:
    """Atomically create a private file without replacing any existing entry.

    The caller owns the open directory descriptor. It must refer to an existing
    owner-only directory owned by the effective user. All operations stay bound
    to that descriptor; neither the working directory nor parent paths are used.
    Existing targets, including symlinks, are never opened or overwritten.
    ``PrivateFilePublishedError`` means the target exists: do not roll it back
    or blindly retry after an acknowledgement error.
    """
    if (os.name != "posix" or not hasattr(os, "O_NOFOLLOW")
            or type(directory_fd) is not int or directory_fd < 0):
        raise ValueError("Private creation requires a POSIX directory descriptor")
    if (not isinstance(name, str) or not name or name in {".", ".."}
            or "/" in name or "\\" in name
            or any(ord(char) < 32 or ord(char) == 127 for char in name)
            or not isinstance(content, bytes)):
        raise ValueError("Private creation requires a basename and bytes")
    info = os.fstat(directory_fd)
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid()
            or stat.S_IMODE(info.st_mode) != PRIVATE_DIR_MODE):
        raise ValueError("Private creation requires an owned 0700 directory")
    temporary = f".pbgui-create-{os.getpid()}-{uuid.uuid4().hex}.tmp"
    created, published, error = False, False, None
    try:
        try:
            descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                                 PRIVATE_FILE_MODE, dir_fd=directory_fd)
        except FileExistsError:
            # A temporary-name collision is not a conflict with the target.
            raise OSError(errno.EIO, "Private temporary creation failed") from None
        created = True
        try:
            os.fchmod(descriptor, PRIVATE_FILE_MODE)
            remaining = memoryview(content)
            while remaining:
                written = os.write(descriptor, remaining)
                if written <= 0:
                    raise OSError(errno.EIO, "Private write made no progress")
                remaining = remaining[written:]
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.link(temporary, name, src_dir_fd=directory_fd, dst_dir_fd=directory_fd,
                follow_symlinks=False)
        published = True
    except OSError as exc:
        error = exc
    finally:
        if created:
            try:
                os.unlink(temporary, dir_fd=directory_fd)
            except FileNotFoundError:
                pass
            except OSError as exc:
                if error is None:
                    error = exc
    if published and error is None:
        try:
            os.fsync(directory_fd)
        except OSError as exc:
            error = exc
    if error is not None:
        if published:
            raise PrivateFilePublishedError("Private file created with an I/O acknowledgement failure") from None
        raise error


def read_regular_file_nofollow(path: Path, approved_root: Path) -> bytes:
    """Read a regular file through no-follow descriptors below an approved root."""
    path = Path(os.path.abspath(Path(path).expanduser()))
    approved_root = Path(os.path.abspath(Path(approved_root).expanduser()))
    try:
        relative = path.relative_to(approved_root)
    except ValueError as exc:
        raise RuntimeError(f"Sensitive source escaped approved root: {path}") from exc
    if not relative.parts:
        raise RuntimeError("Sensitive source must be a file below its approved root")

    nofollow = getattr(os, "O_NOFOLLOW", 0)
    directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | nofollow
    descriptors: list[int] = []
    try:
        directory_fd = os.open(approved_root, directory_flags)
        descriptors.append(directory_fd)
        for part in relative.parts[:-1]:
            directory_fd = os.open(part, directory_flags, dir_fd=directory_fd)
            descriptors.append(directory_fd)
        file_fd = os.open(relative.parts[-1], os.O_RDONLY | nofollow, dir_fd=directory_fd)
        descriptors.append(file_fd)
        file_stat = os.fstat(file_fd)
        if not stat.S_ISREG(file_stat.st_mode):
            raise RuntimeError(f"Sensitive source is not a regular file: {path}")
        chunks: list[bytes] = []
        while True:
            chunk = os.read(file_fd, 1024 * 1024)
            if not chunk:
                return b"".join(chunks)
            chunks.append(chunk)
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def read_regular_file_prefix_nofollow(path: Path, approved_root: Path, limit: int = 4096) -> bytes:
    """Read a bounded header through no-follow descriptors, never an entire large body."""
    if type(limit) is not int or not 0 < limit <= 64 * 1024:
        raise ValueError('Choose a prefix limit from 1 to 65536 bytes')
    path = Path(os.path.abspath(Path(path).expanduser()))
    approved_root = Path(os.path.abspath(Path(approved_root).expanduser()))
    try:
        relative = path.relative_to(approved_root)
    except ValueError as exc:
        raise RuntimeError('Prefix source escaped approved root') from exc
    if not relative.parts:
        raise RuntimeError('Prefix source must be a file below its approved root')
    nofollow = getattr(os, 'O_NOFOLLOW', 0)
    descriptors = []
    try:
        directory_fd = os.open(approved_root, os.O_RDONLY | os.O_DIRECTORY | nofollow)
        descriptors.append(directory_fd)
        for part in relative.parts[:-1]:
            directory_fd = os.open(part, os.O_RDONLY | os.O_DIRECTORY | nofollow, dir_fd=directory_fd)
            descriptors.append(directory_fd)
        file_fd = os.open(relative.parts[-1], os.O_RDONLY | nofollow, dir_fd=directory_fd)
        descriptors.append(file_fd)
        if not stat.S_ISREG(os.fstat(file_fd).st_mode):
            raise RuntimeError('Prefix source is not a regular file')
        chunks, remaining = [], limit
        while remaining:
            chunk = os.read(file_fd, remaining)
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        return b''.join(chunks)
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def _reject_symlink(path: Path) -> None:
    """Reject symlinks at security-sensitive filesystem boundaries."""
    if path.is_symlink():
        raise RuntimeError(f"Refusing security-sensitive symlink: {path}")


def ensure_private_directory(path: Path) -> Path:
    """Create an owner-only directory and repair an existing directory mode."""
    path = Path(path)
    _reject_symlink(path)
    path.mkdir(parents=True, mode=PRIVATE_DIR_MODE, exist_ok=True)
    if not path.is_dir():
        raise RuntimeError(f"Private directory path is not a directory: {path}")
    if os.name == "posix":
        os.chmod(path, PRIVATE_DIR_MODE)
    return path


def ensure_private_directory_tree(root: Path, leaf: Path) -> Path:
    """Create and secure every directory from *root* through *leaf*."""
    root = Path(root)
    leaf = Path(leaf)
    try:
        relative = leaf.relative_to(root)
    except ValueError as exc:
        raise RuntimeError(f"Private directory {leaf} is outside {root}") from exc
    current = ensure_private_directory(root)
    for part in relative.parts:
        current = ensure_private_directory(current / part)
    return current


def secure_private_file(path: Path) -> Path:
    """Repair an existing sensitive file to owner-read/write permissions."""
    path = Path(path)
    _reject_symlink(path)
    if path.exists():
        if not path.is_file():
            raise RuntimeError(f"Private file path is not a file: {path}")
        if os.name == "posix":
            os.chmod(path, PRIVATE_FILE_MODE)
    return path


def atomic_write_private_bytes(path: Path, content: bytes) -> None:
    """Atomically write bytes without ever creating a world-readable file."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    _reject_symlink(path)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, PRIVATE_FILE_MODE)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
        secure_private_file(path)
    finally:
        tmp.unlink(missing_ok=True)


def atomic_write_private_text(path: Path, content: str, *, encoding: str = "utf-8") -> None:
    """Atomically write text with owner-only permissions."""
    atomic_write_private_bytes(Path(path), str(content).encode(encoding))


def copy_private_file(source: Path, destination: Path) -> None:
    """Copy a sensitive file atomically without inheriting permissive modes."""
    atomic_write_private_bytes(Path(destination), Path(source).read_bytes())


def harden_private_tree(root: Path) -> None:
    """Repair an existing sensitive directory tree to owner-only permissions."""
    root = Path(root)
    if not root.exists():
        return
    ensure_private_directory(root)
    for item in root.rglob("*"):
        _reject_symlink(item)
        if item.is_dir():
            ensure_private_directory(item)
        elif item.is_file():
            secure_private_file(item)


def harden_sensitive_paths(
    pbgui_root: Path,
    pb7_root: Path | None = None,
    aws_root: Path | None = None,
) -> None:
    """Migrate known PBGui credential stores to owner-only permissions."""
    pbgui_root = Path(pbgui_root)
    legacy_secrets = pbgui_root / ".streamlit" / "secrets.toml"
    for path in (
        pbgui_root / "pbgui.ini",
        pbgui_root / "data" / "auth" / "secrets.toml",
        legacy_secrets,
    ):
        secure_private_file(path)
    if legacy_secrets.exists():
        ensure_private_directory(legacy_secrets.parent)

    for path in (
        pbgui_root / "data" / "auth",
        pbgui_root / "data" / "api_tokens",
        pbgui_root / "data" / "api-keys",
        pbgui_root / "data" / "cluster" / "secret_blobs",
        pbgui_root / "data" / "vpsmanager",
    ):
        harden_private_tree(path)

    if pb7_root is not None:
        secure_private_file(Path(pb7_root) / "api-keys.json")

    if aws_root is not None and Path(aws_root).exists():
        aws_root = ensure_private_directory(Path(aws_root))
        secure_private_file(aws_root / "credentials")
        secure_private_file(aws_root / "config")
