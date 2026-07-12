"""Owner-only filesystem helpers for PBGui credentials and session data."""

from __future__ import annotations

import os
import uuid
from pathlib import Path


PRIVATE_DIR_MODE = 0o700
PRIVATE_FILE_MODE = 0o600


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


def harden_sensitive_paths(pbgui_root: Path, pb7_root: Path | None = None) -> None:
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
    ):
        harden_private_tree(path)

    if pb7_root is not None:
        secure_private_file(Path(pb7_root) / "api-keys.json")
