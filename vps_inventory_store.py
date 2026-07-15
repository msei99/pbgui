"""Private, process-safe persistence for local VPS inventory records."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
from pathlib import Path
from typing import Any, Callable

from file_lock import advisory_file_lock
from secure_files import atomic_write_private_text, ensure_private_directory, ensure_private_directory_tree


PERMANENT_INVENTORY_DENY_FIELDS = frozenset({"coinmarketcap_api_key"})


def _reject_denied_fields(payload: Any, *, pbgdir: Path | None = None) -> None:
    """Reject retired credential fields recursively before inventory mutation."""

    if isinstance(payload, dict):
        denied = sorted(PERMANENT_INVENTORY_DENY_FIELDS.intersection(payload))
        if denied:
            if pbgdir is not None:
                from credential_store import legacy_credential_writes_frozen

                if legacy_credential_writes_frozen(pbgdir):
                    raise ValueError(
                        f"Legacy credential inventory field is frozen during migration: {denied[0]}"
                    )
            raise ValueError(f"Legacy credential inventory field is permanently denied: {denied[0]}")
        for value in payload.values():
            _reject_denied_fields(value, pbgdir=pbgdir)
    elif isinstance(payload, list):
        for value in payload:
            _reject_denied_fields(value, pbgdir=pbgdir)


def _validated_path(root: Path, path: Path) -> tuple[Path, Path]:
    """Return canonical inventory paths after enforcing the root boundary."""
    root = Path(os.path.abspath(Path(root).expanduser()))
    path = Path(os.path.abspath(Path(path).expanduser()))
    try:
        relative = path.relative_to(root)
    except ValueError as exc:
        raise RuntimeError(f"Inventory path is outside the approved root: {path}") from exc
    if not relative.parts:
        raise RuntimeError("Inventory operation cannot target the inventory root")
    current = root
    if current.is_symlink():
        raise RuntimeError(f"Refusing symlinked inventory root: {current}")
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise RuntimeError(f"Refusing symlinked inventory path: {current}")
    return root, path


def _lock_path(root: Path, path: Path) -> Path:
    """Return one stable lock for every record belonging to the same host."""
    relative = path.relative_to(root)
    if len(relative.parts) >= 2 and relative.parts[0] == "hosts":
        identity = f"host:{relative.parts[1]}"
    else:
        identity = f"path:{relative.as_posix()}"
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()
    lock_dir = ensure_private_directory(root / ".locks")
    return lock_dir / f"{digest}.lock"


def _prepare_parent(root: Path, path: Path) -> None:
    """Create and secure the inventory directory chain for a target file."""
    ensure_private_directory_tree(root, path.parent)


def inventory_file_lock(root: Path, path: Path):
    """Return the shared cross-process lock for one validated inventory file."""

    root, path = _validated_path(root, path)
    ensure_private_directory(root)
    return advisory_file_lock(_lock_path(root, path))


def _write_json_unlocked(path: Path, payload: Any) -> None:
    """Atomically replace one JSON file while its inventory lock is held."""
    atomic_write_private_text(path, json.dumps(payload, indent=4) + "\n")


def write_inventory_json(root: Path, path: Path, payload: Any) -> None:
    """Replace one inventory JSON document under a cross-process lock."""
    _reject_denied_fields(payload, pbgdir=Path(root).parent.parent)
    root, path = _validated_path(root, path)
    ensure_private_directory(root)
    with advisory_file_lock(_lock_path(root, path)):
        _prepare_parent(root, path)
        _write_json_unlocked(path, payload)


def patch_inventory_json(
    root: Path,
    path: Path,
    updates: dict[str, Any],
    *,
    require_exists: bool = True,
) -> bool:
    """Apply a dictionary patch without losing fields written concurrently."""
    _reject_denied_fields(updates, pbgdir=Path(root).parent.parent)
    root, path = _validated_path(root, path)
    ensure_private_directory(root)
    with advisory_file_lock(_lock_path(root, path)):
        if require_exists and not path.exists():
            return False
        _prepare_parent(root, path)
        payload: dict[str, Any] = {}
        if path.exists():
            if path.is_symlink():
                raise RuntimeError(f"Refusing symlinked inventory file: {path}")
            loaded = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(loaded, dict):
                raise ValueError(f"Inventory JSON must contain an object: {path}")
            payload = loaded
        payload.update(dict(updates))
        _write_json_unlocked(path, payload)
        return True


def mutate_inventory_json(
    root: Path,
    path: Path,
    updater: Callable[[dict[str, Any] | None], tuple[bool, dict[str, Any] | None]],
) -> bool:
    """Run one complete JSON read-modify-write transaction under its lock."""
    root, path = _validated_path(root, path)
    ensure_private_directory(root)
    with advisory_file_lock(_lock_path(root, path)):
        current: dict[str, Any] | None = None
        if path.exists():
            loaded = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(loaded, dict):
                raise ValueError(f"Inventory JSON must contain an object: {path}")
            current = loaded
        changed, replacement = updater(dict(current) if current is not None else None)
        if not changed:
            return False
        if replacement is None:
            path.unlink(missing_ok=True)
            return True
        _reject_denied_fields(replacement, pbgdir=Path(root).parent.parent)
        _prepare_parent(root, path)
        _write_json_unlocked(path, replacement)
        return True


def write_versioned_inventory_json(
    root: Path,
    path: Path,
    payload: dict[str, Any],
    *,
    expected_revision: int,
    preserve_on_conflict: tuple[str, ...] = (),
    baseline: dict[str, Any] | None = None,
    deny_fields: frozenset[str] = frozenset(),
) -> dict[str, Any]:
    """Replace a record while retaining independently updated conflict fields."""
    _reject_denied_fields(payload, pbgdir=Path(root).parent.parent)
    saved: dict[str, Any] = {}

    def update(current: dict[str, Any] | None) -> tuple[bool, dict[str, Any]]:
        current = current or {}
        try:
            current_revision = max(int(current.get("_revision") or 0), 0)
        except (TypeError, ValueError):
            current_revision = 0
        replacement = dict(payload)
        if current_revision != max(int(expected_revision or 0), 0):
            if baseline is not None:
                replacement = dict(current)
                for field, value in payload.items():
                    if field not in baseline or baseline[field] != value:
                        replacement[field] = value
            else:
                for field in preserve_on_conflict:
                    if field in current:
                        replacement[field] = current[field]
        for field in deny_fields:
            replacement.pop(field, None)
        replacement["_revision"] = current_revision + 1
        saved.update(replacement)
        return True, replacement

    mutate_inventory_json(root, path, update)
    return saved


def patch_versioned_inventory_json(
    root: Path,
    path: Path,
    updates: dict[str, Any],
    *,
    require_exists: bool = True,
) -> bool:
    """Patch one versioned record and advance its conflict revision."""
    _reject_denied_fields(updates, pbgdir=Path(root).parent.parent)
    def update(current: dict[str, Any] | None) -> tuple[bool, dict[str, Any] | None]:
        if current is None and require_exists:
            return False, None
        replacement = current or {}
        try:
            revision = max(int(replacement.get("_revision") or 0), 0)
        except (TypeError, ValueError):
            revision = 0
        replacement.update(updates)
        replacement["_revision"] = revision + 1
        return True, replacement

    return mutate_inventory_json(root, path, update)


def delete_inventory_path(root: Path, path: Path) -> None:
    """Delete a host record or auxiliary file under its stable inventory lock."""
    root, path = _validated_path(root, path)
    ensure_private_directory(root)
    with advisory_file_lock(_lock_path(root, path)):
        if path.is_symlink():
            raise RuntimeError(f"Refusing symlinked inventory path: {path}")
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink(missing_ok=True)
