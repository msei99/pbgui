"""Generation-neutral live bundle validation, backup, and publication helpers.

PB8 and PB9 live APIs share these helpers through an explicit frozen
``BundleStore`` context. Each generation builds the context per call from its
own module globals and keeps thin native wrappers around the helpers here.
"""

from __future__ import annotations

import json
import os
import shutil
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable

from fastapi import HTTPException

from file_lock import advisory_file_lock
from secure_files import ensure_private_directory_tree, secure_private_file


SERVICE = "LiveBundleStore"


@dataclass(frozen=True)
class BundleStore:
    """Describe one generation's live bundle backup store and native callbacks."""

    label: str
    service: str
    data_root: Path
    backup_root: Path
    log: Callable[..., Any]
    read_backup_metadata: Callable[[Path], dict[str, Any]]
    native_errors: tuple[type[BaseException], ...]
    configuration_error: Callable[[str, Exception], Exception]
    sidecar_filenames: Callable[[dict[str, Any]], Iterable[str]]
    optional_logs: Callable[[str, Path], dict[str, Path]]


def validate_name(name: str) -> str:
    """Validate one filesystem component used as an instance name."""

    value = str(name or "")
    if value != value.strip() or not value or value.startswith(".") or value in {".", ".."}:
        raise HTTPException(status_code=400, detail="Invalid instance name")
    if any(char in value for char in ("/", "\\", "\x00")) or any(ord(char) < 32 for char in value):
        raise HTTPException(status_code=400, detail="Invalid instance name")
    if len(value.encode("utf-8")) > 128:
        raise HTTPException(status_code=400, detail="Instance name is too long")
    return value


def validate_target(hostname: Any) -> str:
    """Validate an exact enabled_on target without normalizing it silently."""

    if not isinstance(hostname, str) or hostname != hostname.strip():
        raise HTTPException(status_code=400, detail="Invalid enabled_on target")
    if not hostname or hostname in {".", ".."} or any(char in hostname for char in ("/", "\\", "\x00")):
        raise HTTPException(status_code=400, detail="Invalid enabled_on target")
    if any(ord(char) < 32 for char in hostname):
        raise HTTPException(status_code=400, detail="Invalid enabled_on target")
    return hostname


def validate_backup_id(backup_id: Any) -> str:
    """Validate an immutable numeric config-version backup ID."""
    value = str(backup_id or "")
    if not value.isdigit() or int(value) < 1:
        raise HTTPException(status_code=400, detail="Invalid backup ID")
    return value


def coerce_version(value: Any) -> int:
    """Return a non-negative integer config version."""

    if isinstance(value, bool):
        return 0
    try:
        return max(0, int(str(value).strip()))
    except (TypeError, ValueError):
        return 0


@contextmanager
def backup_lock(store: BundleStore):
    """Serialize backup settings, snapshots, retention, and deletion."""
    root = store.backup_root
    ensure_private_directory_tree(store.data_root, root.parent)
    ensure_private_directory_tree(root.parent, root)
    if root.is_symlink():
        raise RuntimeError(f"{store.label} backup root must not be a symlink")
    with advisory_file_lock(root / ".write"):
        yield


def backup_settings_unlocked(store: BundleStore) -> dict[str, int]:
    """Load bounded backup settings while holding the backup lock."""
    path = store.backup_root / "_settings.json"
    if not path.exists():
        return {"max_versions": 50}
    if path.is_symlink() or not path.is_file():
        raise HTTPException(status_code=409, detail=f"{store.label} backup settings path is unsafe")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        value = int(payload.get("max_versions", 50)) if isinstance(payload, dict) else 50
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        store.log(store.service, f"{store.label} backup settings are invalid: {exc}", level="WARNING")
        return {"max_versions": 50}
    return {"max_versions": max(1, min(1000, value))}


def backup_dirs_unlocked(instance_root: Path) -> list[Path]:
    """Return valid backup directories newest first."""
    if not instance_root.is_dir() or instance_root.is_symlink():
        return []
    result = []
    for entry in instance_root.iterdir():
        if entry.is_symlink() or not entry.is_dir() or not entry.name.isdigit():
            continue
        config_path = entry / "config.json"
        if config_path.is_file() and not config_path.is_symlink():
            result.append(entry)
    return sorted(result, key=lambda item: (int(item.name), item.stat().st_mtime_ns), reverse=True)


def prune_backups_unlocked(instance_root: Path, max_versions: int) -> None:
    """Remove backup versions beyond retention while holding the lock."""
    for entry in backup_dirs_unlocked(instance_root)[max_versions:]:
        shutil.rmtree(entry)
    if instance_root.is_dir() and not any(instance_root.iterdir()):
        instance_root.rmdir()


def snapshot_bundle_unlocked(
    store: BundleStore,
    name: str,
    instance_dir: Path,
    *,
    retain: Callable[[Path], None] | None = None,
) -> str:
    """Atomically snapshot one exact current live bundle by config version.

    ``retain`` lets a generation route retention through its own wrappers; the
    default applies the store's persisted backup settings.
    """
    label = store.label
    name = validate_name(name)
    if instance_dir.is_symlink() or not instance_dir.is_dir():
        raise HTTPException(status_code=404, detail=f"{label} instance '{name}' not found")
    config_path = instance_dir / "config.json"
    if config_path.is_symlink() or not config_path.is_file():
        raise HTTPException(status_code=404, detail=f"{label} instance '{name}' has no safe config.json")
    try:
        config = store.read_backup_metadata(config_path)
    except store.native_errors as exc:
        raise store.configuration_error(f"Backing up {label} instance '{name}'", exc) from exc
    pbgui = config.get("pbgui") if isinstance(config.get("pbgui"), dict) else {}
    version = coerce_version(pbgui.get("version"))
    if version < 1:
        raise HTTPException(status_code=422, detail=f"{label} backup requires a positive config version")

    instance_root = store.backup_root / name
    ensure_private_directory_tree(store.backup_root, instance_root)
    target = instance_root / str(version)
    if target.exists():
        if target.is_symlink() or not target.is_dir() or not (target / "config.json").is_file():
            raise HTTPException(status_code=409, detail=f"{label} backup '{name}/{version}' is unsafe")
        return str(version)

    stage = instance_root / f".stage-{uuid.uuid4().hex}"
    ensure_private_directory_tree(instance_root, stage)
    try:
        filenames = ["config.json", *store.sidecar_filenames(config)]
        for filename in filenames:
            source = instance_dir / filename
            if source.is_symlink() or not source.is_file():
                raise HTTPException(status_code=422, detail=f"{label} backup source '{filename}' is missing or unsafe")
            destination = stage / filename
            shutil.copy2(source, destination)
            secure_private_file(destination)
        for filename, source in store.optional_logs(name, instance_dir).items():
            if source.is_symlink() or not source.is_file():
                continue
            destination = stage / filename
            shutil.copy2(source, destination)
            secure_private_file(destination)
        os.replace(stage, target)
    except Exception:
        shutil.rmtree(stage, ignore_errors=True)
        raise
    if retain is None:
        prune_backups_unlocked(instance_root, backup_settings_unlocked(store)["max_versions"])
    else:
        retain(instance_root)
    store.log(store.service, f"Backed up {label} live instance '{name}' as version {version}", level="INFO")
    return str(version)


def publish_staged_bundle(stage_dir: Path, target_dir: Path, *, generation_prefix: str, label: str,
                          on_publish=None) -> None:
    """Atomically replace one complete live bundle, restoring on placement failure."""
    previous_dir = target_dir.parent / f"{generation_prefix}-old-{uuid.uuid4().hex}"
    moved_previous = False
    placed = False
    try:
        if target_dir.exists():
            if target_dir.is_symlink() or not target_dir.is_dir():
                raise OSError(f"{label} target directory is unsafe")
            os.replace(target_dir, previous_dir)
            moved_previous = True
        os.replace(stage_dir, target_dir)
        placed = True
        if on_publish is not None:
            on_publish()
    except Exception:
        if placed:
            shutil.rmtree(target_dir)
        if moved_previous and previous_dir.is_dir() and not target_dir.exists():
            os.replace(previous_dir, target_dir)
        raise
    if moved_previous:
        shutil.rmtree(previous_dir, ignore_errors=True)
