"""Idempotent, process-safe migrations run during PBGui API startup."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
from typing import Callable

from file_lock import advisory_file_lock
from secure_files import atomic_write_private_text, ensure_private_directory


PBGDIR = Path(__file__).resolve().parent
MIGRATION_ID = "20260712_logging_cleanup_v1"
RETIRED_SERVICE_LOGS_MIGRATION_ID = "20260712_retired_service_logs_v1"
LOG_INVENTORY_CLEANUP_MIGRATION_ID = "20260712_log_inventory_cleanup_v1"
OBSOLETE_INI_SECTIONS_MIGRATION_ID = "20260713_obsolete_ini_sections_v1"
_OBSOLETE_INI_SECTIONS = (
    "pbremote",
    "dashboard",
    "v7_grid_visualizer",
    "v7_strategy_explorer",
    "streamlit",
)
_OBSOLETE_LOG_STEMS = (
    "ApiLogging", "ApiKeys", "BalanceCalc", "CoinDataUI", "Dashboard",
    "Services", "V7Instances", "MarketDataAPI", "PB7OhlcvAPI", "PBV7UI",
    "BacktestV7", "BacktestV7API", "OptimizeV7", "OptimizeV7API",
)
_ROOT_LOG_NAMES = ("api_server.log", "pbgui.log")
_RETIRED_SERVICE_LOG_STEMS = ("PBRemote", "PBMon", "sync")
_OBSOLETE_INVENTORY_LOG_STEMS = (
    "FastAPI", "FileSync", "PBStat", "V7ConfigSync", "config_archives",
    "Auth", "LiveSession", "ApiKeyState", "User",
)


def _is_skipped() -> bool:
    """Return whether startup migrations are explicitly disabled."""
    return os.getenv("PBGUI_SKIP_STARTUP_MIGRATIONS", "").strip() == "1"


def _approved_candidate(path: Path, root: Path) -> Path:
    """Validate a deletion candidate without following a symlink."""
    root = root.resolve(strict=True)
    if path.is_symlink():
        raise RuntimeError(f"Refusing startup migration symlink: {path.name}")
    resolved = path.resolve(strict=True)
    if resolved.parent != root or not resolved.is_file():
        raise RuntimeError(f"Refusing startup migration path outside approved root: {path}")
    return resolved


def _logging_cleanup(root: Path) -> dict:
    """Remove only obsolete logging artifacts from fixed approved roots."""
    log_root = root / "data" / "logs"
    candidates: list[tuple[Path, Path]] = []
    if log_root.exists():
        allowed = {
            name
            for stem in _OBSOLETE_LOG_STEMS
            for name in (f"{stem}.log", f"{stem}.log.old", f"{stem}.log.lock")
        }
        generation_re = re.compile(
            rf"^(?:{'|'.join(re.escape(stem) for stem in _OBSOLETE_LOG_STEMS)})\.log\.\d+$"
        )
        income_re = re.compile(r"^income_other_[^/\\]+\.json$")
        for path in log_root.iterdir():
            if path.name in allowed or generation_re.fullmatch(path.name) or income_re.fullmatch(path.name):
                candidates.append((path, log_root))
    for name in ("queue_cpu_override.log",):
        path = log_root / name
        if path.exists() or path.is_symlink():
            candidates.append((path, log_root))
    for name in _ROOT_LOG_NAMES:
        path = root / name
        if path.exists() or path.is_symlink():
            candidates.append((path, root))

    removed_names: list[str] = []
    removed_bytes = 0
    for path, approved_root in candidates:
        safe_path = _approved_candidate(path, approved_root)
        size = safe_path.stat().st_size
        safe_path.unlink()
        removed_names.append(path.name)
        removed_bytes += size
    return {
        "names": sorted(removed_names),
        "count": len(removed_names),
        "bytes": removed_bytes,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def _retired_service_logs_cleanup(root: Path) -> dict:
    """Remove exact log generations for services with no remaining writer."""
    log_root = root / "data" / "logs"
    candidates: list[Path] = []
    if log_root.exists():
        allowed = {
            name
            for stem in _RETIRED_SERVICE_LOG_STEMS
            for name in (f"{stem}.log", f"{stem}.log.old", f"{stem}.log.lock")
        }
        generation_re = re.compile(
            rf"^(?:{'|'.join(re.escape(stem) for stem in _RETIRED_SERVICE_LOG_STEMS)})\.log\.\d+$"
        )
        candidates = [
            path
            for path in log_root.iterdir()
            if path.name in allowed or generation_re.fullmatch(path.name)
        ]

    removed_names: list[str] = []
    removed_bytes = 0
    for path in candidates:
        safe_path = _approved_candidate(path, log_root)
        removed_bytes += safe_path.stat().st_size
        safe_path.unlink()
        removed_names.append(path.name)
    return {
        "names": sorted(removed_names),
        "count": len(removed_names),
        "bytes": removed_bytes,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def _obsolete_inventory_logs_cleanup(root: Path) -> dict:
    """Remove retired logs and old files for helpers now grouped into PBGui."""
    log_root = root / "data" / "logs"
    candidates: list[Path] = []
    if log_root.exists():
        allowed = {
            name
            for stem in _OBSOLETE_INVENTORY_LOG_STEMS
            for name in (f"{stem}.log", f"{stem}.log.old", f"{stem}.log.lock")
        }
        generation_re = re.compile(
            rf"^(?:{'|'.join(re.escape(stem) for stem in _OBSOLETE_INVENTORY_LOG_STEMS)})\.log\.\d+$"
        )
        candidates = [
            path
            for path in log_root.iterdir()
            if path.name in allowed or generation_re.fullmatch(path.name)
        ]

    removed_names: list[str] = []
    removed_bytes = 0
    for path in candidates:
        safe_path = _approved_candidate(path, log_root)
        removed_bytes += safe_path.stat().st_size
        safe_path.unlink()
        removed_names.append(path.name)
    return {
        "names": sorted(removed_names),
        "count": len(removed_names),
        "bytes": removed_bytes,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def _obsolete_ini_sections_cleanup(root: Path) -> dict:
    """Remove only sections owned by retired PBRemote and Streamlit UIs."""
    ini_path = root / "pbgui.ini"
    removed: list[str] = []
    if ini_path.exists():
        if ini_path.is_symlink():
            raise RuntimeError("Refusing symlinked pbgui.ini during startup migration")
        with advisory_file_lock(ini_path):
            lines = ini_path.read_text(encoding="utf-8").splitlines(keepends=True)
            output: list[str] = []
            skipping = False
            for line in lines:
                section_match = re.match(r"^\s*\[([^\]]+)\]\s*(?:[#;].*)?(?:\r?\n)?$", line)
                if section_match:
                    section = section_match.group(1).strip()
                    skipping = section in _OBSOLETE_INI_SECTIONS
                    if skipping:
                        removed.append(section)
                        continue
                if not skipping:
                    output.append(line)
            if removed:
                atomic_write_private_text(ini_path, "".join(output))
    return {
        "sections": removed,
        "count": len(removed),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


MIGRATIONS: tuple[tuple[str, Callable[[Path], dict]], ...] = (
    (MIGRATION_ID, _logging_cleanup),
    (RETIRED_SERVICE_LOGS_MIGRATION_ID, _retired_service_logs_cleanup),
    (LOG_INVENTORY_CLEANUP_MIGRATION_ID, _obsolete_inventory_logs_cleanup),
    (OBSOLETE_INI_SECTIONS_MIGRATION_ID, _obsolete_ini_sections_cleanup),
)


def run_startup_migrations(pbgdir: Path | None = None) -> dict:
    """Run pending migrations and atomically persist completion metadata."""
    if _is_skipped():
        return {"skipped": True, "completed": []}
    root = Path(pbgdir) if pbgdir is not None else PBGDIR
    state_dir = root / "data" / "state"
    state_path = state_dir / "startup_migrations.json"
    ensure_private_directory(state_dir)
    with advisory_file_lock(state_path):
        state: dict = {"completed": {}}
        if state_path.exists():
            if state_path.is_symlink():
                raise RuntimeError("Refusing symlinked startup migration state")
            try:
                loaded = json.loads(state_path.read_text(encoding="utf-8"))
                if isinstance(loaded, dict) and isinstance(loaded.get("completed"), dict):
                    state = loaded
            except (OSError, ValueError):
                state = {"completed": {}}
        completed_now = []
        for migration_id, migration in MIGRATIONS:
            if migration_id in state["completed"]:
                continue
            result = migration(root)
            state["completed"][migration_id] = result
            atomic_write_private_text(state_path, json.dumps(state, indent=4, sort_keys=True) + "\n")
            completed_now.append(migration_id)
        return {"skipped": False, "completed": completed_now}
