"""Cross-version arbitration for automatic PB7/PB8 optimizer launches."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

import psutil

from file_lock import advisory_file_lock
from pbgui_purefunc import PBGDIR
from secure_files import atomic_write_private_text, ensure_private_directory

_PENDING_TTL_SECONDS = 120.0


def _state_root() -> Path:
    return Path(PBGDIR) / "data" / "locks"


def _state_path() -> Path:
    return _state_root() / "optimize-autostart.json"


def _lock_target() -> Path:
    return _state_root() / "optimize-autostart"


def _read_state() -> dict | None:
    path = _state_path()
    if not path.exists():
        return None
    if path.is_symlink() or not path.is_file():
        raise RuntimeError("Optimize autostart state is not a regular managed file")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Failed to read optimize autostart state: {exc}") from exc
    return payload if isinstance(payload, dict) else None


def _process_matches(payload: dict) -> bool:
    try:
        pid = int(payload.get("pid") or 0)
        create_time = float(payload.get("create_time") or 0.0)
        process = psutil.Process(pid)
        if abs(process.create_time() - create_time) > 0.01:
            return False
        command = [str(part) for part in process.cmdline()]
        markers = [str(marker) for marker in payload.get("command_markers") or [] if str(marker)]
        return bool(markers) and all(marker in command for marker in markers)
    except (ValueError, TypeError, psutil.NoSuchProcess, psutil.ZombieProcess, psutil.AccessDenied):
        return False


def _claim_is_active(payload: dict | None) -> bool:
    if not payload:
        return False
    if payload.get("state") == "running":
        return _process_matches(payload)
    if payload.get("state") != "pending":
        return False
    claimed_at = 0.0
    try:
        claimed_at = float(payload.get("claimed_at") or 0.0)
        owner_pid = int(payload.get("owner_pid") or 0)
        owner_create_time = float(payload.get("owner_create_time") or 0.0)
        owner = psutil.Process(owner_pid)
        if abs(owner.create_time() - owner_create_time) <= 0.01:
            return True
    except (ValueError, TypeError, psutil.NoSuchProcess, psutil.ZombieProcess, psutil.AccessDenied):
        pass
    return time.time() - claimed_at < _PENDING_TTL_SECONDS


def _is_optimizer_command(command: list[str]) -> bool:
    """Recognize PBGui-managed and direct PB7/PB8 optimizer commands."""
    parts = [str(part) for part in command]
    names = [Path(part).name.lower() for part in parts]
    if "optimize.py" in names:
        return True
    if "pb8_optimize_runner.py" in names and "optimize" in parts:
        return True
    return "optimize" in parts and any(name in {"passivbot", "passivbot.exe"} for name in names)


def _optimizer_process_running() -> bool:
    """Return whether any manual or automatic PB7/PB8 optimizer is active."""
    for process in psutil.process_iter(["cmdline"]):
        try:
            command = process.info.get("cmdline") or []
            if _is_optimizer_command(command):
                return True
        except (psutil.NoSuchProcess, psutil.ZombieProcess, psutil.AccessDenied):
            continue
    return False


def claim_autostart(version: str, job_id: str) -> bool:
    """Claim the one global automatic optimizer slot without affecting manual starts."""
    ensure_private_directory(_state_root())
    with advisory_file_lock(_lock_target()):
        current = _read_state()
        if _claim_is_active(current):
            return False
        if _optimizer_process_running():
            return False
        owner = psutil.Process(os.getpid())
        payload = {
            "state": "pending",
            "version": str(version),
            "job_id": str(job_id),
            "claimed_at": time.time(),
            "owner_pid": os.getpid(),
            "owner_create_time": owner.create_time(),
        }
        atomic_write_private_text(_state_path(), json.dumps(payload, indent=4) + "\n")
        return True


def publish_autostart_process(
    version: str,
    job_id: str,
    pid: int,
    create_time: float,
    command_markers: list[str],
) -> None:
    """Replace a pending claim with the detached optimizer's verified identity."""
    ensure_private_directory(_state_root())
    with advisory_file_lock(_lock_target()):
        current = _read_state()
        if not current or current.get("version") != version or current.get("job_id") != job_id:
            raise RuntimeError("Optimize autostart claim ownership changed before launch publication")
        payload = {
            "state": "running",
            "version": version,
            "job_id": job_id,
            "pid": int(pid),
            "create_time": float(create_time),
            "command_markers": [str(marker) for marker in command_markers],
            "published_at": time.time(),
        }
        if not _process_matches(payload):
            raise RuntimeError("Automatic optimizer process identity could not be verified")
        atomic_write_private_text(_state_path(), json.dumps(payload, indent=4) + "\n")


def release_autostart(version: str, job_id: str) -> None:
    """Release a failed pending claim without disturbing another version's owner."""
    ensure_private_directory(_state_root())
    with advisory_file_lock(_lock_target()):
        current = _read_state()
        if current and current.get("version") == version and current.get("job_id") == job_id:
            _state_path().unlink(missing_ok=True)
