"""Cross-version slot arbitration for automatic PB7/PB8 backtest launches."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

import psutil

from file_lock import advisory_file_lock
from pbgui_purefunc import PBGDIR
from secure_files import atomic_write_private_text, ensure_private_directory

_PENDING_TTL_SECONDS = 180.0


def _state_root() -> Path:
    return Path(PBGDIR) / "data" / "locks"


def _state_path() -> Path:
    return _state_root() / "backtest-autostart.json"


def _lock_target() -> Path:
    return _state_root() / "backtest-autostart"


def _read_claims() -> list[dict]:
    path = _state_path()
    if not path.exists():
        return []
    if path.is_symlink() or not path.is_file():
        raise RuntimeError("Backtest autostart state is not a regular managed file")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Failed to read backtest autostart state: {exc}") from exc
    return [item for item in payload if isinstance(item, dict)] if isinstance(payload, list) else []


def _write_claims(claims: list[dict]) -> None:
    path = _state_path()
    if not claims:
        path.unlink(missing_ok=True)
        return
    atomic_write_private_text(path, json.dumps(claims, indent=4) + "\n")


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


def _claim_is_active(payload: dict) -> bool:
    if payload.get("state") == "running":
        return _process_matches(payload)
    if payload.get("state") != "pending":
        return False
    try:
        claimed_at = float(payload.get("claimed_at") or 0.0)
        owner_pid = int(payload.get("owner_pid") or 0)
        owner_create_time = float(payload.get("owner_create_time") or 0.0)
        owner = psutil.Process(owner_pid)
        return time.time() - claimed_at < _PENDING_TTL_SECONDS and abs(owner.create_time() - owner_create_time) <= 0.01
    except (ValueError, TypeError, psutil.NoSuchProcess, psutil.ZombieProcess, psutil.AccessDenied):
        return False


def _managed_processes() -> dict[int, float]:
    """Find PBGui-compatible PB7 and PB8 backtest parent processes."""
    active = {}
    for process in psutil.process_iter(["pid", "create_time", "cmdline"]):
        try:
            command = [str(part) for part in process.info.get("cmdline") or []]
            names = [Path(part).name.lower() for part in command]
            is_pb7 = "backtest.py" in names
            is_pb8 = "pb8_backtest_runner.py" in names and "backtest" in command
            if is_pb7 or is_pb8:
                active[int(process.info["pid"])] = float(process.info["create_time"])
        except (ValueError, TypeError, psutil.NoSuchProcess, psutil.ZombieProcess, psutil.AccessDenied):
            continue
    return active


def _active_state() -> tuple[list[dict], int]:
    claims = [claim for claim in _read_claims() if _claim_is_active(claim)]
    tracked_pids = {int(claim["pid"]) for claim in claims if claim.get("state") == "running" and claim.get("pid")}
    untracked_processes = [pid for pid in _managed_processes() if pid not in tracked_pids]
    return claims, len(claims) + len(untracked_processes)


def claim_backtest_slot(version: str, job_id: str, limit: int) -> bool:
    """Reserve one shared automatic backtest slot across PB7 and PB8."""
    ensure_private_directory(_state_root())
    with advisory_file_lock(_lock_target()):
        claims, active_count = _active_state()
        if active_count >= max(1, int(limit)):
            _write_claims(claims)
            return False
        owner = psutil.Process(os.getpid())
        claims.append(
            {
                "state": "pending",
                "version": str(version),
                "job_id": str(job_id),
                "claimed_at": time.time(),
                "owner_pid": os.getpid(),
                "owner_create_time": owner.create_time(),
            }
        )
        _write_claims(claims)
        return True


def publish_backtest_process(
    version: str,
    job_id: str,
    pid: int,
    create_time: float,
    command_markers: list[str],
) -> None:
    """Publish the verified process identity for a pending slot claim."""
    ensure_private_directory(_state_root())
    with advisory_file_lock(_lock_target()):
        claims, _active_count = _active_state()
        index = next(
            (
                idx
                for idx, claim in enumerate(claims)
                if claim.get("state") == "pending"
                and claim.get("version") == version
                and claim.get("job_id") == job_id
            ),
            None,
        )
        if index is None:
            raise RuntimeError("Backtest autostart claim ownership changed before launch publication")
        payload = {
            "state": "running",
            "version": str(version),
            "job_id": str(job_id),
            "pid": int(pid),
            "create_time": float(create_time),
            "command_markers": [str(marker) for marker in command_markers],
            "published_at": time.time(),
        }
        if not _process_matches(payload):
            raise RuntimeError("Automatic backtest process identity could not be verified")
        claims[index] = payload
        _write_claims(claims)


def release_backtest_slot(version: str, job_id: str) -> None:
    """Release a failed pending claim without disturbing other running jobs."""
    ensure_private_directory(_state_root())
    with advisory_file_lock(_lock_target()):
        claims, _active_count = _active_state()
        claims = [
            claim
            for claim in claims
            if not (claim.get("version") == version and claim.get("job_id") == job_id)
        ]
        _write_claims(claims)
