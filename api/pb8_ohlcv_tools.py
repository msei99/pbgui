"""PB8-native OHLCV preflight and persistent preload jobs."""

from __future__ import annotations

import asyncio
import copy
import json
import os
import platform
import signal
import subprocess
import sys
import threading
import time
import uuid
from collections import deque
from pathlib import Path
from shutil import which
from typing import Any

import psutil

from file_lock import advisory_file_lock
from logging_helpers import human_log as _log
from master_update_lock import MasterUpdateBusyError, acquire_master_runtime_lock
from pbgui_purefunc import PBGDIR, pb8_runtime_status
from secure_files import atomic_write_private_text, ensure_private_directory_tree

SERVICE = "PB8OhlcvAPI"

_JOBS: dict[str, dict[str, Any]] = {}
_REAPERS: dict[str, threading.Thread] = {}
_LOCK = threading.RLock()
_RESTORED = False
_JOB_TTL_SECONDS = 24 * 60 * 60
_MAX_JOBS = 64


class PB8OhlcvError(RuntimeError):
    """Base error returned through the V8 editor endpoint contract."""


class PB8OhlcvUnavailableError(PB8OhlcvError):
    """Raised when the configured PB8 runtime cannot execute a request."""


class PB8OhlcvBusyError(PB8OhlcvUnavailableError):
    """Raised while an install/update owns the PB8 runtime."""


class PB8OhlcvUnsupportedError(PB8OhlcvError):
    """Raised when PB8 has no safe native preparation path for a request."""


def _utc_ms() -> int:
    """Return current UTC epoch milliseconds."""
    return int(time.time() * 1000)


def _iso_from_ms(value: int | None) -> str | None:
    """Format epoch milliseconds for the shared preload response contract."""
    if not value:
        return None
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(int(value) / 1000.0))


def _runtime() -> dict[str, Any]:
    """Return a ready PB8 runtime with its native download command."""
    status = pb8_runtime_status()
    if not status.get("ready"):
        detail = "; ".join(status.get("errors") or []) or "PB8 runtime is not ready"
        raise PB8OhlcvUnavailableError(detail)
    pb8_dir = Path(str(status.get("pb8dir") or "")).resolve(strict=False)
    pb8_python = Path(str(status.get("pb8venv") or "")).resolve(strict=False)
    cli_file = Path(str(status.get("cli_file") or pb8_python.parent / "passivbot")).resolve(strict=False)
    if cli_file.parent != pb8_python.parent or not cli_file.is_file() or not os.access(cli_file, os.X_OK):
        raise PB8OhlcvUnavailableError(
            f"PB8 native downloader is not executable in the configured virtualenv: {cli_file}"
        )
    return {**status, "pb8dir": str(pb8_dir), "pb8venv": str(pb8_python), "cli_file": str(cli_file)}


def _is_below(path: Path, root: Path) -> bool:
    """Return whether a resolved path is inside an approved root."""
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _pbgui_market_data_root() -> Path:
    """Return PBGui's approved absolute OHLCV source root."""
    from market_data import get_market_data_root_dir

    return Path(get_market_data_root_dir()).expanduser().resolve(strict=False)


def resolve_pb8_ohlcv_paths(
    raw_config: dict[str, Any], status: dict[str, Any] | None = None
) -> tuple[dict[str, Any], Path | None, Path]:
    """Resolve default/relative PB8 paths and approved PBGui absolute sources."""
    if not isinstance(raw_config, dict):
        raise PB8OhlcvError("config must be an object")
    runtime = status or _runtime()
    pb8_dir = Path(str(runtime.get("pb8dir") or "")).expanduser().resolve(strict=False)
    if not pb8_dir.is_dir():
        raise PB8OhlcvUnavailableError(f"PB8 source directory does not exist: {pb8_dir}")
    config = copy.deepcopy(raw_config)
    backtest = config.get("backtest")
    if not isinstance(backtest, dict):
        backtest = {}
        config["backtest"] = backtest
    source_value = str(backtest.get("ohlcv_source_dir") or "").strip()
    source_dir = None
    if source_value:
        source_candidate = Path(source_value).expanduser()
        if not source_candidate.is_absolute():
            source_candidate = pb8_dir / source_candidate
        source_dir = source_candidate.resolve(strict=False)
        market_root = _pbgui_market_data_root()
        if not _is_below(source_dir, pb8_dir) and not _is_below(source_dir, market_root):
            raise PB8OhlcvUnsupportedError(
                "PB8 ohlcv_source_dir must be relative to the configured PB8 directory or below "
                f"the PBGui market-data root ({market_root})."
            )
        backtest["ohlcv_source_dir"] = str(source_dir)
    catalog_path = (pb8_dir / "caches" / "ohlcvs" / "catalog.sqlite").resolve(strict=False)
    return config, source_dir, catalog_path


def _acquire_runtime_lease():
    """Acquire the shared launch lease with an endpoint-specific retry message."""
    try:
        return acquire_master_runtime_lock(Path(PBGDIR))
    except MasterUpdateBusyError as exc:
        raise PB8OhlcvBusyError(
            "PB8 is being installed or updated. Retry the OHLCV operation when the update finishes."
        ) from exc


def _run_preflight_helper(raw_config: dict[str, Any]) -> dict[str, Any]:
    """Run read-only PB8 planning in its isolated virtualenv."""
    runtime_lease = _acquire_runtime_lease()
    try:
        status = _runtime()
        config, _source_dir, _catalog = resolve_pb8_ohlcv_paths(raw_config, status)
        helper = Path(__file__).resolve().with_name("pb8_ohlcv_runtime_helper.py")
        request = {"pb8_dir": status["pb8dir"], "config": config}
        try:
            proc = subprocess.run(
                [status["pb8venv"], str(helper)],
                cwd=status["pb8dir"],
                input=json.dumps(request),
                text=True,
                capture_output=True,
                check=False,
                timeout=120,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise PB8OhlcvUnavailableError(f"PB8 OHLCV preflight helper failed: {exc}") from exc
    finally:
        runtime_lease.release()
    try:
        response = json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        detail = (proc.stderr or proc.stdout or "empty helper response").strip()[-2000:]
        raise PB8OhlcvError(f"Invalid PB8 OHLCV preflight response: {detail}") from exc
    if proc.returncode != 0 or not response.get("ok"):
        detail = str(response.get("detail") or proc.stderr or "PB8 OHLCV preflight failed").strip()
        raise PB8OhlcvError(detail[-2000:])
    result = response.get("result")
    if not isinstance(result, dict):
        raise PB8OhlcvError("PB8 OHLCV preflight returned no result")
    return result


async def build_pb8_ohlcv_preflight(raw_config: dict[str, Any]) -> dict[str, Any]:
    """Build the shared editor payload without importing PB8 into the API."""
    return await asyncio.to_thread(_run_preflight_helper, raw_config)


def _work_dir() -> Path:
    """Return owner-only persistent PB8 preload state storage."""
    data_dir = Path(PBGDIR) / "data"
    return ensure_private_directory_tree(data_dir, data_dir / "pb8_ohlcv_preload")


def _log_dir() -> Path:
    """Return the managed preload log directory."""
    data_dir = Path(PBGDIR) / "data"
    return ensure_private_directory_tree(data_dir, data_dir / "logs" / "ohlcv-preloads")


def _config_path(job_id: str) -> Path:
    """Return the private config path for one preload job."""
    return _work_dir() / f"preload_{job_id}.json"


def _state_path(job_id: str) -> Path:
    """Return the private supervisor state path for one preload job."""
    return _work_dir() / f"preload_{job_id}.state.json"


def _log_path(job_id: str) -> Path:
    """Return the managed log path for one preload job."""
    return _log_dir() / f"pb8_preload_{job_id}.log"


def _persist_locked(job: dict[str, Any]) -> None:
    """Atomically persist the JSON-compatible portion of a job."""
    payload = {
        key: value
        for key, value in job.items()
        if isinstance(value, (str, int, float, bool, list, dict)) or value is None
    }
    state_path = Path(str(job["state_path"]))
    with advisory_file_lock(state_path):
        atomic_write_private_text(state_path, json.dumps(payload, indent=4) + "\n")


def _update_persisted_locked(job: dict[str, Any], updates: dict[str, Any]) -> None:
    """Merge API-owned fields into the latest supervisor state atomically."""
    state_path = Path(str(job["state_path"]))
    with advisory_file_lock(state_path):
        try:
            persisted = json.loads(state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, TypeError):
            persisted = {}
        if str(persisted.get("job_id") or job.get("job_id") or "") == str(job.get("job_id") or ""):
            job.update(persisted)
        job.update(updates)
        payload = {
            key: value
            for key, value in job.items()
            if isinstance(value, (str, int, float, bool, list, dict)) or value is None
        }
        atomic_write_private_text(state_path, json.dumps(payload, indent=4) + "\n")


def _finish_active_persisted_locked(job: dict[str, Any], error: str) -> bool:
    """Publish a terminal failure only if the supervisor is still active."""
    state_path = Path(str(job["state_path"]))
    with advisory_file_lock(state_path):
        try:
            persisted = json.loads(state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, TypeError):
            persisted = {}
        if str(persisted.get("job_id") or job.get("job_id") or "") == str(job.get("job_id") or ""):
            job.update(persisted)
        if str(job.get("status") or "") not in {"queued", "launching", "running"}:
            return False
        stopped = bool(job.get("stop_requested"))
        job.update(
            status="stopped" if stopped else "error",
            returncode=-1,
            finished_at=job.get("finished_at") or _utc_ms(),
        )
        if not stopped:
            job["error"] = error
        payload = {
            key: value
            for key, value in job.items()
            if isinstance(value, (str, int, float, bool, list, dict)) or value is None
        }
        atomic_write_private_text(state_path, json.dumps(payload, indent=4) + "\n")
        return True


def _refresh_locked(job: dict[str, Any]) -> None:
    """Merge authoritative supervisor state into an in-memory job."""
    state_path = Path(str(job["state_path"]))
    try:
        with advisory_file_lock(state_path):
            persisted = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError) as exc:
        _log(SERVICE, f"Failed to refresh PB8 OHLCV preload state: {exc}", level="WARNING")
        return
    if str(persisted.get("job_id") or "") == str(job.get("job_id") or ""):
        job.update(persisted)


def _process_identity(job: dict[str, Any]) -> bool | None:
    """Verify PID reuse protection before controlling a preload supervisor."""
    try:
        pid = int(job.get("pid") or 0)
    except (TypeError, ValueError):
        return False
    if pid <= 0 or not psutil.pid_exists(pid):
        return False
    try:
        process = psutil.Process(pid)
        command = [str(part) for part in process.cmdline()]
        expected_created = float(job.get("process_created_at") or 0.0)
        if expected_created and abs(process.create_time() - expected_created) > 0.01:
            return False
    except (psutil.NoSuchProcess, psutil.ZombieProcess):
        return False
    except psutil.AccessDenied:
        return None
    state_path = str(Path(str(job.get("state_path") or "")).resolve())
    return any(part.endswith("ohlcv_preload_worker.py") for part in command) and state_path in command


def _reconcile_locked(job: dict[str, Any]) -> None:
    """Convert vanished active supervisors to deterministic terminal state."""
    _refresh_locked(job)
    if str(job.get("status") or "") not in {"queued", "launching", "running"}:
        return
    identity = _process_identity(job)
    if identity is True or identity is None:
        return
    unit_state = _systemd_unit_active(job)
    if unit_state is True or (_systemd_unit_for_job(job) is not None and unit_state is None):
        return
    _finish_active_persisted_locked(job, "PB8 OHLCV preload supervisor exited without a terminal status")


def _restore_jobs_locked() -> None:
    """Restore bounded persistent jobs lazily after an API restart."""
    global _RESTORED
    if _RESTORED:
        return
    _RESTORED = True
    work_dir = _work_dir()
    for state_path in sorted(work_dir.glob("preload_*.state.json"), reverse=True)[:_MAX_JOBS]:
        try:
            job = json.loads(state_path.read_text(encoding="utf-8"))
            job_id = str(job.get("job_id") or "")
            if len(job_id) != 12 or not all(char in "0123456789abcdef" for char in job_id):
                continue
            job["state_path"] = str(state_path)
            _JOBS[job_id] = job
            _reconcile_locked(job)
        except (OSError, json.JSONDecodeError, TypeError) as exc:
            _log(SERVICE, f"Failed to restore PB8 OHLCV preload state {state_path.name}: {exc}", level="WARNING")


def _cleanup_jobs(*, reserve_slot: bool = False) -> None:
    """Bound memory and remove expired terminal job artifacts."""
    cutoff = _utc_ms() - _JOB_TTL_SECONDS * 1000
    stale_jobs = []
    with _LOCK:
        _restore_jobs_locked()
        terminal = sorted(
            (
                (int(job.get("finished_at") or 0), job_id)
                for job_id, job in _JOBS.items()
                if int(job.get("finished_at") or 0)
            )
        )
        stale_ids = {job_id for finished, job_id in terminal if finished < cutoff}
        excess = max(0, len(_JOBS) - _MAX_JOBS + (1 if reserve_slot else 0))
        stale_ids.update(job_id for _finished, job_id in terminal[:excess])
        for job_id in stale_ids:
            job = _JOBS.pop(job_id, None)
            if job:
                stale_jobs.append(job)
    for job in stale_jobs:
        for key in ("config_path", "log_path", "state_path"):
            try:
                artifact = Path(str(job.get(key) or ""))
                artifact.unlink(missing_ok=True)
                if key == "state_path":
                    artifact.with_name(f"{artifact.name}.lock").unlink(missing_ok=True)
            except OSError as exc:
                _log(SERVICE, f"Failed to remove expired PB8 preload artifact: {exc}", level="WARNING")


def _systemd_user_manager_available() -> bool:
    """Return whether a transient user unit can isolate a persistent preload."""
    if platform.system() != "Linux" or which("systemd-run") is None:
        return False
    runtime_dir = Path(os.environ.get("XDG_RUNTIME_DIR") or f"/run/user/{os.getuid()}")
    return (runtime_dir / "systemd" / "private").exists()


def _launch_supervisor(
    job_id: str,
    command: list[str],
    log_path: Path,
    *,
    systemd_unit: str | None = None,
    allow_systemd: bool = True,
) -> tuple[subprocess.Popen | None, str | None]:
    """Launch a preload supervisor outside the API cgroup when possible."""
    if allow_systemd and (systemd_unit is not None or _systemd_user_manager_available()):
        atomic_write_private_text(log_path, "")
        unit = systemd_unit or f"pbgui-pb8-ohlcv-{job_id}-{time.time_ns()}"
        runtime_dir = os.environ.get("XDG_RUNTIME_DIR") or f"/run/user/{os.getuid()}"
        result = subprocess.run(
            [
                str(which("systemd-run")),
                "--user",
                "--quiet",
                "--collect",
                f"--unit={unit}",
                "--property=Type=exec",
                "--property=UMask=0077",
                f"--property=WorkingDirectory={PBGDIR}",
                f"--property=StandardOutput=append:{log_path}",
                f"--property=StandardError=append:{log_path}",
                "--",
                *command,
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=15,
            env={**os.environ, "XDG_RUNTIME_DIR": runtime_dir},
        )
        if result.returncode != 0:
            detail = ((result.stderr or "") + (result.stdout or "")).strip()
            raise OSError(f"Could not start persistent PB8 OHLCV preload unit: {detail or result.returncode}")
        return None, unit
    popen_kwargs: dict[str, Any] = {
        "cwd": str(PBGDIR),
        "stdout": subprocess.DEVNULL,
        "stderr": subprocess.DEVNULL,
    }
    if platform.system() == "Windows":
        popen_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS
    else:
        popen_kwargs["start_new_session"] = True
    return subprocess.Popen(command, **popen_kwargs), None


def _systemd_unit_for_job(job: dict[str, Any]) -> str | None:
    """Return only a PBGui-generated unit belonging to this exact job."""
    job_id = str(job.get("job_id") or "")
    unit = str(job.get("systemd_unit") or "")
    prefix = f"pbgui-pb8-ohlcv-{job_id}-"
    if not unit.startswith(prefix):
        return None
    suffix = unit[len(prefix):]
    return unit if len(job_id) == 12 and suffix.isdigit() else None


def _systemd_unit_active(job: dict[str, Any]) -> bool | None:
    """Return whether a validated preload unit is active or still activating."""
    unit = _systemd_unit_for_job(job)
    systemctl = which("systemctl")
    if unit is None:
        return False
    if systemctl is None:
        return None
    runtime_dir = os.environ.get("XDG_RUNTIME_DIR") or f"/run/user/{os.getuid()}"
    try:
        result = subprocess.run(
            [str(systemctl), "--user", "show", unit, "--property=ActiveState", "--value"],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
            env={**os.environ, "XDG_RUNTIME_DIR": runtime_dir},
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    state = str(result.stdout or "").strip().lower()
    if state in {"active", "activating", "reloading"}:
        return True
    if state in {"inactive", "failed", "deactivating"}:
        return False
    return None


def _control_systemd_unit(job: dict[str, Any], action: str) -> bool:
    """Stop or kill one validated PBGui preload unit."""
    unit = _systemd_unit_for_job(job)
    systemctl = which("systemctl")
    if unit is None or systemctl is None or action not in {"stop", "kill"}:
        return False
    args = [str(systemctl), "--user", action]
    if action == "kill":
        args.append("--signal=SIGKILL")
    args.append(unit)
    runtime_dir = os.environ.get("XDG_RUNTIME_DIR") or f"/run/user/{os.getuid()}"
    try:
        result = subprocess.run(
            args,
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
            env={**os.environ, "XDG_RUNTIME_DIR": runtime_dir},
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        _log(SERVICE, f"Failed to {action} PB8 OHLCV preload unit {unit}: {exc}", level="WARNING")
        return False
    return result.returncode == 0


def _spawn_worker(job_id: str) -> None:
    """Launch PBGui's detached supervisor for one PB8 native command."""
    use_systemd = _systemd_user_manager_available()
    systemd_unit = f"pbgui-pb8-ohlcv-{job_id}-{time.time_ns()}" if use_systemd else None
    with _LOCK:
        job = _JOBS.get(job_id)
        if job is None:
            raise PB8OhlcvError("PB8 OHLCV preload job disappeared before launch")
        job["status"] = "launching"
        if systemd_unit is not None:
            job["systemd_unit"] = systemd_unit
        _persist_locked(job)
        state_path = Path(str(job["state_path"])).resolve()
    worker = Path(__file__).resolve().with_name("ohlcv_preload_worker.py")
    supervisor_command = [sys.executable, "-u", str(worker), str(state_path)]
    try:
        proc, _launched_unit = _launch_supervisor(
            job_id,
            supervisor_command,
            Path(str(job["log_path"])),
            systemd_unit=systemd_unit,
            allow_systemd=use_systemd,
        )
    except (OSError, psutil.Error) as exc:
        with _LOCK:
            current = _JOBS.get(job_id)
            if current is not None:
                current.update(status="error", error=str(exc), finished_at=_utc_ms(), returncode=-1)
                _persist_locked(current)
        raise PB8OhlcvUnavailableError(f"Failed to start PB8 OHLCV preload: {exc}") from exc
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        with _LOCK:
            current = _JOBS.get(job_id)
            if current is None:
                break
            _refresh_locked(current)
            if current.get("pid") and not current.get("process_created_at"):
                try:
                    created_at = psutil.Process(int(current["pid"])).create_time()
                    _update_persisted_locked(current, {"process_created_at": created_at})
                except (psutil.NoSuchProcess, psutil.ZombieProcess, psutil.AccessDenied, OSError, ValueError, TypeError):
                    pass
            if current.get("child_pid") or str(current.get("status") or "") in {"completed", "stopped", "error"}:
                break
        if proc is not None and proc.poll() is not None:
            break
        time.sleep(0.05)
    else:
        try:
            if systemd_unit is not None:
                with _LOCK:
                    current = _JOBS.get(job_id)
                    if current is not None:
                        _control_systemd_unit(current, "stop")
            elif platform.system() == "Windows":
                proc.terminate()
            else:
                with _LOCK:
                    current = _JOBS.get(job_id)
                    verified = current is not None and _process_identity(current) is True
                if verified:
                    os.killpg(proc.pid, signal.SIGTERM)
        except (OSError, psutil.Error):
            pass
        try:
            if proc is not None:
                proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            if proc is not None:
                proc.kill()
                proc.wait(timeout=2)
        with _LOCK:
            current = _JOBS.get(job_id)
            if current is not None:
                _finish_active_persisted_locked(current, "PB8 OHLCV preload did not start within 5 seconds")
        raise PB8OhlcvUnavailableError("PB8 OHLCV preload did not start within 5 seconds")

    if proc is None:
        return

    def reap() -> None:
        """Reap the detached supervisor and reconcile an incomplete exit."""
        returncode = proc.wait()
        with _LOCK:
            current = _JOBS.get(job_id)
            if current is not None:
                _refresh_locked(current)
                if str(current.get("status") or "") in {"launching", "running"}:
                    current.update(
                        status="stopped" if current.get("stop_requested") else "error",
                        returncode=returncode,
                        finished_at=_utc_ms(),
                    )
                    if not current.get("stop_requested"):
                        current["error"] = f"PB8 OHLCV preload supervisor exited with code {returncode}"
                    _persist_locked(current)
            _REAPERS.pop(job_id, None)

    reaper = threading.Thread(target=reap, daemon=True, name=f"pb8-ohlcv-preload-{job_id}")
    with _LOCK:
        _REAPERS[job_id] = reaper
    reaper.start()


def _job_payload(job: dict[str, Any]) -> dict[str, Any]:
    """Return the frontend-compatible status payload for one PB8 job."""
    log_path = Path(str(job["log_path"]))
    tail: list[str] = []
    line_count = 0
    updated_at = None
    size_bytes = 0
    try:
        if log_path.is_file() and not log_path.is_symlink():
            recent = deque(maxlen=40)
            with log_path.open("r", encoding="utf-8", errors="replace") as handle:
                for line in handle:
                    recent.append(line.rstrip("\n"))
                    line_count += 1
            tail = list(recent)
            stat = log_path.stat()
            updated_at = int(stat.st_mtime * 1000)
            size_bytes = int(stat.st_size)
    except OSError as exc:
        _log(SERVICE, f"Failed to read PB8 OHLCV preload log: {exc}", level="WARNING")
    return {
        "job_id": job["job_id"],
        "status": job["status"],
        "started_at": job.get("started_at"),
        "started_at_iso": _iso_from_ms(job.get("started_at")),
        "finished_at": job.get("finished_at"),
        "finished_at_iso": _iso_from_ms(job.get("finished_at")),
        "pid": job.get("pid"),
        "returncode": job.get("returncode"),
        "error": job.get("error"),
        "log_path": str(log_path),
        "target_end_ms": job.get("target_end_ms"),
        "target_end_iso": _iso_from_ms(job.get("target_end_ms")),
        "log_tail": tail,
        "log_line_count": line_count,
        "last_log_line": next((line for line in reversed(tail) if line.strip()), None),
        "log_updated_at": updated_at,
        "log_updated_at_iso": _iso_from_ms(updated_at),
        "log_size_bytes": size_bytes,
        "progress": {
            "tracker": None,
            "observed_tasks": 0,
            "active_tasks": 0,
            "finished_tasks": 0,
            "current_task": None,
            "tasks": [],
        },
    }


def start_pb8_ohlcv_preload_job(raw_config: dict[str, Any]) -> dict[str, Any]:
    """Persist and launch PB8's supported native OHLCV preparation command."""
    _cleanup_jobs(reserve_slot=True)
    with _LOCK:
        if len(_JOBS) >= _MAX_JOBS:
            raise PB8OhlcvUnavailableError(
                "The PB8 OHLCV preload job limit is reached. Stop or wait for an active preload and retry."
            )
    runtime_lease = _acquire_runtime_lease()
    try:
        status = _runtime()
        config, source_dir, _catalog = resolve_pb8_ohlcv_paths(raw_config, status)
        if source_dir is not None and not source_dir.is_dir():
            raise PB8OhlcvUnsupportedError(
                f"PB8 explicit OHLCV source is not ready: {source_dir}. Populate it with PBGui Market Data first."
            )
        config.pop("pbgui", None)
        job_id = uuid.uuid4().hex[:12]
        config_path = _config_path(job_id)
        state_path = _state_path(job_id)
        log_path = _log_path(job_id)
        atomic_write_private_text(config_path, json.dumps(config, indent=4) + "\n")
        job = {
            "job_id": job_id,
            "runtime": "pb8",
            "status": "queued",
            "started_at": _utc_ms(),
            "finished_at": None,
            "pid": None,
            "process_created_at": None,
            "returncode": None,
            "error": None,
            "command": [status["cli_file"], "download", str(config_path)],
            "cwd": status["pb8dir"],
            "config_path": str(config_path),
            "log_path": str(log_path),
            "state_path": str(state_path),
            "stop_requested": False,
            "target_end_ms": None,
        }
        with _LOCK:
            _JOBS[job_id] = job
            _persist_locked(job)
        _spawn_worker(job_id)
    finally:
        runtime_lease.release()
    with _LOCK:
        return _job_payload(copy.deepcopy(_JOBS[job_id]))


def get_pb8_ohlcv_preload_job(job_id: str) -> dict[str, Any] | None:
    """Return one PB8 preload job, reconciling detached supervisor state."""
    _cleanup_jobs()
    with _LOCK:
        job = _JOBS.get(str(job_id))
        if job is not None:
            _reconcile_locked(job)
            return _job_payload(copy.deepcopy(job))
    return None


def stop_pb8_ohlcv_preload_job(job_id: str) -> dict[str, Any] | None:
    """Stop only a supervisor whose exact PBGui-owned identity still matches."""
    _cleanup_jobs()
    with _LOCK:
        job = _JOBS.get(str(job_id))
        if job is None:
            return None
        _refresh_locked(job)
        _update_persisted_locked(job, {"stop_requested": True})
        status = str(job.get("status") or "")
        pid = int(job.get("pid") or 0)
        identity = _process_identity(job) if pid else False
        systemd_unit = _systemd_unit_for_job(job)
    termination_requested = False
    active = status in {"queued", "launching", "running"}
    if active and systemd_unit is not None:
        termination_requested = _control_systemd_unit(job, "stop")
    elif active and pid and identity is True:
        try:
            if platform.system() == "Windows":
                os.kill(pid, signal.SIGTERM)
                termination_requested = True
            else:
                os.killpg(pid, signal.SIGTERM)
                termination_requested = True
        except ProcessLookupError:
            pass
        except OSError as exc:
            _log(SERVICE, f"Failed to stop PB8 OHLCV preload {job_id}: {exc}", level="WARNING")
    elif active and identity is None:
        _log(SERVICE, f"Refused to signal unverified PB8 OHLCV preload PID {pid}", level="WARNING")
    if active and (systemd_unit is not None or (pid and identity is True)) and termination_requested:
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            with _LOCK:
                current = _JOBS.get(str(job_id))
                if current is None:
                    break
                _refresh_locked(current)
                if str(current.get("status") or "") in {"completed", "stopped", "error"}:
                    break
                current_unit = _systemd_unit_for_job(current)
            if current_unit is not None:
                if _systemd_unit_active(current) is not True:
                    break
            elif not psutil.pid_exists(pid):
                break
            time.sleep(0.05)
        else:
            with _LOCK:
                current = _JOBS.get(str(job_id))
                current_unit = _systemd_unit_for_job(current) if current is not None else None
                verified = current is not None and _process_identity(current) is True
            if current_unit is not None:
                termination_requested = _control_systemd_unit(current, "kill") or termination_requested
            elif verified:
                try:
                    if platform.system() == "Windows":
                        os.kill(pid, signal.SIGKILL)
                        termination_requested = True
                    else:
                        os.killpg(pid, signal.SIGKILL)
                        termination_requested = True
                except ProcessLookupError:
                    pass
                except OSError as exc:
                    _log(SERVICE, f"Failed to force-stop PB8 OHLCV preload {job_id}: {exc}", level="WARNING")
    with _LOCK:
        current = _JOBS.get(str(job_id))
        if current is None:
            return None
        _refresh_locked(current)
        current_status = str(current.get("status") or "")
        current_unit = _systemd_unit_for_job(current)
        if current_status in {"queued", "launching", "running"}:
            if current_unit is not None:
                stopped_safely = termination_requested and _systemd_unit_active(current) is not True
            elif pid:
                stopped_safely = termination_requested and not psutil.pid_exists(pid)
            else:
                stopped_safely = current_status == "queued"
            if stopped_safely:
                _finish_active_persisted_locked(current, "PB8 OHLCV preload stopped")
        return _job_payload(copy.deepcopy(current))
