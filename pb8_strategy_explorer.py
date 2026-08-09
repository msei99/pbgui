"""PBGui-side client for isolated Passivbot V8 Strategy Explorer operations."""

from __future__ import annotations

import json
import subprocess
import threading
import uuid
from pathlib import Path
from typing import Any

from master_update_lock import MasterUpdateBusyError, acquire_master_runtime_lock
from pbgui_purefunc import PBGDIR, pb8_runtime_status


class PB8StrategyExplorerError(RuntimeError):
    """Raised when an isolated PB8 Strategy Explorer operation fails."""

    status_code = 422


class PB8StrategyExplorerBusyError(PB8StrategyExplorerError):
    """Raised when PB8 or all Strategy Explorer helper slots are busy."""

    retryable = True
    status_code = 503


class PB8StrategyExplorerCancelledError(PB8StrategyExplorerError):
    """Raised when the owner cancels a running Strategy Explorer helper."""

    status_code = 409


BusyError = PB8StrategyExplorerBusyError
CancelledError = PB8StrategyExplorerCancelledError

_MAX_REQUEST_BYTES = 2 * 1024 * 1024
_MAX_RESPONSE_BYTES = 32 * 1024 * 1024
_MAX_TIMEOUT_SECONDS = 300.0
_HELPER_SLOTS = threading.BoundedSemaphore(2)
_PROCESS_LOCK = threading.RLock()
_PROCESSES: dict[str, subprocess.Popen] = {}
_PENDING: set[str] = set()
_CANCELLED: set[str] = set()
_SHUTTING_DOWN = False


def _detail(value: object) -> str:
    """Return a bounded single-operation diagnostic string."""
    return str(value or "").strip()[-2000:]


def _runtime() -> dict[str, Any]:
    """Return a ready PB8 runtime without importing PB8 modules."""
    status = pb8_runtime_status()
    if not status.get("ready"):
        detail = "; ".join(str(item) for item in status.get("errors") or [])
        raise PB8StrategyExplorerError(detail or "PB8 runtime is not ready")
    return status


def _stop_process(proc: subprocess.Popen) -> None:
    """Terminate one helper and escalate only if it does not exit promptly."""
    if proc.poll() is not None:
        return
    try:
        proc.terminate()
        proc.wait(timeout=2)
    except subprocess.TimeoutExpired:
        proc.kill()
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            pass
    except ProcessLookupError:
        pass


def cancel(operation_id: str) -> bool:
    """Cancel only the helper currently registered under ``operation_id``."""
    key = str(operation_id or "").strip()
    if not key:
        return False
    with _PROCESS_LOCK:
        proc = _PROCESSES.get(key)
        if proc is None and key not in _PENDING:
            return False
        _CANCELLED.add(key)
    if proc is not None:
        _stop_process(proc)
    return True


def startup() -> None:
    """Allow helper launches after the API lifespan has started."""
    global _SHUTTING_DOWN
    with _PROCESS_LOCK:
        _SHUTTING_DOWN = False


def shutdown() -> None:
    """Deterministically stop all API-owned helper processes; repeated calls are safe."""
    global _SHUTTING_DOWN
    with _PROCESS_LOCK:
        _SHUTTING_DOWN = True
        owned = list(_PROCESSES.items())
        _CANCELLED.update(_PENDING)
        _CANCELLED.update(operation_id for operation_id, _proc in owned)
    for _operation_id, proc in owned:
        _stop_process(proc)
    with _PROCESS_LOCK:
        _PROCESSES.clear()
        _CANCELLED.intersection_update(_PENDING)


def _call_helper(
    operation: str,
    payload: dict[str, Any] | None = None,
    *,
    operation_id: str | None = None,
    timeout: float = 120.0,
) -> dict[str, Any]:
    """Execute one bounded request in PB8's configured interpreter and directory."""
    op_id = str(operation_id or uuid.uuid4().hex)
    if not op_id or len(op_id) > 128 or any(ord(char) < 33 or ord(char) > 126 for char in op_id):
        raise PB8StrategyExplorerError("Invalid Strategy Explorer operation id")
    try:
        timeout_value = float(timeout)
    except (TypeError, ValueError):
        timeout_value = 120.0
    timeout_value = max(1.0, min(_MAX_TIMEOUT_SECONDS, timeout_value))
    request = {"operation": str(operation), **dict(payload or {})}
    try:
        request_bytes = json.dumps(request, separators=(",", ":"), allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise PB8StrategyExplorerError(f"Strategy Explorer request is not valid JSON: {_detail(exc)}") from exc
    if len(request_bytes) > _MAX_REQUEST_BYTES:
        raise PB8StrategyExplorerError("Strategy Explorer request exceeds the 2 MiB limit")
    if not _HELPER_SLOTS.acquire(blocking=False):
        raise PB8StrategyExplorerBusyError("Two PB8 Strategy Explorer operations are already running")

    lease = None
    proc: subprocess.Popen | None = None
    try:
        with _PROCESS_LOCK:
            if _SHUTTING_DOWN:
                raise PB8StrategyExplorerBusyError("Strategy Explorer is shutting down")
            if op_id in _PROCESSES or op_id in _PENDING:
                raise PB8StrategyExplorerBusyError("Strategy Explorer operation id is already running")
            _PENDING.add(op_id)
        try:
            lease = acquire_master_runtime_lock(Path(PBGDIR))
        except MasterUpdateBusyError as exc:
            raise PB8StrategyExplorerBusyError(
                "PB8 is being installed or updated. Retry when the update finishes."
            ) from exc
        status = _runtime()
        helper = Path(__file__).resolve().with_name("pb8_strategy_explorer_helper.py")
        request["pb8_dir"] = str(status["pb8dir"])
        request_bytes = json.dumps(request, separators=(",", ":"), allow_nan=False).encode("utf-8")
        if len(request_bytes) > _MAX_REQUEST_BYTES:
            raise PB8StrategyExplorerError("Strategy Explorer request exceeds the 2 MiB limit")
        with _PROCESS_LOCK:
            if _SHUTTING_DOWN or op_id in _CANCELLED:
                raise PB8StrategyExplorerCancelledError("Strategy Explorer operation was cancelled")
        try:
            proc = subprocess.Popen(
                [str(status["pb8venv"]), str(helper)],
                cwd=str(status["pb8dir"]),
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                shell=False,
            )
        except OSError as exc:
            raise PB8StrategyExplorerError(f"Could not start PB8 Strategy Explorer helper: {_detail(exc)}") from exc
        with _PROCESS_LOCK:
            if _SHUTTING_DOWN or op_id in _CANCELLED:
                _stop_process(proc)
                raise PB8StrategyExplorerCancelledError("Strategy Explorer operation was cancelled")
            _PROCESSES[op_id] = proc
        try:
            stdout, stderr = proc.communicate(input=request_bytes, timeout=timeout_value)
        except subprocess.TimeoutExpired as exc:
            _stop_process(proc)
            raise PB8StrategyExplorerError(
                f"PB8 Strategy Explorer {operation} timed out after {timeout_value:g} seconds"
            ) from exc
        with _PROCESS_LOCK:
            was_cancelled = op_id in _CANCELLED
        if was_cancelled:
            raise PB8StrategyExplorerCancelledError("PB8 Strategy Explorer operation was cancelled")
        if len(stdout or b"") > _MAX_RESPONSE_BYTES:
            raise PB8StrategyExplorerError("PB8 Strategy Explorer response exceeds the 32 MiB limit")
        try:
            response = json.loads((stdout or b"").decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            diagnostic = _detail((stderr or stdout or b"empty helper response").decode("utf-8", errors="replace"))
            raise PB8StrategyExplorerError(f"Invalid PB8 Strategy Explorer response: {diagnostic}") from exc
        if not isinstance(response, dict):
            raise PB8StrategyExplorerError("PB8 Strategy Explorer helper returned a non-object response")
        if proc.returncode != 0 or not response.get("ok"):
            diagnostic = _detail(response.get("detail") or (stderr or b"").decode("utf-8", errors="replace"))
            raise PB8StrategyExplorerError(diagnostic or "PB8 Strategy Explorer operation failed")
        result = response.get("result")
        if not isinstance(result, dict):
            raise PB8StrategyExplorerError("PB8 Strategy Explorer helper returned no result")
        return result
    finally:
        with _PROCESS_LOCK:
            if _PROCESSES.get(op_id) is proc:
                _PROCESSES.pop(op_id, None)
            _PENDING.discard(op_id)
            _CANCELLED.discard(op_id)
        if lease is not None:
            lease.release()
        _HELPER_SLOTS.release()


def capabilities(config: dict | None = None, *, operation_id: str | None = None) -> dict[str, Any]:
    """Return PB8 Strategy Explorer capabilities and dynamic strategy metadata."""
    payload = {"config": config} if isinstance(config, dict) else {}
    return _call_helper("capabilities", payload, operation_id=operation_id, timeout=60)


def markets(config: dict, options: dict | None = None, *, operation_id: str | None = None) -> dict[str, Any]:
    """Return locally discoverable PB8 exchanges and approved coins."""
    return _call_helper("markets", {"config": config, "options": options or {}}, operation_id=operation_id, timeout=60)


def snapshot(config: dict, options: dict | None = None, *, operation_id: str | None = None) -> dict[str, Any]:
    """Compute a native PB8 ideal-order snapshot."""
    return _call_helper("snapshot", {"config": config, "options": options or {}}, operation_id=operation_id, timeout=120)


def replay(config: dict, options: dict | None = None, *, operation_id: str | None = None) -> dict[str, Any]:
    """Run a bounded native PB8 replay."""
    return _call_helper("replay", {"config": config, "options": options or {}}, operation_id=operation_id, timeout=300)


def compare(
    config: dict,
    options: dict | None = None,
    *,
    result_path: str = "",
    compare_config: dict | None = None,
    operation_id: str | None = None,
) -> dict[str, Any]:
    """Compare a native replay with a validated result or another native replay."""
    payload: dict[str, Any] = {"config": config, "options": options or {}}
    if result_path:
        payload["result_path"] = result_path
    if isinstance(compare_config, dict):
        payload["compare_config"] = compare_config
    return _call_helper("compare", payload, operation_id=operation_id, timeout=300)


def movie(config: dict, options: dict | None = None, *, operation_id: str | None = None) -> dict[str, Any]:
    """Build bounded movie frames from native PB8 candles and fills."""
    return _call_helper("movie", {"config": config, "options": options or {}}, operation_id=operation_id, timeout=300)


__all__ = [
    "PB8StrategyExplorerError",
    "PB8StrategyExplorerBusyError",
    "PB8StrategyExplorerCancelledError",
    "BusyError",
    "CancelledError",
    "startup",
    "capabilities",
    "markets",
    "snapshot",
    "replay",
    "compare",
    "movie",
    "cancel",
    "shutdown",
]
