"""PBGui-side client for isolated Passivbot V8 Strategy Explorer operations."""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
import json
import subprocess
import threading
import time
import traceback
import uuid
from pathlib import Path
from typing import Any

from master_update_lock import MasterUpdateBusyError, acquire_master_runtime_lock
from pbgui_purefunc import PBGDIR, pb8_runtime_status
from pb8_config import PB8ConfigurationError, PB8RuntimeBusyError, _runtime_fingerprint
from logging_helpers import human_log as _log

SERVICE = "StrategyExplorer"


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


class PB8StrategyExplorerUnavailableError(PB8StrategyExplorerError):
    """Raised when the configured PB8 runtime is unavailable."""

    status_code = 503


BusyError = PB8StrategyExplorerBusyError
CancelledError = PB8StrategyExplorerCancelledError

_MAX_REQUEST_BYTES = 2 * 1024 * 1024
_MAX_RESPONSE_BYTES = 32 * 1024 * 1024
_MAX_TIMEOUT_SECONDS = 300.0
_HELPER_SLOTS = threading.BoundedSemaphore(2)
# Sync API routes share AnyIO's normally 40-thread pool. Bound helpers AND
# singleflight followers together; separate budgets could occupy 32 threads.
_ADMISSION_SLOTS = threading.BoundedSemaphore(16)
_SLOT_WAIT_SECONDS = 15.0
_WAIT_INTERVAL_SECONDS = 0.1
_CAPABILITIES_TTL_SECONDS = 30.0
_CAPABILITIES_FLIGHT_SECONDS = 79.0  # slot wait + execution + terminate/kill
_PROCESS_LOCK = threading.RLock()
_CAPABILITIES_CONDITION = threading.Condition(_PROCESS_LOCK)
_CAPABILITIES_CACHE: tuple[float, tuple, dict[str, Any]] | None = None
_LIFECYCLE = 0


@dataclass
class _CapabilitiesFlight:
    """One bounded calculation shared by all concurrently admitted followers."""

    fingerprint: tuple
    lifecycle: int
    deadline: float
    done: bool = False
    result: dict[str, Any] = field(default_factory=dict)
    error: BaseException | None = None


_CAPABILITIES_FLIGHT: _CapabilitiesFlight | None = None
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
        raise PB8StrategyExplorerUnavailableError(detail or "PB8 runtime is not ready")
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


def _cleanup_process(proc: subprocess.Popen) -> None:
    """Stop an owned process and close every pipe even if another cleanup fails."""
    try:
        _stop_process(proc)
    except Exception:
        _log(SERVICE, "Could not stop Strategy Explorer helper", level="ERROR",
             meta={"traceback": traceback.format_exc()})
    finally:
        for name in ("stdin", "stdout", "stderr"):
            pipe = getattr(proc, name, None)
            if not callable(getattr(pipe, "close", None)):
                continue
            try:
                pipe.close()
            except Exception:
                _log(SERVICE, "Could not close Strategy Explorer helper pipe", level="ERROR",
                     meta={"traceback": traceback.format_exc()})


def shutdown() -> None:
    """Invalidate flights and stop only API-owned helpers, including partial launches."""
    global _SHUTTING_DOWN, _LIFECYCLE, _CAPABILITIES_CACHE, _CAPABILITIES_FLIGHT
    with _CAPABILITIES_CONDITION:
        _SHUTTING_DOWN = True
        _LIFECYCLE += 1
        _CAPABILITIES_CACHE = None
        if _CAPABILITIES_FLIGHT is not None:
            _CAPABILITIES_FLIGHT.error = PB8StrategyExplorerBusyError("Strategy Explorer is shutting down")
            _CAPABILITIES_FLIGHT.done = True
            _CAPABILITIES_FLIGHT = None
        _CAPABILITIES_CONDITION.notify_all()
        owned = list(_PROCESSES.items())
        _CANCELLED.update(_PENDING)
        _CANCELLED.update(operation_id for operation_id, _proc in owned)
    for _operation_id, proc in owned:
        _cleanup_process(proc)
    with _PROCESS_LOCK:
        for operation_id, proc in owned:
            if _PROCESSES.get(operation_id) is proc:
                _PROCESSES.pop(operation_id, None)
        _CANCELLED.intersection_update(_PENDING)


def _check_operation(op_id: str, lifecycle: int) -> None:
    """Reject cancelled or previous-lifecycle work while holding the process lock."""
    if _SHUTTING_DOWN or lifecycle != _LIFECYCLE or op_id in _CANCELLED:
        raise PB8StrategyExplorerCancelledError("Strategy Explorer operation was cancelled")


def _acquire_helper_slot(operation: str, op_id: str, lifecycle: int) -> None:
    """Wait briefly only for bootstrap operations, without holding a runtime lease."""
    deadline = time.monotonic() + (_SLOT_WAIT_SECONDS if operation in {"capabilities", "snapshot", "markets"} else 0)
    while True:
        with _PROCESS_LOCK:
            _check_operation(op_id, lifecycle)
        if _HELPER_SLOTS.acquire(blocking=False):
            return
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise PB8StrategyExplorerBusyError("Two PB8 Strategy Explorer operations are already running; retry shortly")
        # Cancellation/shutdown are rechecked after each bounded wait.
        time.sleep(min(_WAIT_INTERVAL_SECONDS, remaining))


def _call_helper(
    operation: str,
    payload: dict[str, Any] | None = None,
    *,
    operation_id: str | None = None,
    timeout: float = 120.0,
    _admitted: bool = False,
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
    if not _admitted and not _ADMISSION_SLOTS.acquire(blocking=False):
        raise PB8StrategyExplorerBusyError("Strategy Explorer request limit reached; retry shortly")

    slot_acquired = False
    registered = False
    lease = None
    proc: subprocess.Popen | None = None
    try:
        with _PROCESS_LOCK:
            if _SHUTTING_DOWN:
                raise PB8StrategyExplorerBusyError("Strategy Explorer is shutting down")
            if op_id in _PROCESSES or op_id in _PENDING:
                raise PB8StrategyExplorerBusyError("Strategy Explorer operation id is already running")
            _PENDING.add(op_id)
            registered = True
            lifecycle = _LIFECYCLE
        _acquire_helper_slot(operation, op_id, lifecycle)
        slot_acquired = True
        with _PROCESS_LOCK:
            _check_operation(op_id, lifecycle)
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
            _check_operation(op_id, lifecycle)
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
            _check_operation(op_id, lifecycle)
            _PROCESSES[op_id] = proc
        try:
            stdout, stderr = proc.communicate(input=request_bytes, timeout=timeout_value)
        except subprocess.TimeoutExpired as exc:
            raise PB8StrategyExplorerError(
                f"PB8 Strategy Explorer {operation} timed out after {timeout_value:g} seconds"
            ) from exc
        except (OSError, ValueError):
            # Shutdown may close a pipe while communicate is returning. Preserve
            # the cancellation status instead of surfacing a spurious HTTP 500.
            with _PROCESS_LOCK:
                _check_operation(op_id, lifecycle)
            raise
        with _PROCESS_LOCK:
            _check_operation(op_id, lifecycle)
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
        try:
            if proc is not None:
                _cleanup_process(proc)
        finally:
            with _PROCESS_LOCK:
                if registered:
                    if _PROCESSES.get(op_id) is proc:
                        _PROCESSES.pop(op_id, None)
                    _PENDING.discard(op_id)
                    _CANCELLED.discard(op_id)
            try:
                if lease is not None:
                    lease.release()
            finally:
                try:
                    if slot_acquired:
                        _HELPER_SLOTS.release()
                finally:
                    if not _admitted:
                        _ADMISSION_SLOTS.release()


def _capabilities_fingerprint() -> tuple:
    """Check readiness and identify PB8/helper code outside all Explorer locks."""
    status = _runtime()
    try:
        fingerprint = _runtime_fingerprint(status)
    except PB8RuntimeBusyError as exc:
        raise PB8StrategyExplorerBusyError(str(exc)) from exc
    except PB8ConfigurationError as exc:
        raise PB8StrategyExplorerUnavailableError(str(exc)) from exc
    helper = Path(__file__).resolve().with_name("pb8_strategy_explorer_helper.py")
    try:
        stat = helper.stat()
    except OSError as exc:
        raise PB8StrategyExplorerUnavailableError("Strategy Explorer helper is unavailable") from exc
    return fingerprint, (stat.st_mtime_ns, stat.st_size)


def capabilities(config: dict | None = None, *, operation_id: str | None = None) -> dict[str, Any]:
    """Cache only default capabilities, sharing one bounded cold calculation."""
    global _CAPABILITIES_CACHE, _CAPABILITIES_FLIGHT
    if config is not None or operation_id is not None:
        payload = {"config": config} if isinstance(config, dict) else {}
        return _call_helper("capabilities", payload, operation_id=operation_id, timeout=60)

    # Fingerprinting itself may wait on PB8Config's shared scan lock. Admit the
    # whole call before that wait, then keep the same permit for a leader/follower.
    # Even warm hits briefly reserve capacity and release it before returning.
    if not _ADMISSION_SLOTS.acquire(blocking=False):
        raise PB8StrategyExplorerBusyError("Strategy Explorer request limit reached; retry shortly")
    try:
        # A source/site-packages scan must never hold Explorer cancellation locks.
        with _PROCESS_LOCK:
            lifecycle = _LIFECYCLE
            if _SHUTTING_DOWN:
                raise PB8StrategyExplorerBusyError("Strategy Explorer is shutting down")
        fingerprint = _capabilities_fingerprint()
        with _CAPABILITIES_CONDITION:
            if _SHUTTING_DOWN or lifecycle != _LIFECYCLE:
                raise PB8StrategyExplorerBusyError("Strategy Explorer lifecycle changed; retry shortly")
            now = time.monotonic()
            if _CAPABILITIES_CACHE is not None:
                expires, cached_fingerprint, cached = _CAPABILITIES_CACHE
                if expires > now and cached_fingerprint == fingerprint:
                    return copy.deepcopy(cached)
            flight = _CAPABILITIES_FLIGHT
            if flight is None:
                flight = _CapabilitiesFlight(fingerprint, lifecycle, now + _CAPABILITIES_FLIGHT_SECONDS)
                _CAPABILITIES_FLIGHT = flight
            else:
                while not flight.done:
                    remaining = flight.deadline - time.monotonic()
                    if remaining <= 0:
                        raise PB8StrategyExplorerBusyError("Strategy Explorer capabilities are still loading; retry shortly")
                    _CAPABILITIES_CONDITION.wait(timeout=remaining)
                if flight.error is not None:
                    raise flight.error
                if flight.fingerprint != fingerprint or flight.lifecycle != lifecycle:
                    raise PB8StrategyExplorerBusyError("PB8 runtime changed; retry shortly")
                return copy.deepcopy(flight.result)

        try:
            result = _call_helper("capabilities", {}, timeout=60, _admitted=True)
            final_fingerprint = _capabilities_fingerprint()
            if final_fingerprint != fingerprint:
                raise PB8StrategyExplorerBusyError("PB8 runtime changed while loading capabilities; retry shortly")
            with _CAPABILITIES_CONDITION:
                if _SHUTTING_DOWN or lifecycle != _LIFECYCLE or flight.done or time.monotonic() >= flight.deadline:
                    raise PB8StrategyExplorerBusyError("Strategy Explorer lifecycle changed; retry shortly")
                flight.result = copy.deepcopy(result)
                _CAPABILITIES_CACHE = (time.monotonic() + _CAPABILITIES_TTL_SECONDS, fingerprint, copy.deepcopy(result))
            return result
        except BaseException as exc:
            with _CAPABILITIES_CONDITION:
                if not flight.done:
                    flight.error = exc
            raise
        finally:
            with _CAPABILITIES_CONDITION:
                flight.done = True
                if _CAPABILITIES_FLIGHT is flight:
                    _CAPABILITIES_FLIGHT = None
                _CAPABILITIES_CONDITION.notify_all()
    finally:
        _ADMISSION_SLOTS.release()


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
