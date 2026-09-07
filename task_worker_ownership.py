"""Process-safe ownership and startup coordination for the resident task worker."""

from __future__ import annotations

from contextlib import AbstractContextManager
from dataclasses import asdict, dataclass
import json
import errno
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from typing import Any, Callable
from uuid import uuid4

import psutil

from file_lock import advisory_file_lock
from process_identity import ProcessIdentity, signal_exact_process
from secure_files import atomic_write_private_text

try:
    import fcntl
except ImportError:  # pragma: no cover - PBGui production targets Linux
    fcntl = None


SERVICE = "TaskWorkerOwnership"
OWNER_ENV = "PBGUI_TASK_WORKER_OWNER_ID"
STARTUP_TIMEOUT_S = 5.0
OWNER_VERSION = 1
SUPPRESSION_VERSION = 1


class TaskWorkerStartupError(RuntimeError):
    """Raised when a resident worker does not acknowledge startup in time."""


class TaskWorkerStopTimeout(RuntimeError):
    """Raised when an owned resident worker does not stop in time."""


class TaskWorkerSuppressedError(RuntimeError):
    """Raised when automatic startup is disabled by a durable manual stop."""


class TaskWorkerInspectionError(RuntimeError):
    """Raised when live worker ownership cannot be inspected safely."""


@dataclass(frozen=True)
class WorkerOwner:
    """Persisted identity for one resident task-worker process."""

    version: int
    owner_id: str
    pid: int
    create_time: float
    executable: str
    script: str
    state: str
    heartbeat_ts: float


@dataclass(frozen=True)
class WorkerStatus:
    """Validated resident worker state."""

    running: bool
    process_alive: bool
    pid: int | None
    owner: WorkerOwner | None
    reason: str = ""
    suppressed: bool = False
    suppression_generation: int = 0
    inspection_uncertain: bool = False


@dataclass(frozen=True)
class WorkerStartResult:
    """Result of a coordinated resident-worker start."""

    status: WorkerStatus
    spawned: bool


@dataclass(frozen=True)
class WorkerSuppression:
    """Persisted generation controlling automatic resident-worker startup."""

    version: int
    generation: int
    suppressed: bool
    updated_ts: float


@dataclass(frozen=True)
class _ProcessIdentity:
    pid: int
    create_time: float
    executable: str
    cmdline: tuple[str, ...]
    cwd: str


class WorkerLifetimeLease(AbstractContextManager["WorkerLifetimeLease"]):
    """Nonblocking process-lifetime singleton lease."""

    def __init__(self, lock_path: Path, handle: Any) -> None:
        self.lock_path = Path(lock_path)
        self._handle = handle

    def release(self) -> None:
        """Release the lifetime lease once."""
        if self._handle is None:
            return
        handle = self._handle
        self._handle = None
        try:
            if fcntl is not None:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.release()


class TaskWorkerOwnership:
    """Worker-specific identity validation and serialized ownership mutations."""

    def __init__(self, tasks_root: Path, worker_script: Path) -> None:
        self.tasks_root = Path(tasks_root).expanduser().resolve(strict=False)
        self.worker_script = Path(worker_script).expanduser().resolve(strict=False)
        self.pid_path = self.tasks_root / "worker.pid"
        self.owner_path = self.tasks_root / "worker.owner.json"
        self.owner_lock_target = self.tasks_root / ".worker-owner"
        self.spawn_lock_target = self.tasks_root / ".worker-spawn"
        self.lifetime_lock_path = self.tasks_root / ".worker-lifetime.lock"
        self.suppression_path = self.tasks_root / "worker.suppression.json"

    def acquire_lifetime(self) -> WorkerLifetimeLease | None:
        """Acquire the singleton lease, returning immediately on contention."""
        if fcntl is None:
            raise RuntimeError("Resident task-worker ownership requires POSIX file locks")
        self.tasks_root.mkdir(parents=True, exist_ok=True)
        handle = self.lifetime_lock_path.open("a+b")
        try:
            os.chmod(self.lifetime_lock_path, 0o600)
            os.set_inheritable(handle.fileno(), False)
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            handle.close()
            return None
        except Exception:
            handle.close()
            raise
        return WorkerLifetimeLease(self.lifetime_lock_path, handle)

    def status(self) -> WorkerStatus:
        """Return validated state and safely remove stale or reused PID ownership."""
        with advisory_file_lock(self.owner_lock_target):
            return self._status_locked()

    def claim_current(self, owner_id: str | None = None) -> WorkerOwner | None:
        """Publish the current process as ready unless another worker still owns it."""
        identity = self._read_process_identity(os.getpid())
        if identity is None or not self._identity_runs_worker(identity):
            raise RuntimeError("Current process is not the expected resident task worker")
        token = str(owner_id or "").strip() or uuid4().hex
        with advisory_file_lock(self.owner_lock_target):
            current = self._status_locked()
            if current.inspection_uncertain:
                raise TaskWorkerInspectionError(current.reason)
            if (
                current.process_alive
                and current.owner
                and current.owner.state == "ready"
                and current.owner.owner_id != token
            ):
                return None
            owner = self._owner_from_identity(identity, token, "ready")
            self._write_owner_locked(owner)
            return owner

    def publish_starting(self, owner_id: str, pid: int) -> WorkerOwner | None:
        """Publish a coordinator-owned child while it waits to acknowledge readiness."""
        identity = self._read_process_identity(pid)
        if identity is None or not self._identity_runs_worker(identity):
            return None
        with advisory_file_lock(self.owner_lock_target):
            current = self._status_locked()
            if current.inspection_uncertain:
                raise TaskWorkerInspectionError(current.reason)
            if current.process_alive and current.owner:
                if current.owner.owner_id == owner_id:
                    return current.owner
                return None
            owner = self._owner_from_identity(identity, owner_id, "starting")
            self._write_owner_locked(owner)
            return owner

    def heartbeat(self, owner: WorkerOwner) -> WorkerOwner | None:
        """Refresh only the caller's still-current ready ownership record."""
        with advisory_file_lock(self.owner_lock_target):
            current = self._read_owner()
            if not self._same_owner(current, owner):
                return None
            try:
                if not self._owner_is_live(current):
                    return None
            except TaskWorkerInspectionError:
                return current
            refreshed = WorkerOwner(**{**asdict(current), "state": "ready", "heartbeat_ts": time.time()})
            self._write_owner_locked(refreshed)
            return refreshed

    def release(self, owner: WorkerOwner) -> bool:
        """Clear files only if they still name the caller's exact ownership."""
        with advisory_file_lock(self.owner_lock_target):
            return self._clear_owner_locked(owner)

    def signal_owner(self, owner: WorkerOwner, signum: int) -> bool:
        """Signal only the exact PID/create-time worker still recorded as owner."""
        with advisory_file_lock(self.owner_lock_target):
            current = self._read_owner()
            if not self._same_owner(current, owner) or not self._owner_is_live(current):
                return False
            return self._signal_identity(current, signum)

    def _status_locked(self) -> WorkerStatus:
        suppression = self._read_suppression()
        owner = self._read_owner()
        if owner is not None:
            try:
                owner_is_live = self._owner_is_live(owner)
            except TaskWorkerInspectionError as exc:
                return WorkerStatus(
                    running=owner.state == "ready",
                    process_alive=True,
                    pid=owner.pid,
                    owner=owner,
                    reason=str(exc),
                    suppressed=suppression.suppressed,
                    suppression_generation=suppression.generation,
                    inspection_uncertain=True,
                )
            if owner_is_live:
                if self._read_pid() != owner.pid:
                    self._write_pid(owner.pid)
                return WorkerStatus(
                    running=owner.state == "ready",
                    process_alive=True,
                    pid=owner.pid,
                    owner=owner,
                    reason="" if owner.state == "ready" else "worker startup is pending",
                    suppressed=suppression.suppressed,
                    suppression_generation=suppression.generation,
                )
            self._clear_owner_locked(owner)

        legacy_pid = self._read_pid()
        if legacy_pid is None:
            return WorkerStatus(
                False,
                False,
                None,
                None,
                "manual stop suppression active" if suppression.suppressed else "",
                suppression.suppressed,
                suppression.generation,
            )
        try:
            identity = self._read_process_identity(legacy_pid)
        except TaskWorkerInspectionError as exc:
            return WorkerStatus(
                False,
                True,
                legacy_pid,
                None,
                str(exc),
                suppression.suppressed,
                suppression.generation,
                True,
            )
        if identity is None or not self._identity_runs_worker(identity):
            self._clear_pid_if_matches(legacy_pid)
            return WorkerStatus(
                False,
                False,
                None,
                None,
                "stale worker PID cleared",
                suppression.suppressed,
                suppression.generation,
            )

        adopted = self._owner_from_identity(
            identity,
            f"legacy-{identity.pid}-{identity.create_time:.6f}",
            "ready",
        )
        self._write_owner_locked(adopted)
        return WorkerStatus(
            True,
            True,
            adopted.pid,
            adopted,
            "legacy worker ownership adopted",
            suppression.suppressed,
            suppression.generation,
        )

    def suppression(self) -> WorkerSuppression:
        """Return durable automatic-start suppression under the spawn lock."""
        with advisory_file_lock(self.spawn_lock_target):
            return self._read_suppression()

    def set_suppressed_locked(self, suppressed: bool) -> WorkerSuppression:
        """Persist the next suppression generation while the caller owns spawn lock."""
        current = self._read_suppression()
        updated = WorkerSuppression(
            version=SUPPRESSION_VERSION,
            generation=current.generation + 1,
            suppressed=bool(suppressed),
            updated_ts=time.time(),
        )
        self.tasks_root.mkdir(parents=True, exist_ok=True)
        atomic_write_private_text(
            self.suppression_path,
            json.dumps(asdict(updated), indent=4, sort_keys=True) + "\n",
        )
        return updated

    def _read_suppression(self) -> WorkerSuppression:
        """Read suppression fail-closed so malformed state cannot auto-start work."""
        if not self.suppression_path.exists():
            return WorkerSuppression(SUPPRESSION_VERSION, 0, False, 0.0)
        try:
            value = json.loads(self.suppression_path.read_text(encoding="utf-8"))
            state = WorkerSuppression(
                version=int(value["version"]),
                generation=int(value["generation"]),
                suppressed=bool(value["suppressed"]),
                updated_ts=float(value["updated_ts"]),
            )
            if state.version != SUPPRESSION_VERSION or state.generation < 0:
                raise ValueError("invalid suppression state")
            return state
        except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
            return WorkerSuppression(SUPPRESSION_VERSION, 0, True, 0.0)

    def _owner_from_identity(self, identity: _ProcessIdentity, owner_id: str, state: str) -> WorkerOwner:
        return WorkerOwner(
            version=OWNER_VERSION,
            owner_id=str(owner_id),
            pid=identity.pid,
            create_time=identity.create_time,
            executable=identity.executable,
            script=str(self.worker_script),
            state=state,
            heartbeat_ts=time.time(),
        )

    def _read_owner(self) -> WorkerOwner | None:
        try:
            value = json.loads(self.owner_path.read_text(encoding="utf-8"))
            owner = WorkerOwner(
                version=int(value["version"]),
                owner_id=str(value["owner_id"]),
                pid=int(value["pid"]),
                create_time=float(value["create_time"]),
                executable=str(value["executable"]),
                script=str(value["script"]),
                state=str(value["state"]),
                heartbeat_ts=float(value["heartbeat_ts"]),
            )
            if owner.version != OWNER_VERSION or owner.pid <= 1 or not owner.owner_id:
                raise ValueError("invalid worker owner")
            return owner
        except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
            self.owner_path.unlink(missing_ok=True)
            return None

    def _read_pid(self) -> int | None:
        try:
            pid = int(self.pid_path.read_text(encoding="utf-8").strip())
            return pid if pid > 1 else None
        except (OSError, ValueError):
            return None

    def _write_owner_locked(self, owner: WorkerOwner) -> None:
        self.tasks_root.mkdir(parents=True, exist_ok=True)
        atomic_write_private_text(
            self.owner_path,
            json.dumps(asdict(owner), indent=4, sort_keys=True) + "\n",
        )
        self._write_pid(owner.pid)

    def _write_pid(self, pid: int) -> None:
        atomic_write_private_text(self.pid_path, f"{int(pid)}\n")

    def _clear_owner_locked(self, expected: WorkerOwner) -> bool:
        current = self._read_owner()
        if not self._same_owner(current, expected):
            return False
        self.owner_path.unlink(missing_ok=True)
        self._clear_pid_if_matches(expected.pid)
        return True

    def _clear_pid_if_matches(self, expected_pid: int) -> None:
        if self._read_pid() == int(expected_pid):
            self.pid_path.unlink(missing_ok=True)

    @staticmethod
    def _same_owner(left: WorkerOwner | None, right: WorkerOwner | None) -> bool:
        return bool(
            left
            and right
            and left.owner_id == right.owner_id
            and left.pid == right.pid
            and abs(left.create_time - right.create_time) < 0.01
        )

    def _owner_is_live(self, owner: WorkerOwner) -> bool:
        if owner.script != str(self.worker_script) or owner.state not in {"starting", "ready"}:
            return False
        identity = self._read_process_identity(owner.pid)
        return bool(
            identity
            and abs(identity.create_time - owner.create_time) < 0.01
            and identity.executable == owner.executable
            and self._identity_runs_worker(identity)
        )

    def _identity_runs_worker(self, identity: _ProcessIdentity) -> bool:
        if "--run-job" in identity.cmdline:
            return False
        for argument in identity.cmdline[1:]:
            if not argument or argument.startswith("-"):
                continue
            candidate = Path(argument).expanduser()
            if candidate.suffix.lower() != ".py":
                continue
            if not candidate.is_absolute():
                candidate = Path(identity.cwd) / candidate
            return candidate.resolve(strict=False) == self.worker_script
        return False

    @staticmethod
    def _read_process_identity(pid: int) -> _ProcessIdentity | None:
        try:
            process = psutil.Process(int(pid))
            return _ProcessIdentity(
                pid=int(pid),
                create_time=float(process.create_time()),
                executable=str(Path(process.exe()).resolve(strict=False)),
                cmdline=tuple(process.cmdline()),
                cwd=str(Path(process.cwd()).resolve(strict=False)),
            )
        except (psutil.NoSuchProcess, ProcessLookupError, ValueError):
            return None
        except OSError as exc:
            if exc.errno == errno.ESRCH:
                return None
            raise TaskWorkerInspectionError(f"Could not inspect task worker PID {pid}: {exc}") from exc
        except psutil.Error as exc:
            raise TaskWorkerInspectionError(f"Could not inspect task worker PID {pid}: {exc}") from exc

    def _signal_identity(self, owner: WorkerOwner, signum: int) -> bool:
        identity = self._read_process_identity(owner.pid)
        if identity is None or abs(identity.create_time - owner.create_time) >= 0.01:
            return False
        return signal_exact_process(ProcessIdentity(owner.pid, owner.create_time), signum)


def _default_manager() -> TaskWorkerOwnership:
    from task_queue import get_tasks_root_dir

    return TaskWorkerOwnership(
        get_tasks_root_dir(),
        Path(__file__).resolve().parent / "task_worker.py",
    )


def get_task_worker_status() -> WorkerStatus:
    """Return centrally validated resident task-worker status."""
    return _default_manager().status()


def acquire_task_worker_lifetime() -> tuple[TaskWorkerOwnership, WorkerLifetimeLease | None]:
    """Return the production ownership manager and a nonblocking lifetime lease."""
    manager = _default_manager()
    return manager, manager.acquire_lifetime()


def claim_task_worker(manager: TaskWorkerOwnership) -> WorkerOwner | None:
    """Publish the current worker using a coordinator token when supplied."""
    return manager.claim_current(os.environ.get(OWNER_ENV))


def ensure_task_worker_started(
    *,
    startup_timeout_s: float = STARTUP_TIMEOUT_S,
    manager: TaskWorkerOwnership | None = None,
    command: list[str] | None = None,
    popen_factory: Callable[..., subprocess.Popen] = subprocess.Popen,
    explicit: bool = False,
) -> WorkerStartResult:
    """Serialize resident starts and wait for exact-owner readiness acknowledgement."""
    manager = manager or _default_manager()
    command = list(command or [sys.executable, str(manager.worker_script)])
    timeout_s = max(0.2, float(startup_timeout_s))
    with advisory_file_lock(manager.spawn_lock_target):
        suppression = manager._read_suppression()
        if explicit:
            manager.set_suppressed_locked(False)
        elif suppression.suppressed:
            raise TaskWorkerSuppressedError(
                f"Market Data Queue automatic start is suppressed at generation {suppression.generation}"
            )
        status = manager.status()
        if status.inspection_uncertain:
            raise TaskWorkerInspectionError(status.reason)
        if status.running:
            return WorkerStartResult(status=status, spawned=False)
        if status.process_alive and status.owner:
            status = _wait_for_ready(manager, status.owner.owner_id, None, timeout_s)
            if status.running:
                return WorkerStartResult(status=status, spawned=False)
            if status.owner is not None and not _cleanup_starting_owner(manager, status.owner):
                raise TaskWorkerStartupError(
                    f"Existing task worker PID {status.owner.pid} did not leave starting state"
                )

        owner_id = uuid4().hex
        environment = os.environ.copy()
        environment[OWNER_ENV] = owner_id
        process = popen_factory(
            command,
            cwd=str(manager.worker_script.parent),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            close_fds=True,
            start_new_session=True,
            env=environment,
        )
        deadline = time.monotonic() + timeout_s
        owner: WorkerOwner | None = None
        try:
            while time.monotonic() < deadline:
                status = manager.status()
                if status.running:
                    process.poll()
                    return WorkerStartResult(status=status, spawned=status.owner is not None and status.owner.owner_id == owner_id)
                if status.process_alive and status.owner and status.owner.owner_id != owner_id:
                    if process.poll() is not None:
                        process.wait(timeout=0)
                    return WorkerStartResult(status=status, spawned=False)
                if owner is None:
                    owner = manager.publish_starting(owner_id, process.pid)
                if process.poll() is not None:
                    status = manager.status()
                    if status.running:
                        process.wait(timeout=0)
                        return WorkerStartResult(status=status, spawned=False)
                    raise TaskWorkerStartupError(
                        f"Task worker exited during startup with code {process.returncode}"
                    )
                time.sleep(0.05)
            status = manager.status()
            if status.running:
                return WorkerStartResult(status=status, spawned=status.owner is not None and status.owner.owner_id == owner_id)
            raise TaskWorkerStartupError(
                f"Task worker did not acknowledge startup within {timeout_s:g}s"
            )
        except Exception:
            if owner is None:
                current = manager.status().owner
                if current is not None and current.owner_id == owner_id:
                    owner = current
            if owner is not None:
                _cleanup_spawned_process(manager, owner, process)
            else:
                process.poll()
                if process.returncode is not None:
                    process.wait(timeout=0)
            raise


def stop_task_worker(
    timeout_s: float,
    *,
    manager: TaskWorkerOwnership | None = None,
) -> WorkerStatus:
    """Stop only the validated owner while excluding coordinated concurrent starts."""
    manager = manager or _default_manager()
    timeout_s = max(0.1, float(timeout_s))
    with advisory_file_lock(manager.spawn_lock_target):
        manager.set_suppressed_locked(True)
        status = manager.status()
        if status.inspection_uncertain:
            raise TaskWorkerInspectionError(status.reason)
        if not status.process_alive or status.owner is None:
            return status
        owner = status.owner
        if not manager.signal_owner(owner, signal.SIGTERM):
            return manager.status()
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            current = manager.status()
            if not current.process_alive or current.owner is None or current.owner.owner_id != owner.owner_id:
                return current
            time.sleep(0.1)
        current = manager.status()
        if current.process_alive and current.owner and current.owner.owner_id == owner.owner_id:
            raise TaskWorkerStopTimeout(
                f"Market Data Queue worker PID {owner.pid} did not stop within {int(timeout_s)}s"
            )
        return current


def _wait_for_ready(
    manager: TaskWorkerOwnership,
    owner_id: str,
    process: subprocess.Popen | None,
    timeout_s: float,
) -> WorkerStatus:
    deadline = time.monotonic() + timeout_s
    status = manager.status()
    while time.monotonic() < deadline:
        if status.running or not status.process_alive or not status.owner or status.owner.owner_id != owner_id:
            return status
        if process is not None and process.poll() is not None:
            return manager.status()
        time.sleep(0.05)
        status = manager.status()
    return status


def _cleanup_starting_owner(manager: TaskWorkerOwnership, owner: WorkerOwner) -> bool:
    if manager.signal_owner(owner, signal.SIGTERM):
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            status = manager.status()
            if not status.owner or status.owner.owner_id != owner.owner_id:
                return True
            time.sleep(0.05)
        if manager.signal_owner(owner, signal.SIGKILL):
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline:
                status = manager.status()
                if not status.owner or status.owner.owner_id != owner.owner_id:
                    return True
                time.sleep(0.05)
    status = manager.status()
    if status.owner and status.owner.owner_id == owner.owner_id and status.process_alive:
        return False
    manager.release(owner)
    return True


def _cleanup_spawned_process(
    manager: TaskWorkerOwnership,
    owner: WorkerOwner,
    process: subprocess.Popen,
) -> None:
    if manager.signal_owner(owner, signal.SIGTERM):
        try:
            process.wait(timeout=2.0)
        except subprocess.TimeoutExpired:
            if manager.signal_owner(owner, signal.SIGKILL):
                try:
                    process.wait(timeout=2.0)
                except subprocess.TimeoutExpired:
                    pass
    else:
        process.poll()
        if process.returncode is not None:
            process.wait(timeout=0)
    status = manager.status()
    if not status.owner or status.owner.owner_id != owner.owner_id or not status.process_alive:
        manager.release(owner)
