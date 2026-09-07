"""Process and identity regression tests for resident task-worker ownership."""

from __future__ import annotations

import json
import multiprocessing
import os
from pathlib import Path
import subprocess
import sys
import threading
import time

import pytest

import task_worker_ownership as ownership


def _try_lifetime_lease(
    tasks_root: str,
    worker_script: str,
    start: multiprocessing.synchronize.Event,
    release: multiprocessing.synchronize.Event,
    outcomes: multiprocessing.queues.Queue,
) -> None:
    """Contend for one lifetime lease in a child process."""
    manager = ownership.TaskWorkerOwnership(Path(tasks_root), Path(worker_script))
    start.wait(5)
    lease = manager.acquire_lifetime()
    outcomes.put(lease is not None)
    if lease is not None:
        release.wait(5)
        lease.release()


def _coordinated_start(
    tasks_root: str,
    worker_script: str,
    stop_path: str,
    outcomes: multiprocessing.queues.Queue,
) -> None:
    """Run one cross-process coordinated start request."""
    manager = ownership.TaskWorkerOwnership(Path(tasks_root), Path(worker_script))
    result = ownership.ensure_task_worker_started(
        manager=manager,
        command=[sys.executable, worker_script, tasks_root, stop_path],
        startup_timeout_s=4,
    )
    outcomes.put((result.spawned, result.status.pid))


def _write_worker_helper(path: Path) -> None:
    """Write a cooperative resident-worker stand-in below the pytest temp tree."""
    project_root = Path(__file__).resolve().parents[1]
    path.write_text(
        "\n".join(
            [
                "import os",
                "from pathlib import Path",
                "import sys",
                "import time",
                f"sys.path.insert(0, {str(project_root)!r})",
                "from task_worker_ownership import TaskWorkerOwnership",
                "tasks_root = Path(sys.argv[1])",
                "stop_path = Path(sys.argv[2])",
                "manager = TaskWorkerOwnership(tasks_root, Path(__file__))",
                "lease = manager.acquire_lifetime()",
                "if lease is None:",
                "    raise SystemExit(0)",
                "owner = manager.claim_current(os.environ.get('PBGUI_TASK_WORKER_OWNER_ID'))",
                "if owner is None:",
                "    lease.release()",
                "    raise SystemExit(0)",
                "try:",
                "    while not stop_path.exists():",
                "        refreshed = manager.heartbeat(owner)",
                "        if refreshed is None:",
                "            raise SystemExit(2)",
                "        owner = refreshed",
                "        time.sleep(0.02)",
                "finally:",
                "    manager.release(owner)",
                "    lease.release()",
                "",
            ]
        ),
        encoding="utf-8",
    )


def _wait_for_stopped(manager: ownership.TaskWorkerOwnership, timeout: float = 5.0) -> None:
    """Wait for a cooperative helper to release its ownership."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not manager.status().process_alive:
            return
        time.sleep(0.05)
    raise AssertionError("test worker did not stop")


def test_lifetime_lease_allows_one_process_owner(tmp_path: Path) -> None:
    """Concurrent processes cannot both hold the worker lifetime lease."""
    context = multiprocessing.get_context("spawn")
    start = context.Event()
    release = context.Event()
    outcomes = context.Queue()
    workers = [
        context.Process(
            target=_try_lifetime_lease,
            args=(str(tmp_path / "tasks"), str(tmp_path / "task_worker.py"), start, release, outcomes),
        )
        for _ in range(2)
    ]
    for worker in workers:
        worker.start()
    start.set()
    results = [outcomes.get(timeout=5) for _ in workers]
    release.set()
    for worker in workers:
        worker.join(timeout=5)

    assert sorted(results) == [False, True]
    assert all(worker.exitcode == 0 for worker in workers)


def test_stale_reused_pid_is_cleared_without_signalling(monkeypatch, tmp_path: Path) -> None:
    """A live reused PID does not satisfy older worker creation ownership."""
    script = tmp_path / "task_worker.py"
    manager = ownership.TaskWorkerOwnership(tmp_path / "tasks", script)
    stale = ownership.WorkerOwner(
        version=1,
        owner_id="stale",
        pid=4321,
        create_time=10.0,
        executable=str(Path(sys.executable).resolve()),
        script=str(script.resolve()),
        state="ready",
        heartbeat_ts=time.time(),
    )
    manager._write_owner_locked(stale)
    reused = ownership._ProcessIdentity(
        pid=4321,
        create_time=11.0,
        executable=stale.executable,
        cmdline=(stale.executable, str(script)),
        cwd=str(tmp_path),
    )
    monkeypatch.setattr(manager, "_read_process_identity", lambda _pid: reused)
    monkeypatch.setattr(
        manager,
        "_signal_identity",
        lambda *_args: (_ for _ in ()).throw(AssertionError("reused PID must not be signalled")),
    )

    status = manager.status()

    assert status.running is False
    assert status.pid is None
    assert not manager.owner_path.exists()
    assert not manager.pid_path.exists()


def test_owner_cleanup_and_heartbeat_are_compare_before_clear(monkeypatch, tmp_path: Path) -> None:
    """An old process cannot refresh or clear a replacement owner's files."""
    script = tmp_path / "task_worker.py"
    manager = ownership.TaskWorkerOwnership(tmp_path / "tasks", script)
    identity = ownership._ProcessIdentity(
        pid=4321,
        create_time=10.0,
        executable=str(Path(sys.executable).resolve()),
        cmdline=(str(Path(sys.executable).resolve()), str(script)),
        cwd=str(tmp_path),
    )
    monkeypatch.setattr(manager, "_read_process_identity", lambda _pid: identity)
    old = manager._owner_from_identity(identity, "old", "ready")
    replacement = manager._owner_from_identity(identity, "replacement", "ready")
    manager._write_owner_locked(replacement)

    assert manager.heartbeat(old) is None
    assert manager.release(old) is False
    assert manager.status().owner == replacement
    assert manager.pid_path.read_text(encoding="utf-8").strip() == "4321"


def test_transient_owner_inspection_failure_preserves_live_ownership(monkeypatch, tmp_path: Path) -> None:
    """Inspection uncertainty assumes a persisted owner is live and never clears its files."""
    script = tmp_path / "task_worker.py"
    manager = ownership.TaskWorkerOwnership(tmp_path / "tasks", script)
    owner = ownership.WorkerOwner(
        version=1,
        owner_id="current",
        pid=4321,
        create_time=10.0,
        executable=str(Path(sys.executable).resolve()),
        script=str(script.resolve()),
        state="ready",
        heartbeat_ts=time.time(),
    )
    manager._write_owner_locked(owner)
    monkeypatch.setattr(
        manager,
        "_read_process_identity",
        lambda _pid: (_ for _ in ()).throw(ownership.TaskWorkerInspectionError("inspection unavailable")),
    )

    status = manager.status()

    assert status.running is True
    assert status.process_alive is True
    assert status.inspection_uncertain is True
    assert status.owner == owner
    assert manager.owner_path.exists()
    assert manager.pid_path.exists()
    assert manager.heartbeat(owner) == owner


def test_ownership_loss_runs_worker_coordinated_shutdown(monkeypatch, tmp_path: Path) -> None:
    """Ownership leases remain held until an active job thread has stopped."""
    import task_worker

    events: list[str] = []
    running_dir = tmp_path / "running"
    pending_dir = tmp_path / "pending"
    running_dir.mkdir()
    pending_dir.mkdir()
    pending_job = pending_dir / "active.json"
    pending_job.write_text(json.dumps({"type": "test", "status": "pending"}), encoding="utf-8")
    owner = object()
    job_started = threading.Event()
    allow_job_finish = threading.Event()
    ownership_lost = threading.Event()
    owner_released = threading.Event()
    lifetime_released = threading.Event()

    class Manager:
        """Lose ownership after allowing one job thread to start."""

        heartbeat_count = 0

        def heartbeat(self, current):
            assert current is owner
            self.heartbeat_count += 1
            if self.heartbeat_count == 1:
                return owner
            assert job_started.wait(timeout=2)
            events.append("heartbeat-lost")
            ownership_lost.set()
            return None

        def release(self, current) -> None:
            assert current is owner
            events.append("release-owner")
            owner_released.set()

    class Lease:
        """Record release of the worker lifetime lock."""

        def release(self) -> None:
            events.append("release-lifetime")
            lifetime_released.set()

    class Capability:
        """Minimal capability heartbeat used by task_worker.main."""

        def __init__(self, *_args) -> None:
            pass

        def __enter__(self):
            return self

        def close(self) -> None:
            events.append("close-capability")

    import credential_process_registry

    monkeypatch.setattr(task_worker, "_STOP", False)
    monkeypatch.setattr(task_worker, "ensure_task_dirs", lambda: None)
    monkeypatch.setattr(task_worker, "acquire_task_worker_lifetime", lambda: (Manager(), Lease()))
    monkeypatch.setattr(task_worker, "claim_task_worker", lambda _manager: owner)
    monkeypatch.setattr(task_worker, "_requeue_stale_running_jobs", lambda max_age_s: events.append(f"requeue:{max_age_s}"))
    monkeypatch.setattr(
        task_worker,
        "get_task_state_dir",
        lambda state: running_dir if state == "running" else pending_dir,
    )
    monkeypatch.setattr(task_worker, "_run_cache_sweep_thread", lambda **_kwargs: None)
    monkeypatch.setattr(task_worker, "_load_job", lambda path: json.loads(path.read_text(encoding="utf-8")))

    def move_job(path: Path, state: str) -> Path:
        destination = (running_dir if state == "running" else pending_dir) / path.name
        path.replace(destination)
        return destination

    def run_job(_path: Path) -> None:
        events.append("job-started")
        job_started.set()
        assert allow_job_finish.wait(timeout=2)
        events.append("job-finished")

    monkeypatch.setattr(task_worker, "move_job_file", move_job)
    monkeypatch.setattr(task_worker, "_run_job", run_job)
    monkeypatch.setattr(task_worker.signal, "signal", lambda *_args: None)
    monkeypatch.setattr(task_worker, "_job_log", lambda message, **_kwargs: events.append(message))
    monkeypatch.setattr(credential_process_registry, "ProcessCapabilityHeartbeat", Capability)

    result: list[int] = []
    worker_thread = threading.Thread(target=lambda: result.append(task_worker.main()))
    worker_thread.start()
    assert ownership_lost.wait(timeout=2)
    time.sleep(0.05)
    assert worker_thread.is_alive()
    assert not owner_released.is_set()
    assert not lifetime_released.is_set()

    allow_job_finish.set()
    worker_thread.join(timeout=2)

    assert not worker_thread.is_alive()
    assert result == [1]
    assert task_worker._STOP is True
    assert "worker stopping; waiting for active jobs..." in events
    assert events.index("job-finished") < events.index("release-owner")
    assert events[-3:] == ["release-owner", "close-capability", "release-lifetime"]


def test_repeated_loop_errors_run_worker_coordinated_shutdown(monkeypatch, tmp_path: Path) -> None:
    """Fatal loop errors keep worker leases until an active job has stopped."""
    import task_worker

    events: list[str] = []
    running_dir = tmp_path / "running"
    pending_dir = tmp_path / "pending"
    running_dir.mkdir()
    pending_dir.mkdir()
    pending_job = pending_dir / "active.json"
    pending_job.write_text(json.dumps({"type": "test", "status": "pending"}), encoding="utf-8")
    owner = object()
    job_started = threading.Event()
    allow_job_finish = threading.Event()
    quiescing = threading.Event()
    owner_released = threading.Event()
    lifetime_released = threading.Event()

    class Manager:
        """Raise repeated heartbeat errors after one job thread starts."""

        heartbeat_count = 0

        def heartbeat(self, current):
            assert current is owner
            self.heartbeat_count += 1
            if self.heartbeat_count == 1:
                return owner
            assert job_started.wait(timeout=2)
            raise RuntimeError("forced heartbeat failure")

        def release(self, current) -> None:
            assert current is owner
            events.append("release-owner")
            owner_released.set()

    manager = Manager()

    class Lease:
        """Record release of the worker lifetime lock."""

        def release(self) -> None:
            events.append("release-lifetime")
            lifetime_released.set()

    class Capability:
        """Minimal capability heartbeat used by task_worker.main."""

        def __init__(self, *_args) -> None:
            pass

        def __enter__(self):
            return self

        def close(self) -> None:
            events.append("close-capability")

    import credential_process_registry

    monkeypatch.setattr(task_worker, "_STOP", False)
    monkeypatch.setattr(task_worker, "ensure_task_dirs", lambda: None)
    monkeypatch.setattr(task_worker, "acquire_task_worker_lifetime", lambda: (manager, Lease()))
    monkeypatch.setattr(task_worker, "claim_task_worker", lambda _manager: owner)
    monkeypatch.setattr(task_worker, "_requeue_stale_running_jobs", lambda max_age_s: events.append(f"requeue:{max_age_s}"))
    monkeypatch.setattr(
        task_worker,
        "get_task_state_dir",
        lambda state: running_dir if state == "running" else pending_dir,
    )
    monkeypatch.setattr(task_worker, "_run_cache_sweep_thread", lambda **_kwargs: None)
    monkeypatch.setattr(task_worker, "_load_job", lambda path: json.loads(path.read_text(encoding="utf-8")))

    def move_job(path: Path, state: str) -> Path:
        destination = (running_dir if state == "running" else pending_dir) / path.name
        path.replace(destination)
        return destination

    def run_job(_path: Path) -> None:
        events.append("job-started")
        job_started.set()
        assert allow_job_finish.wait(timeout=2)
        events.append("job-finished")

    def record_log(message: str, **_kwargs) -> None:
        events.append(message)
        if message == "worker stopping; waiting for active jobs...":
            quiescing.set()

    monkeypatch.setattr(task_worker, "move_job_file", move_job)
    monkeypatch.setattr(task_worker, "_run_job", run_job)
    monkeypatch.setattr(task_worker.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(task_worker.signal, "signal", lambda *_args: None)
    monkeypatch.setattr(task_worker, "_job_log", record_log)
    monkeypatch.setattr(credential_process_registry, "ProcessCapabilityHeartbeat", Capability)

    result: list[int] = []
    worker_thread = threading.Thread(target=lambda: result.append(task_worker.main()))
    worker_thread.start()
    assert quiescing.wait(timeout=2)
    assert manager.heartbeat_count == 11
    assert worker_thread.is_alive()
    assert not owner_released.is_set()
    assert not lifetime_released.is_set()

    allow_job_finish.set()
    worker_thread.join(timeout=2)

    assert not worker_thread.is_alive()
    assert result == [1]
    assert task_worker._STOP is True
    assert events.index("job-finished") < events.index("release-owner")
    assert events[-3:] == ["release-owner", "close-capability", "release-lifetime"]


def test_numeric_legacy_pid_is_adopted_with_identity_metadata(monkeypatch, tmp_path: Path) -> None:
    """A valid shipped numeric PID file is upgraded to explicit owner metadata."""
    script = tmp_path / "task_worker.py"
    manager = ownership.TaskWorkerOwnership(tmp_path / "tasks", script)
    manager.tasks_root.mkdir(parents=True)
    manager.pid_path.write_text("4321", encoding="utf-8")
    identity = ownership._ProcessIdentity(
        pid=4321,
        create_time=10.0,
        executable=str(Path(sys.executable).resolve()),
        cmdline=(str(Path(sys.executable).resolve()), str(script)),
        cwd=str(tmp_path),
    )
    monkeypatch.setattr(manager, "_read_process_identity", lambda _pid: identity)

    status = manager.status()
    persisted = json.loads(manager.owner_path.read_text(encoding="utf-8"))

    assert status.running is True
    assert status.pid == 4321
    assert persisted["create_time"] == 10.0
    assert persisted["script"] == str(script.resolve())
    assert persisted["executable"] == str(Path(sys.executable).resolve())


def test_concurrent_coordinators_and_sequential_start_share_one_worker(tmp_path: Path) -> None:
    """Cross-process and repeated starts converge on one acknowledged worker PID."""
    helper = tmp_path / "task_worker.py"
    tasks_root = tmp_path / "tasks"
    stop_path = tmp_path / "stop"
    _write_worker_helper(helper)
    context = multiprocessing.get_context("spawn")
    outcomes = context.Queue()
    starters = [
        context.Process(
            target=_coordinated_start,
            args=(str(tasks_root), str(helper), str(stop_path), outcomes),
        )
        for _ in range(2)
    ]
    for starter in starters:
        starter.start()
    results = [outcomes.get(timeout=10) for _ in starters]
    for starter in starters:
        starter.join(timeout=5)

    manager = ownership.TaskWorkerOwnership(tasks_root, helper)
    repeated = ownership.ensure_task_worker_started(
        manager=manager,
        command=[sys.executable, str(helper), str(tasks_root), str(stop_path)],
        startup_timeout_s=2,
    )
    try:
        assert sum(1 for spawned, _pid in results if spawned) == 1
        assert len({pid for _spawned, pid in results}) == 1
        assert repeated.spawned is False
        assert repeated.status.pid == results[0][1]
    finally:
        stop_path.touch()
        _wait_for_stopped(manager)


def test_manual_and_coordinated_start_contention_has_one_ready_owner(tmp_path: Path) -> None:
    """A manual process and watchdog-style start cannot both become resident owners."""
    helper = tmp_path / "task_worker.py"
    tasks_root = tmp_path / "tasks"
    stop_path = tmp_path / "stop"
    _write_worker_helper(helper)
    manager = ownership.TaskWorkerOwnership(tasks_root, helper)
    manual = subprocess.Popen(
        [sys.executable, str(helper), str(tasks_root), str(stop_path)],
        cwd=str(tmp_path),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        close_fds=True,
    )
    try:
        result = ownership.ensure_task_worker_started(
            manager=manager,
            command=[sys.executable, str(helper), str(tasks_root), str(stop_path)],
            startup_timeout_s=4,
        )
        assert result.status.running is True
        current = manager.status().owner
        assert current is not None
        assert result.status.owner is not None
        assert current.owner_id == result.status.owner.owner_id
        assert current.pid == result.status.owner.pid
        assert current.create_time == result.status.owner.create_time
    finally:
        stop_path.touch()
        manual.wait(timeout=5)
        _wait_for_stopped(manager)


def test_failed_startup_cleans_only_its_owned_child(monkeypatch, tmp_path: Path) -> None:
    """Startup cleanup signals the exact coordinator owner and preserves replacements."""
    script = tmp_path / "task_worker.py"
    manager = ownership.TaskWorkerOwnership(tmp_path / "tasks", script)
    child = ownership._ProcessIdentity(
        pid=4321,
        create_time=10.0,
        executable=str(Path(sys.executable).resolve()),
        cmdline=(str(Path(sys.executable).resolve()), str(script)),
        cwd=str(tmp_path),
    )
    replacement_identity = ownership._ProcessIdentity(
        pid=9876,
        create_time=20.0,
        executable=child.executable,
        cmdline=child.cmdline,
        cwd=child.cwd,
    )
    child_owner = manager._owner_from_identity(child, "child", "starting")
    replacement = manager._owner_from_identity(replacement_identity, "replacement", "ready")
    manager._write_owner_locked(replacement)
    signalled: list[int] = []
    monkeypatch.setattr(manager, "_read_process_identity", lambda pid: replacement_identity if pid == 9876 else child)
    monkeypatch.setattr(manager, "_signal_identity", lambda owner, _sig: signalled.append(owner.pid) or True)

    process = type("Process", (), {"returncode": None, "poll": lambda self: None})()
    ownership._cleanup_spawned_process(manager, child_owner, process)

    assert signalled == []
    assert manager.status().owner == replacement


def test_startup_timeout_reaps_exact_owned_child(monkeypatch, tmp_path: Path) -> None:
    """A failed acknowledgement terminates and clears only the launched owner."""
    script = tmp_path / "task_worker.py"
    manager = ownership.TaskWorkerOwnership(tmp_path / "tasks", script)
    identity = ownership._ProcessIdentity(
        pid=4321,
        create_time=10.0,
        executable=str(Path(sys.executable).resolve()),
        cmdline=(str(Path(sys.executable).resolve()), str(script)),
        cwd=str(tmp_path),
    )
    alive = True
    signals: list[int] = []

    class Process:
        """Popen double that exits when the ownership layer signals it."""

        pid = 4321
        returncode = None

        def poll(self):
            return self.returncode

        def wait(self, timeout=0):
            self.returncode = 0
            return 0

    process = Process()

    monkeypatch.setattr(manager, "_read_process_identity", lambda _pid: identity if alive else None)

    def signal_child(_owner, signum):
        nonlocal alive
        signals.append(signum)
        alive = False
        return True

    monkeypatch.setattr(manager, "_signal_identity", signal_child)

    with pytest.raises(ownership.TaskWorkerStartupError, match="did not acknowledge"):
        ownership.ensure_task_worker_started(
            manager=manager,
            command=[sys.executable, str(script)],
            startup_timeout_s=0.2,
            popen_factory=lambda *_args, **_kwargs: process,
        )

    assert signals == [ownership.signal.SIGTERM]
    assert process.returncode == 0
    assert not manager.owner_path.exists()
    assert not manager.pid_path.exists()


def test_manual_stop_persists_suppression_and_blocks_watchdog_start(tmp_path: Path) -> None:
    """A manual stop remains durable even when queued work requests auto-start."""
    manager = ownership.TaskWorkerOwnership(tmp_path / "tasks", tmp_path / "task_worker.py")

    status = ownership.stop_task_worker(0.1, manager=manager)

    assert status.process_alive is False
    suppression = manager.suppression()
    assert suppression.suppressed is True
    assert suppression.generation == 1
    with pytest.raises(ownership.TaskWorkerSuppressedError, match="generation 1"):
        ownership.ensure_task_worker_started(
            manager=manager,
            popen_factory=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not spawn")),
        )


def test_explicit_start_clears_suppression_generation(monkeypatch, tmp_path: Path) -> None:
    """The Services Start action clears durable suppression before revalidation."""
    manager = ownership.TaskWorkerOwnership(tmp_path / "tasks", tmp_path / "task_worker.py")
    with ownership.advisory_file_lock(manager.spawn_lock_target):
        manager.set_suppressed_locked(True)
    ready = ownership.WorkerStatus(True, True, 42, None)
    monkeypatch.setattr(manager, "status", lambda: ready)

    result = ownership.ensure_task_worker_started(manager=manager, explicit=True)

    assert result.spawned is False
    suppression = manager.suppression()
    assert suppression.suppressed is False
    assert suppression.generation == 2


def test_stop_contention_persists_suppression_under_spawn_lock(tmp_path: Path) -> None:
    """A contending watchdog/start cannot pass a manual stop generation update."""
    manager = ownership.TaskWorkerOwnership(tmp_path / "tasks", tmp_path / "task_worker.py")
    finished = threading.Event()

    def stop() -> None:
        ownership.stop_task_worker(0.1, manager=manager)
        finished.set()

    with ownership.advisory_file_lock(manager.spawn_lock_target):
        contender = threading.Thread(target=stop)
        contender.start()
        time.sleep(0.05)
        assert finished.is_set() is False
    contender.join(timeout=2)

    assert finished.is_set() is True
    assert manager.suppression().suppressed is True
    with pytest.raises(ownership.TaskWorkerSuppressedError):
        ownership.ensure_task_worker_started(manager=manager)
