"""Tests for the detached PB8 optimize status runner."""

from __future__ import annotations

import json
import os
import sys
from types import SimpleNamespace

import pytest

import pb8_optimize_runner
from sweep_cycles import SWEEP_PLAN_FILENAME, build_sweep_plan


@pytest.mark.parametrize('succeeds', [False, True])
def test_runner_records_mocked_resume_completion_or_native_error(tmp_path, monkeypatch, succeeds):
    """Test resume argv and state/error propagation, without native signature validation."""
    options = tmp_path / 'options.json'
    options.write_text(json.dumps({'mode': 'checkpoint_resume', 'source': '/results/session'}))
    ownership = tmp_path / 'ownership.json'
    ownership.write_text(json.dumps({'pid': os.getpid()}))
    state = tmp_path / 'state.json'
    reason = 'GPU checkpoint does not match current search, scoring, suite, or prepared execution contract'

    async def native_main():
        """Return or raise a representative error from the mocked native entry point."""
        assert sys.argv[-2:] == ['--resume', '/results/session']
        if not succeeds:
            raise ValueError(reason)
        return 0

    monkeypatch.setattr(pb8_optimize_runner.importlib, 'import_module', lambda _: SimpleNamespace(main=native_main))
    monkeypatch.setattr(pb8_optimize_runner, 'acquire_master_runtime_lock', lambda _: SimpleNamespace(release=lambda: None))
    code = pb8_optimize_runner.main(['optimize', str(state), str(ownership), str(tmp_path / 'ready'),
                                   'passivbot', '/pb8', '/queue/config.json', str(options)])
    payload = json.loads(state.read_text())
    assert code == (0 if succeeds else 1)
    if succeeds:
        assert 'error' not in payload
    else:
        assert payload['error'] == 'ValueError: ' + reason


@pytest.mark.parametrize(
    ("options", "expected_tail"),
    [
        ({"mode": "fresh"}, []),
        ({"mode": "pareto_seed", "source": "/results/pareto/one.json"}, ["--start", "/results/pareto/one.json"]),
        ({"mode": "checkpoint_resume", "source": "/results/run"}, ["--resume", "/results/run"]),
    ],
)
def test_runner_builds_distinct_pb8_launch_modes(tmp_path, monkeypatch, options, expected_tail) -> None:
    """Fresh, Pareto seed, and checkpoint resume must map to different PB8 flags."""
    state = tmp_path / "state.json"
    ownership = tmp_path / "ownership.json"
    ready = tmp_path / "ready"
    options_path = tmp_path / "options.json"
    ownership.write_text(json.dumps({"pid": os.getpid()}), encoding="utf-8")
    options_path.write_text(json.dumps(options), encoding="utf-8")
    captured = {}

    async def fake_main() -> None:
        captured["argv"] = list(sys.argv)

    class Lease:
        """Minimal runtime lock lease."""

        def release(self) -> None:
            captured["released"] = True

    monkeypatch.setattr(pb8_optimize_runner.importlib, "import_module", lambda name: SimpleNamespace(main=fake_main))
    monkeypatch.setattr(pb8_optimize_runner, "acquire_master_runtime_lock", lambda _path: Lease())

    returncode = pb8_optimize_runner.main(
        [
            "optimize",
            str(state),
            str(ownership),
            str(ready),
            "/venv_pb8/bin/passivbot",
            "/pb8",
            "/queue/optimize.json",
            str(options_path),
        ]
    )

    assert returncode == 0
    assert captured["argv"] == ["/venv_pb8/bin/passivbot", "/queue/optimize.json", *expected_tail]
    assert captured["released"] is True
    assert json.loads(state.read_text(encoding="utf-8"))["returncode"] == 0
    assert json.loads(ownership.read_text(encoding="utf-8"))["pid"] == os.getpid()
    assert json.loads(ownership.read_text(encoding="utf-8"))["create_time"] > 0
    assert ready.read_text(encoding="utf-8") == f"{os.getpid()}\n"


def test_runner_passes_fine_tune_and_polish_options(tmp_path, monkeypatch) -> None:
    """PB8 expert launch controls must survive the detached runner boundary."""
    state = tmp_path / "state.json"
    ownership = tmp_path / "ownership.json"
    ready = tmp_path / "ready"
    options_path = tmp_path / "options.json"
    ownership.write_text(json.dumps({"pid": os.getpid()}), encoding="utf-8")
    options_path.write_text(
        json.dumps(
            {
                "mode": "pareto_seed",
                "source": "/results/pareto/one.json",
                "fine_tune_params": ["bot.long.risk.n_positions", "bot.short.risk.n_positions"],
                "polish_percentage": 0.2,
                "polish_bounds_mode": "override-tunable",
            }
        ),
        encoding="utf-8",
    )
    captured = {}

    async def fake_main() -> None:
        captured["argv"] = list(sys.argv)

    class Lease:
        """Minimal runtime lock lease."""

        def release(self) -> None:
            return None

    monkeypatch.setattr(pb8_optimize_runner.importlib, "import_module", lambda name: SimpleNamespace(main=fake_main))
    monkeypatch.setattr(pb8_optimize_runner, "acquire_master_runtime_lock", lambda _path: Lease())

    assert pb8_optimize_runner.main(
        ["optimize", str(state), str(ownership), str(ready), "passivbot", "/pb8", "/queue/config.json", str(options_path)]
    ) == 0
    assert captured["argv"] == [
        "passivbot",
        "/queue/config.json",
        "--start",
        "/results/pareto/one.json",
        "--fine-tune-params",
        "bot.long.risk.n_positions,bot.short.risk.n_positions",
        "--polish-pct",
        "0.2",
        "--polish-bounds-mode",
        "override-tunable",
    ]


def test_runner_persists_sweep_plan_only_beside_its_open_result(tmp_path, monkeypatch) -> None:
    """The runner binds immutable PBGui sweep metadata to its own PB8 result stream."""
    pb8_dir = tmp_path / "pb8"
    result_dir = pb8_dir / "optimize_results" / "run"
    result_dir.mkdir(parents=True)
    all_results = result_dir / "all_results.bin"
    all_results.write_bytes(b"")
    config = {
        "backtest": {"suite_enabled": True, "scenarios": [{"label": "train_01", "start_date": "2020-01-01", "end_date": "2020-03-30"}]},
        "pbgui": {
            "scenario_template": {
                "template": "sweep_cycles",
                "parameters": {
                    "window_days": 90,
                    "stride_days": 97,
                    "sweep_policy": {"starting_balance": 1000, "balance_multiplier": 2, "refill_cost": 5, "cooldown_days": 7},
                },
                "holdout_scenarios": [],
            }
        },
    }
    plan = build_sweep_plan(config)
    monkeypatch.setattr(
        pb8_optimize_runner.psutil,
        "Process",
        lambda _pid: SimpleNamespace(open_files=lambda: [SimpleNamespace(path=str(all_results))]),
    )

    assert pb8_optimize_runner._persist_open_sweep_plan(pb8_dir, plan) is True

    saved = json.loads((result_dir / SWEEP_PLAN_FILENAME).read_text(encoding="utf-8"))
    assert saved == plan
    assert oct((result_dir / SWEEP_PLAN_FILENAME).stat().st_mode & 0o777) == "0o600"


def test_runner_publishes_exact_evaluation_count_sidecar(tmp_path) -> None:
    """New runs expose ResultRecorder.n_iters without rescanning all_results.bin."""
    result_dir = tmp_path / "result"
    result_dir.mkdir()

    class Store:
        """Minimal PB8 Pareto store."""

        directory = str(result_dir)
        n_iters = 0

    class ResultRecorder:
        """Minimal PB8 recorder patched by the PBGui runner."""

        def __init__(self):
            self.store = Store()
            self.results_file = None

        def record(self, _data):
            self.store.n_iters += 1

    module = SimpleNamespace(ResultRecorder=ResultRecorder)
    publish_final = pb8_optimize_runner._install_evaluation_progress_publisher(module)
    recorder = ResultRecorder()

    recorder.record({})
    recorder.record({})
    publish_final()

    payload = json.loads(
        (result_dir / pb8_optimize_runner.EVALUATION_PROGRESS_FILENAME).read_text(encoding="utf-8")
    )
    assert payload["evaluations"] == 2
    assert payload["contract_version"] == 1


def _local_native(tmp_path, monkeypatch, *, backend='pymoo', fault=None, gpu_threads=None, real_signals=False):
    """Model the installed synchronous backend inside asyncio.run, with isolated files."""
    import logging
    import threading
    import time
    import pb8_local_convergence as local

    events = []
    record_threads, signal_threads, handler_threads, watchdogs = [], [], [], []
    monitors = []
    original_monitor = local.LocalConvergence

    class ObservedMonitor(original_monitor):
        """Expose ownership evidence without replacing the runner's stop logic."""
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            monitors.append(self)

    monkeypatch.setattr(local, 'LocalConvergence', ObservedMonitor)
    result = tmp_path / 'pb8' / 'optimize_results' / 'run'
    (result / 'pareto').mkdir(parents=True)
    point = result / 'pareto' / 'a.json'
    point.write_text(json.dumps({'metrics': {'constraint_violation': 0, 'unpenalized_objectives': [1.0]}}))
    checkpoint = result / 'checkpoint.pkl'
    checkpoint.write_bytes(b'previous-safe-checkpoint')

    class Interrupted(KeyboardInterrupt):
        """Native resource-carrying interrupt contract."""
        def __init__(self, *, pool, pool_terminated):
            self.pool = pool
            self.pool_terminated = pool_terminated

    class Latch:
        """Model the GPU handler without sending any real signal."""
        requested = False
        def handler(self, *_args):
            handler_threads.append(threading.current_thread())
            self.requested = True

    class Pool:
        """An owned pool records all native cleanup calls."""
        def terminate(self):
            events.append('terminate')
        def join(self):
            events.append('join')
            if fault == 'join':
                raise RuntimeError('join failed')

    pool = Pool()
    latch = Latch()

    class Recorder:
        """Only exact recorder calls increment the authoritative completed counter."""
        def __init__(self):
            self.store = SimpleNamespace(directory=str(result), n_iters=0, _lock=threading.RLock(),
                                         _front=['a'], _entries={'a': str(point)})
            self.scoring_specs = [SimpleNamespace(to_config=lambda: {'metric': 'test', 'goal': 'min'})]
            self.results_file = None
        def record(self, _data):
            before = self.store.n_iters
            if gpu_threads:
                # Deliberately expose a read/increment race unless the full Recorder
                # boundary is serialized, including original_record itself.
                time.sleep(.002)
                record_threads.append(threading.current_thread())
            self.store.n_iters = before + 128
            events.append('record-return')
        def flush(self):
            events.append('flush')
            if fault == 'flush':
                raise RuntimeError('flush failed')
        def close(self):
            events.append('close')

    class AsyncRunner:
        """Bind the actual recorder and pool, just as the installed PyMoo runner does."""
        def __init__(self, *, recorder, pool):
            self.recorder, self.pool = recorder, pool

    def terminate(owned, terminated):
        """Native main transfers the exception's owned pool into its finalizer."""
        assert owned is pool
        if not terminated:
            owned.terminate()
        return True

    module = SimpleNamespace(ResultRecorder=Recorder, _terminate_optimizer_pool=terminate)
    collector_errors = []
    signal_pending = threading.Event()

    def collect(recorder):
        """Model gpu-exact-collector without allowing a worker exception to go unseen."""
        try:
            recorder.record({})
        except BaseException as exc:
            collector_errors.append(exc)

    def join_collector(worker):
        """Always finish collector bookkeeping before the native cleanup path."""
        worker.join(timeout=5)
        assert not worker.is_alive()
        assert not collector_errors

    def gpu_checkpoint(recorder):
        """Handle a queued model signal on main, then reach the next safe GPU checkpoint."""
        if signal_pending.is_set():
            latch.handler()
            signal_pending.clear()
        if latch.requested:
            recorder.record({})  # Current exact bookkeeping may finish after latching.
            events.append('gpu-record-exact-return')
            raise Interrupted(pool=pool, pool_terminated=False)

    async def native_main():
        """Model the installed main's except KeyboardInterrupt and Shutdown complete. contract."""
        recorder = Recorder()
        owned = None
        interrupted = False
        if fault != 'pool':
            AsyncRunner(recorder=recorder, pool=pool)
        previous_handler = None
        if real_signals:
            previous_handler = local.signal.getsignal(local.signal.SIGINT)
            local.signal.signal(local.signal.SIGINT, latch.handler)
        try:
            if gpu_threads == 'concurrent':
                recorder.record({})  # Seed/main warmup.
                recorder.record({})  # Accepted exact baseline.
                worker = threading.Thread(target=collect, args=(recorder,), name='gpu-exact-collector')
                worker.start()
                try:
                    recorder.record({})  # Main and collector race to dispatch the same stop.
                finally:
                    join_collector(worker)
                gpu_checkpoint(recorder)
            for _ in range(5):
                if fault == 'interrupt':
                    raise KeyboardInterrupt
                if gpu_threads == 'collector' and _ > 0:
                    worker = threading.Thread(target=collect, args=(recorder,), name='gpu-exact-collector')
                    worker.start()
                    join_collector(worker)
                else:
                    recorder.record({})
                if backend == 'gpu':
                    gpu_checkpoint(recorder)
                if fault == 'thread':
                    break
        except KeyboardInterrupt as exc:
            if fault == 'interrupt':
                raise
            interrupted = True
            owned = getattr(exc, "pool", owned)
            terminated = bool(getattr(exc, "pool_terminated", False))
            assert json.loads((tmp_path / 'state.convergence.json').read_text())['convergence']['stop_requested']
            assert events[-1] in ('record-return', 'gpu-record-exact-return')
            assert not recorder.store._lock._is_owned()
            assert not monitors[0]._observation_lock._is_owned()
            events.append('native-catch')
            module._terminate_optimizer_pool(owned, terminated)
        finally:
            try:
                recorder.flush()
            except Exception:
                logging.getLogger().error('Failed to flush recorder')
            recorder.close()
            if owned:
                owned.join()
            logging.getLogger().warning('Shutdown complete.')
            if real_signals:
                local.signal.signal(local.signal.SIGINT, previous_handler)
        raise SystemExit(130 if interrupted else 0)

    module.main = native_main
    original_import = pb8_optimize_runner.importlib.import_module
    def imported(name):
        """Resolve fake native modules while leaving ordinary Python imports intact."""
        if name == 'optimize':
            return module
        if name == 'optimization.interrupts':
            return SimpleNamespace(OptimizerBackendInterrupted=Interrupted, OptimizerInterruptLatch=Latch)
        if name == 'optimization.problem':
            return SimpleNamespace(PymooAsyncRecordingRunner=AsyncRunner)
        return original_import(name)
    monkeypatch.setattr(pb8_optimize_runner.importlib, 'import_module', imported)
    monkeypatch.setattr(pb8_optimize_runner, 'acquire_master_runtime_lock', lambda _: SimpleNamespace(release=lambda: None))
    monkeypatch.setattr(local, '_log', lambda *args, **kwargs: None)
    ticks = iter(range(0, 10000, 61))
    monkeypatch.setattr(local, 'time', SimpleNamespace(monotonic=lambda: next(ticks), time=time.time))
    signals = []
    original_kill = local.os.kill

    def send_signal(pid, sig):
        """Assert the durable request and unlocked dispatch before modelling/sending SIGINT."""
        assert not monitors[0]._observation_lock._is_owned()
        assert not monitors[0].recorder.store._lock._is_owned()
        assert json.loads((tmp_path / 'state.convergence.json').read_text())['convergence']['stop_requested']
        signals.append(sig)
        signal_threads.append(threading.current_thread())
        if real_signals:
            original_kill(pid, sig)
        else:
            signal_pending.set()

    monkeypatch.setattr(local.os, 'kill', send_signal)
    if not real_signals:
        monkeypatch.setattr(local.signal, 'getsignal', lambda _: latch.handler)
    if gpu_threads == 'concurrent':
        dispatch_barrier = threading.Barrier(2)
        original_stop = original_monitor.request_stop

        def concurrent_stop(monitor, recorder):
            """Force both callers past observation before either reserves dispatch."""
            assert not monitor._observation_lock._is_owned()
            dispatch_barrier.wait(timeout=5)
            return original_stop(monitor, recorder)

        monkeypatch.setattr(ObservedMonitor, 'request_stop', concurrent_stop)
    original_popen = local.subprocess.Popen

    def spawn_watchdog(*args, **kwargs):
        """Count and later verify deterministic shutdown of every created watchdog."""
        process = original_popen(*args, **kwargs)
        watchdogs.append(process)
        return process

    monkeypatch.setattr(local.subprocess, 'Popen', spawn_watchdog)
    if fault == 'thread':
        monkeypatch.setattr(local.threading, 'current_thread', lambda: object())
    if fault == 'persist':
        original_write = local.atomic_write_private_text
        def failing_write(path, text):
            """Reject only stop requests, after baseline state was persisted successfully."""
            if json.loads(text)['convergence'].get('stop_requested'):
                raise OSError('test disk full')
            return original_write(path, text)
        monkeypatch.setattr(local, 'atomic_write_private_text', failing_write)
    options = tmp_path / 'options.json'
    options.write_text(json.dumps({'mode': 'fresh', 'convergence_backend': backend,
        'convergence_config': {'convergence_enabled': True, 'convergence_min_exact': 256,
                               'convergence_patience': 128, 'convergence_tolerance_pct': .25}}))
    original_record = Recorder.record
    original_init = AsyncRunner.__init__
    code = pb8_optimize_runner.main(['optimize', str(tmp_path / 'state.json'), str(tmp_path / 'owner.json'),
        str(tmp_path / 'ready'), 'passivbot', str(tmp_path / 'pb8'), '/isolated/config.json', str(options)])
    assert Recorder.record is original_record
    assert 'join' not in pool.__dict__
    assert AsyncRunner.__init__ is original_init
    assert not hasattr(Recorder, '_pbgui_progress_patched')
    assert checkpoint.read_bytes() == b'previous-safe-checkpoint'
    assert not any(thread.name == 'pb8-convergence-watchdog' for thread in threading.enumerate())
    assert all(process.poll() == 0 and testInput.closed for process in watchdogs)
    if gpu_threads:
        assert len(watchdogs) == 1
        assert not monitors[0].unavailable
        assert monitors[0].stop_sent
        assert any(thread.name == 'gpu-exact-collector' for thread in record_threads)
        assert handler_threads == [threading.main_thread()]
        if gpu_threads == 'collector':
            assert signal_threads[0].name == 'gpu-exact-collector'
    return code, json.loads((tmp_path / 'state.json').read_text()), events, signals


@pytest.mark.parametrize('backend', ['pymoo', 'gpu'])
def test_local_convergence_native_shutdown_under_asyncio(tmp_path, monkeypatch, backend):
    """Persist after full record; use CPU exception/GPU latch and prove flush/close/join."""
    from optimize_convergence import convergence_complete
    code, state, events, signals = _local_native(tmp_path, monkeypatch, backend=backend)
    assert code == 130
    assert convergence_complete(state)
    assert state['evaluations'] == (512 if backend == 'gpu' else 384)
    assert events[-5:] == ['native-catch', 'terminate', 'flush', 'close', 'join']
    assert len(signals) == (1 if backend == 'gpu' else 0)
    assert not state['shutdown']['forced']


@pytest.mark.parametrize('fault', ['pool', 'thread', 'persist', 'flush', 'join', 'interrupt'])
def test_local_convergence_failures_never_claim_success(tmp_path, monkeypatch, fault):
    """Missing ownership, wrong thread, disk/cleanup errors and foreign interrupts fail closed."""
    from optimize_convergence import convergence_complete
    code, state, _events, signals = _local_native(tmp_path, monkeypatch, fault=fault)
    assert not convergence_complete(state)
    assert not signals
    if fault in ('flush', 'join', 'interrupt'):
        assert code != 0


def test_exact_snapshot_rejects_incomplete_active_front_and_ignores_stale_files(tmp_path):
    """No silently smaller front or unrelated on-disk candidates can drive convergence."""
    import threading
    from pb8_local_convergence import exact_snapshot
    directory = tmp_path / 'run'
    pareto = directory / 'pareto'
    pareto.mkdir(parents=True)
    good = pareto / 'a.json'
    good.write_text(json.dumps({'metrics': {'constraint_violation': 0, 'unpenalized_objectives': [-1, 2]}}))
    (pareto / 'stale.json').write_text('invalid and irrelevant')
    store = SimpleNamespace(directory=str(directory), n_iters=512, _lock=threading.RLock(),
                            _front=['a'], _entries={'a': str(good)})
    recorder = SimpleNamespace(store=store)
    assert exact_snapshot(recorder, tmp_path)[1:] == (512, [[-1, 2]])
    store._front.append('missing')
    with pytest.raises(KeyError):
        exact_snapshot(recorder, tmp_path)
    store._front = ['a']
    good.unlink()
    good.symlink_to(pareto / 'stale.json')
    with pytest.raises(ValueError):
        exact_snapshot(recorder, tmp_path)


@pytest.mark.parametrize('mode,stopped,compatible', [
    ('checkpoint_resume', False, True), ('checkpoint_resume', True, True),
    ('checkpoint_resume', False, False), ('fresh', False, True), ('pareto_seed', False, True),
])
def test_convergence_resume_watermark_never_consumes_history_twice(tmp_path, monkeypatch, mode, stopped, compatible):
    """Restored counts are watermarks; only new exact records advance preserved patience."""
    import threading
    import pb8_local_convergence as local
    from optimize_convergence import advance, DEFAULTS
    directory = tmp_path / 'run'
    (directory / 'pareto').mkdir(parents=True)
    point = directory / 'pareto' / 'a.json'
    point.write_text(json.dumps({'metrics': {'constraint_violation': 0, 'unpenalized_objectives': [1]}}))
    store = SimpleNamespace(directory=str(directory), n_iters=1000, _front=['a'],
        _entries={'a': str(point)}, _lock=threading.RLock())
    recorder = SimpleNamespace(store=store, scoring_specs=[SimpleNamespace(to_config=lambda: {'metric': 'x'})])
    options = {'mode': 'fresh', 'convergence_config': {**DEFAULTS, 'convergence_enabled': True}}
    monkeypatch.setattr(local, '_log', lambda *a, **k: None)
    first = local.LocalConvergence(SimpleNamespace(), options, tmp_path / 'first.json', tmp_path)
    first.attach(recorder)
    first.observed = 700
    first.state = advance({}, [[1]], 512, first.config)
    first.state = advance(first.state, [[1]], 700, first.config)
    if stopped:
        first.state = advance(first.state, [[1]], 1024, first.config)
        first.observed = 1024
        first.shutdown['complete'] = True
    first.persist()
    historical = (directory / local.STATE_FILENAME).read_bytes()
    options = {**options, 'mode': mode, 'convergence_identity': None if compatible else 'changed'}
    resumed = local.LocalConvergence(SimpleNamespace(), options, tmp_path / 'second.json', tmp_path)
    resumed.attach(recorder)
    initial_observed = resumed.observed
    if mode == 'checkpoint_resume' and stopped and compatible:
        assert 'new convergence window' in resumed.state['reason']
    assert 'checked_at' not in resumed.state  # Restoration alone is not a new detector check.
    assert initial_observed == (700 if mode == 'checkpoint_resume' and compatible and not stopped else 0)
    assert not resumed.observe(recorder)  # Identical snapshot, even after a restart.
    assert resumed.observed == initial_observed
    resumed.pool_by_recorder[id(recorder)] = object()
    store.n_iters += 1
    assert not resumed.observe(recorder)
    assert resumed.observed == initial_observed + 1
    if initial_observed:
        assert resumed.state['last_improvement_exact'] == 512
        assert resumed.state['stalled_exact'] == 189
    if mode == 'checkpoint_resume' and stopped:
        assert resumed.previous_completion['completion_reason'] == 'convergence'
        assert b'stop_requested' in historical
    before = dict(resumed.state)
    # Proxy progress is deliberately absent from the Recorder: repeated observations do nothing.
    for _ in range(5):
        assert not resumed.observe(recorder)
    assert resumed.state == before


def test_missing_native_contract_is_unavailable_and_preserves_normal_run(tmp_path, monkeypatch):
    """An older native install cannot enable a guessed interrupt fallback."""
    import pb8_local_convergence as local
    monkeypatch.setattr(local, '_log', lambda *a, **k: None)
    monkeypatch.setattr(local.importlib, 'import_module', lambda _: SimpleNamespace())
    monitor = local.LocalConvergence(SimpleNamespace(), {'convergence_config': {'convergence_enabled': True}},
                                    tmp_path / 'state.json', tmp_path)
    monitor.install()
    assert monitor.state['phase'] == 'unavailable'
    assert not monitor.stop_sent
    assert not monitor.restores
    assert not monitor.finish(0, '')['shutdown']['complete']


@pytest.mark.parametrize('marker_error', [False, True])
def test_watchdog_bounds_and_pid_verification_use_only_mocked_processes(tmp_path, monkeypatch, marker_error):
    """TERM/KILL deadlines survive parent termination and never signal a recycled child PID."""
    import signal
    import pb8_convergence_watchdog as watchdog
    calls, waits = [], []
    class Process:
        """No process or signal is created by this test."""
        def __init__(self, pid, created):
            self.pid, self.created = pid, created
        def create_time(self):
            return self.created
        def children(self, recursive):
            return [child]
        def send_signal(self, sig):
            calls.append((self.pid, sig))
            if self.pid == 10:
                self.created = 9  # A reused child must survive the second phase.
    parent, child = Process(20, 2), Process(10, 1)
    monkeypatch.setattr(watchdog.psutil, 'Process', lambda pid: parent if pid == 20 else child)
    monkeypatch.setattr(watchdog.os, 'getpid', lambda: 30)
    forced = tmp_path / 'forced.json'
    if marker_error:
        def fail_marker(*_args):
            """Model a full disk without touching runtime storage."""
            raise OSError('isolated disk full')
        monkeypatch.setattr(watchdog, 'atomic_write_private_text', fail_marker)
    assert watchdog.watch(20, 2, forced, wait=lambda seconds: waits.append(seconds) or False) == 1
    assert waits == [120, 10]
    assert calls == [(10, signal.SIGTERM), (20, signal.SIGTERM), (20, signal.SIGKILL)]
    if marker_error:
        assert not forced.exists()
    else:
        assert json.loads(forced.read_text())['shutdown']['forced']
    calls.clear()
    assert watchdog.watch(20, 2, tmp_path / 'unused', wait=lambda _: True) == 0
    assert not calls
    assert not (tmp_path / 'unused').exists()


def test_watchdog_error_exit_excludes_convergence_even_without_forced_marker(tmp_path, monkeypatch):
    """A late successful native cleanup cannot hide escalation with failed marker persistence."""
    import pb8_local_convergence as local
    from optimize_convergence import convergence_complete
    monkeypatch.setattr(local, '_log', lambda *a, **k: None)
    monitor = local.LocalConvergence(SimpleNamespace(), {}, tmp_path / 'state.json', tmp_path)
    monitor.stop_sent = True
    monitor.state.update(stop_requested=True, request_run_id=monitor.run_id)
    monitor.shutdown.update(native_complete=True, flush=True, close=True, terminated=True, joined=True)
    monitor.watchdog = SimpleNamespace(returncode=1, communicate=lambda **_kwargs: None)
    state = monitor.finish(130, '')
    assert state['shutdown']['errors']
    assert not state['shutdown']['complete']
    assert not convergence_complete({**state, 'returncode': 130})


@pytest.mark.parametrize('kind', ['invalid', 'infeasible', 'liquidated', 'unsupported', 'missing'])
def test_local_invalid_front_waits_without_erasing_baseline(tmp_path, monkeypatch, kind):
    """Invalid and incomplete fronts suspend stopping; exact-only normalization stays frozen."""
    import threading
    import pb8_local_convergence as local
    directory = tmp_path / 'run'
    (directory / 'pareto').mkdir(parents=True)
    point = directory / 'pareto' / 'a.json'
    metrics = {'constraint_violation': 0, 'unpenalized_objectives': [1]}
    point.write_text(json.dumps({'metrics': metrics}))
    recorder = SimpleNamespace(scoring_specs=[SimpleNamespace(to_config=lambda: {'metric': 'x'})],
        store=SimpleNamespace(directory=str(directory), n_iters=0, _front=['a'],
                              _entries={'a': str(point)}, _lock=threading.RLock()))
    monitor = local.LocalConvergence(SimpleNamespace(), {'convergence_config': {
        'convergence_enabled': True, 'convergence_min_exact': 256, 'convergence_patience': 128}},
        tmp_path / 'state.json', tmp_path)
    monkeypatch.setattr(local, '_log', lambda *a, **k: None)
    monitor.attach(recorder)
    monitor.pool_by_recorder[id(recorder)] = object()
    recorder.store.n_iters = 256
    assert not monitor.observe(recorder)
    scale = monitor.state['scale']
    if kind == 'invalid':
        metrics['unpenalized_objectives'] = [float('nan')]
    elif kind == 'infeasible':
        metrics['constraint_violation'] = 1
    elif kind == 'liquidated':
        metrics['liquidated'] = True
    elif kind == 'unsupported':
        metrics['unpenalized_objectives'] = [1, 2, 3, 4]
    point.write_text(json.dumps({'metrics': metrics}))
    if kind == 'missing':
        point.unlink()
    recorder.store.n_iters = 1024
    monitor.last_check = None
    assert not monitor.observe(recorder)
    assert monitor.state['phase'] == 'waiting'
    assert monitor.state['reason']
    assert monitor.state['scale'] == scale
    assert not monitor.state.get('stop_requested')


@pytest.mark.parametrize('gpu_threads,expected_exact', [('collector', 512), ('concurrent', 640)])
def test_gpu_collector_and_main_share_one_durable_stagnation_stop(tmp_path, monkeypatch, gpu_threads, expected_exact):
    """Background exact results converge; concurrent dispatch yields one SIGINT and one watchdog."""
    import signal
    from optimize_convergence import convergence_complete
    code, state, events, signals = _local_native(tmp_path, monkeypatch, backend='gpu', gpu_threads=gpu_threads)
    assert code == 130
    assert signals == [signal.SIGINT]
    assert convergence_complete(state)
    assert state['evaluations'] == expected_exact
    assert state['convergence']['checked_exact'] == 384
    assert state['convergence']['stalled_exact'] == 128
    assert state['convergence']['last_improvement_exact'] == 256
    assert events[-5:] == ['native-catch', 'terminate', 'flush', 'close', 'join']
    assert not state['shutdown']['forced']


def test_gpu_collector_sigint_reaches_main_thread_latch_in_isolated_process(tmp_path):
    """A child-only real SIGINT proves Python dispatches the collector request on main."""
    import subprocess
    from pathlib import Path
    script = '''
import json
from pathlib import Path
import runpy
import sys
import pytest

helper = runpy.run_path(sys.argv[1])['_local_native']
with pytest.MonkeyPatch.context() as patch:
    code, state, events, signals = helper(Path(sys.argv[2]), patch, backend='gpu',
                                         gpu_threads='collector', real_signals=True)
(Path(sys.argv[2]) / 'child-proof.json').write_text(json.dumps({'code': code, 'state': state,
                                                            'signals': signals}))
'''
    result = subprocess.run([sys.executable, '-c', script, str(Path(__file__).resolve()), str(tmp_path)],
                            cwd=Path(__file__).resolve().parents[1], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr[-4000:]
    from optimize_convergence import convergence_complete
    proof = json.loads((tmp_path / 'child-proof.json').read_text())
    assert proof['code'] == 130
    assert len(proof['signals']) == 1
    assert convergence_complete(proof['state'])


def test_cpu_background_observation_postpones_without_disabling_detection(tmp_path, monkeypatch):
    """Wrong-thread CPU calls neither signal nor consume evidence; main can check it later."""
    import threading
    import pb8_local_convergence as local
    directory = tmp_path / 'run'
    (directory / 'pareto').mkdir(parents=True)
    point = directory / 'pareto' / 'a.json'
    point.write_text(json.dumps({'metrics': {'constraint_violation': 0, 'unpenalized_objectives': [1]}}))
    recorder = SimpleNamespace(scoring_specs=[SimpleNamespace(to_config=lambda: {'metric': 'x'})],
        store=SimpleNamespace(directory=str(directory), n_iters=0, _front=['a'],
                              _entries={'a': str(point)}, _lock=threading.RLock()))
    monitor = local.LocalConvergence(SimpleNamespace(), {'convergence_config': {
        'convergence_enabled': True, 'convergence_min_exact': 256, 'convergence_patience': 128}},
        tmp_path / 'state.json', tmp_path)
    monkeypatch.setattr(local, '_log', lambda *a, **k: None)
    monitor.attach(recorder)
    monitor.pool_by_recorder[id(recorder)] = object()
    recorder.store.n_iters = 256
    results = []
    worker = threading.Thread(target=lambda: results.append(monitor.observe(recorder)), name='cpu-test-worker')
    worker.start()
    worker.join(timeout=5)
    assert not worker.is_alive()
    assert results == [False]
    assert monitor.unavailable is None
    assert monitor.observed == 0 and monitor.last_check is None
    assert 'postponed' in monitor.state['reason']
    assert not monitor.stop_sent and monitor.watchdog is None
    assert not monitor.observe(recorder)
    assert monitor.state['phase'] == 'tracking'
    assert monitor.state['checked_exact'] == 256
    assert monitor.state['last_improvement_exact'] == 256
