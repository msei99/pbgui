"""Owned local exact snapshots, durable convergence and verified graceful shutdown."""
from __future__ import annotations

import hashlib
import importlib
import inspect
import json
import logging
import math
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time
import uuid

import psutil

from logging_helpers import human_log as _log
from optimize_convergence import advance, validate_config
from secure_files import atomic_write_private_text

SERVICE = 'OptimizeV8'
STATE_FILENAME = '.pbgui_convergence.json'


class ContractUnavailable(ValueError):
    """The installed native runner cannot supply a safe automatic stop contract."""


def read_state(path):
    """Read bounded non-secret state; malformed state never supplies a stop decision."""
    path = Path(path)
    if not path.exists():
        return {}
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 1024 * 1024:
        raise ValueError('Invalid convergence state file')
    value = json.loads(path.read_text(encoding='utf-8'))
    if not isinstance(value, dict):
        raise ValueError('Invalid convergence state')
    return value


def compatible_history(previous, signature, directory):
    """Reject corrupt or incompatible persisted normalization and observation counters."""
    if (previous.get('version') != 1 or previous.get('signature') != signature
            or previous.get('result') != str(directory)):
        return False
    observed = previous.get('observed_exact')
    state = previous.get('convergence')
    if type(observed) is not int or observed < 0 or not isinstance(state, dict):
        return False
    for key in ('checked_exact', 'last_improvement_exact'):
        value = state.get(key, 0)
        if type(value) is not int or not 0 <= value <= observed:
            return False
    if 'scale' in state:
        dimensions = state.get('dimensions')
        if type(dimensions) is not int or not 1 <= dimensions <= 3:
            return False
        for key in ('scale', 'origin', 'reference'):
            values = state.get(key)
            if not isinstance(values, list) or len(values) != dimensions or any(
                    type(x) not in (float, int) or not math.isfinite(x) or (key == 'scale' and x <= 0)
                    for x in values):
                return False
        baseline = state.get('baseline_hv')
        if type(baseline) not in (float, int) or not math.isfinite(baseline) or baseline < 0:
            return False
    return True


def exact_snapshot(recorder, results_root):
    """Capture the complete active Store front and exact count under its own lock."""
    store = recorder.store
    directory = Path(store.directory)
    root = Path(results_root).resolve()
    if directory.is_symlink() or directory.parent.resolve() != root or not directory.is_dir():
        raise ValueError('Unsupported result directory')
    pareto = directory / 'pareto'
    if pareto.is_symlink() or not pareto.is_dir():
        raise ValueError('Invalid Pareto directory')
    points = []
    with store._lock:
        exact = store.n_iters
        if type(exact) is not int or exact < 0:
            raise ValueError('Invalid exact evaluation count')
        members = list(store._front)
        if len(set(members)) != len(members):
            raise ValueError('Invalid active front membership')
        for member in members:
            path = Path(store._entries[member])
            if (path.is_symlink() or path.parent.resolve() != pareto.resolve()
                    or not path.is_file() or path.stat().st_size > 4 * 1024**2):
                raise ValueError('Incomplete or invalid active exact front')
            metrics = json.loads(path.read_text(encoding='utf-8'))['metrics']
            violation = metrics['constraint_violation']
            values = metrics['unpenalized_objectives']
            if type(violation) not in (float, int) or not math.isfinite(violation):
                raise ValueError('Invalid constraint violation')
            if not isinstance(values, list) or not values or any(
                    type(x) not in (int, float) or not math.isfinite(x) for x in values):
                raise ValueError('Invalid signed exact objectives')
            liquidated = metrics.get('liquidated', False)
            if type(liquidated) is not bool:
                raise ValueError('Invalid liquidation flag')
            if violation <= 0 and not liquidated:
                points.append(values)
    return directory.resolve(), exact, points


class _ShutdownEvidence(logging.Handler):
    """Observe native cleanup diagnostics without changing PB8's logger or handlers."""

    def __init__(self, owner):
        super().__init__()
        self.owner = owner

    def emit(self, record):
        """Retain only fixed evidence flags; never copy arbitrary native log payloads."""
        if record.levelno >= logging.ERROR:
            self.owner.shutdown['errors'] = True
        if record.getMessage().strip() == 'Shutdown complete.':
            self.owner.shutdown['native_complete'] = True


class LocalConvergence:
    """One detached runner owns all hooks, state and its bounded stop watchdog."""

    def __init__(self, module, options, state_path, results_root):
        self.module = module
        self.options = options
        self.config = validate_config(options.get('convergence_config'))
        self.state_path = Path(state_path).with_suffix('.convergence.json')
        self.results_root = Path(results_root)
        self.run_id = uuid.uuid4().hex
        self.state = {'phase': 'warming', 'reason': 'Waiting for new exact evaluations'}
        self.signature = None
        self.directory = None
        self.recorder = None
        self.initial_exact = 0
        self.observed = 0
        self.last_exact = 0
        self.last_check = None
        self._observation_lock = threading.RLock()
        self.pool_by_recorder = {}
        self.restores = []
        self.shutdown = {'errors': False, 'forced': False}
        self.done = threading.Event()
        self.watchdog = None
        self.stop_sent = False
        self.backend = options.get('convergence_backend', 'pymoo')
        self.interrupted_class = None
        self.latch_class = None
        self.handler = None
        self.unavailable = None
        self.evaluations = None
        self.previous_completion = None

    def log(self, message, level='INFO'):
        """Publish convergence in the existing managed optimizer transcript."""
        _log(SERVICE, 'Convergence: ' + message, level=level, logfile=self.options.get('convergence_log'))

    def persist(self):
        """Save the result-owned history before publishing the API projection."""
        payload = {'version': 1, 'run_id': self.run_id, 'signature': self.signature,
                   'result': str(self.directory or ''), 'config': self.config,
                   'observed_exact': self.observed, 'evaluations': self.evaluations,
                   'convergence': self.state, 'shutdown': self.shutdown,
                   'previous_completion': self.previous_completion}
        serialized = json.dumps(payload, indent=4, allow_nan=False) + '\n'
        if self.directory is not None:
            atomic_write_private_text(self.directory / STATE_FILENAME, serialized)
        atomic_write_private_text(self.state_path, serialized)

    def patch(self, obj, name, value):
        """Restore every temporary hook in reverse installation order."""
        original = getattr(obj, name)
        owned_attribute = name in getattr(obj, '__dict__', {})
        setattr(obj, name, value)
        self.restores.append((obj, name, original, owned_attribute))
        return original

    def install(self):
        """Check installed native contracts before allowing automatic termination."""
        try:
            interrupts = importlib.import_module('optimization.interrupts')
            self.interrupted_class = interrupts.OptimizerBackendInterrupted
            self.latch_class = interrupts.OptimizerInterruptLatch
            source = inspect.getsource(self.module.main)
            if not issubclass(self.interrupted_class, KeyboardInterrupt) or not all(
                    marker in source for marker in ['except KeyboardInterrupt', '"pool"',
                                                     '"pool_terminated"', 'Shutdown complete.']):
                raise ValueError('Native graceful shutdown contract unavailable')
            recorder_class = self.module.ResultRecorder
            self.module._terminate_optimizer_pool
            if self.backend not in ('pymoo', 'gpu'):
                raise ValueError('Unsupported local optimizer backend')
            if self.backend == 'pymoo':
                runner_class = importlib.import_module('optimization.problem').PymooAsyncRecordingRunner
                original_runner_init = runner_class.__init__

                def init_runner(instance, *args, **kwargs):
                    original_runner_init(instance, *args, **kwargs)
                    recorder, pool = instance.recorder, instance.pool
                    previous = self.pool_by_recorder.setdefault(id(recorder), pool)
                    if previous is not pool:
                        self.pool_by_recorder[id(recorder)] = None

                self.patch(runner_class, '__init__', init_runner)
            original_init = recorder_class.__init__
            original_record = recorder_class.record

            def init_recorder(instance, *args, **kwargs):
                original_init(instance, *args, **kwargs)
                self.attach(instance)

            def record(instance, data):
                # Serialize the completed Recorder boundary as well as its snapshot:
                # GPU collector and seed/main paths share the same result stream.
                with self._observation_lock:
                    result = original_record(instance, data)
                    stop = self.observe(instance)
                # The intentional CPU BaseException is outside all general handlers.
                # Both native stop paths also run outside our observation/store locks.
                if stop:
                    self.request_stop(instance)
                return result

            self.patch(recorder_class, '__init__', init_recorder)
            self.patch(recorder_class, 'record', record)
            for method in ('flush', 'close'):
                original = getattr(recorder_class, method)

                def cleanup(instance, *args, _name=method, _original=original, **kwargs):
                    try:
                        result = _original(instance, *args, **kwargs)
                    except BaseException:
                        self.shutdown['errors'] = True
                        raise
                    if instance is self.recorder:
                        self.shutdown[_name] = True
                    return result

                self.patch(recorder_class, method, cleanup)
            original_terminate = self.module._terminate_optimizer_pool

            def terminate(pool, terminated):
                if pool is not None and not self.shutdown.get('pool_hooked'):
                    original_join = pool.join

                    def join(*args, **kwargs):
                        result = original_join(*args, **kwargs)
                        self.shutdown['joined'] = True
                        return result

                    self.patch(pool, 'join', join)
                    self.shutdown['pool_hooked'] = True
                result = original_terminate(pool, terminated)
                self.shutdown['terminated'] = bool(result)
                return result

            self.patch(self.module, '_terminate_optimizer_pool', terminate)
            self.handler = _ShutdownEvidence(self)
            logging.getLogger().addHandler(self.handler)
        except (ImportError, AttributeError, TypeError, ValueError, OSError) as exc:
            self.restore()
            self.unavailable = str(exc)
            self.state.update(phase='unavailable', reason=self.unavailable)
            self.log(self.unavailable + '; normal optimizer run continues', 'WARNING')
        try:
            self.persist()
        except (OSError, ValueError, TypeError) as exc:
            self.unavailable = 'Convergence state persistence unavailable: ' + str(exc)
            self.state.update(phase='unavailable', reason=self.unavailable)
            self.log(self.unavailable + '; normal optimizer run continues', 'WARNING')

    def attach(self, recorder):
        """Treat restored Recorder progress as a watermark, never as new evidence."""
        try:
            if self.recorder is not None:
                raise ValueError('Multiple recorders are unsupported')
            self.recorder = recorder
            directory, exact, _points = exact_snapshot(recorder, self.results_root)
            self.directory = directory
            self.initial_exact = self.last_exact = exact
            self.evaluations = exact
            specs = [spec.to_config() for spec in recorder.scoring_specs]
            self.signature = hashlib.sha256(json.dumps(
                [self.config, specs, self.options.get('convergence_identity')],
                sort_keys=True, allow_nan=False).encode()).hexdigest()
            reason = 'Fresh convergence history'
            if self.options.get('mode') == 'checkpoint_resume':
                try:
                    previous = read_state(directory / STATE_FILENAME)
                except (OSError, ValueError, TypeError) as exc:
                    previous = {}
                    self.log('Cannot restore convergence history: ' + str(exc), 'WARNING')
                signatures = [self.signature]
                # Migration never trusts a saved opaque hash as evidence of compatibility.
                # Only identities independently rebuilt from validated checkpoint configs qualify.
                for identity in self.options.get('convergence_legacy_identities', []):
                    signatures.append(hashlib.sha256(json.dumps(
                        [self.config, specs, identity], sort_keys=True, allow_nan=False).encode()).hexdigest())
                if any(compatible_history(previous, signature, directory) for signature in signatures):
                    self.state = dict(previous['convergence'])
                    self.observed = int(previous['observed_exact'])
                    self.previous_completion = previous.get('previous_completion')
                    stopped = self.state.pop('stop_requested', False)
                    if stopped and previous.get('shutdown', {}).get('complete'):
                        self.previous_completion = {'run_id': previous['run_id'],
                            'completion_reason': 'convergence', 'convergence': dict(previous['convergence'])}
                        self.state = {'phase': 'warming', 'reason': 'Explicit resume starts a new convergence window'}
                        self.observed = 0
                        reason = 'Explicit resume starts a new convergence window; waiting for new exact evaluations'
                    else:
                        reason = 'Compatible convergence history restored; waiting for new exact evaluations'
                else:
                    reason = 'Missing or incompatible convergence history; waiting for new exact evaluations'
            self.state.update(reason=reason, restored_at=time.time())
            self.persist()
            self.log(reason)
        except (OSError, ValueError, TypeError, KeyError, AttributeError) as exc:
            self.unavailable = 'Recorder/store contract unavailable: ' + str(exc)
            self.state.update(phase='unavailable', reason=self.unavailable)
            self.log(self.unavailable, 'WARNING')

    def observe(self, recorder):
        """Only a new completed exact record may advance observation progress."""
        with self._observation_lock:
            return self._observe_locked(recorder)

    def _observe_locked(self, recorder):
        """Own the exact counter, minute gate and persisted decision as one operation."""
        if self.unavailable or recorder is not self.recorder or self.stop_sent:
            return False
        try:
            cpu_thread_reason = 'Waiting for main-thread CPU recording; convergence check postponed'
            if self.backend == 'pymoo' and threading.current_thread() is not threading.main_thread():
                if self.state.get('reason') != cpu_thread_reason:
                    self.state.update(phase='waiting', reason=cpu_thread_reason)
                    self.persist()
                    self.log(cpu_thread_reason, 'WARNING')
                return False
            if self.state.get('reason') == cpu_thread_reason:
                self.state.pop('reason', None)
            # Another completed record may arrive between observation and dispatch.
            # Reuse the durable request; request_stop reserves its dispatch once.
            if self.state.get('stop_requested'):
                return self.state.get('request_run_id') == self.run_id
            exact = recorder.store.n_iters
            self.evaluations = exact
            if exact <= self.last_exact:
                return False
            self.observed += exact - self.last_exact
            self.last_exact = exact
            now = time.monotonic()
            if self.last_check is not None and now - self.last_check < 60:
                return False
            self.last_check = now
            if self.backend == 'pymoo' and self.pool_by_recorder.get(id(recorder)) is None:
                raise ContractUnavailable('Unique CPU worker pool unavailable')
            if self.backend == 'gpu':
                handler = signal.getsignal(signal.SIGINT)
                if not isinstance(getattr(handler, '__self__', None), self.latch_class):
                    raise ContractUnavailable('Native GPU interrupt latch unavailable')
            directory, captured_exact, points = exact_snapshot(recorder, self.results_root)
            if directory != self.directory or captured_exact != exact:
                raise ValueError('Inconsistent exact snapshot')
            updated = advance(self.state, points, self.observed, self.config)
            # Invalid local snapshots suspend decisions without discarding the frozen baseline.
            self.state = {**self.state, **updated} if updated.get('phase') == 'waiting' else updated
            self.state.update(checked_at=time.time(), evaluations=exact,
                              patience=self.config['convergence_patience'],
                              minimum=self.config['convergence_min_exact'])
            if self.state.get('phase') in ('tracking', 'stopping'):
                self.state.pop('reason', None)
            if self.state.get('stop_requested'):
                self.state.update(request_run_id=self.run_id, requested_at=time.time())
            self.persist()
            self.log(f"{self.state['phase']} | exact={exact} observed={self.observed} "
                     f"last_improvement={self.state.get('last_improvement_exact', '-')} "
                     f"stagnation={self.state.get('stalled_exact', 0)}/{self.config['convergence_patience']} "
                     f"{self.state.get('reason', '')}")
            return bool(self.state.get('stop_requested'))
        except (OSError, ValueError, TypeError, KeyError, AttributeError) as exc:
            # Incomplete evidence never establishes convergence or authorizes a stop.
            self.state.pop('stop_requested', None)
            self.state.update(phase='waiting', reason='Convergence check unavailable: ' + str(exc))
            if isinstance(exc, ContractUnavailable):
                self.unavailable = self.state['reason']
                self.state['phase'] = 'unavailable'
            self.log(self.state['reason'], 'WARNING')
            try:
                self.persist()
            except (OSError, ValueError, TypeError):
                self.log('State persistence failed; automatic stop suspended', 'ERROR')
            return False

    def request_stop(self, recorder):
        """Leave every snapshot/persistence lock before invoking the native stop path."""
        with self._observation_lock:
            if (self.stop_sent or self.unavailable or recorder is not self.recorder
                    or not self.state.get('stop_requested')
                    or self.state.get('request_run_id') != self.run_id):
                return
            if self.backend == 'pymoo' and threading.current_thread() is not threading.main_thread():
                return
            owned_pool = self.pool_by_recorder.get(id(recorder))
            if self.backend == 'pymoo' and owned_pool is None:
                self.state.pop('stop_requested', None)
                self.unavailable = 'Unique CPU worker pool unavailable'
                self.state.update(phase='unavailable', reason=self.unavailable)
                self.persist()
                self.log(self.unavailable, 'WARNING')
                return
            # A separate process survives this runner's SIGTERM and can enforce SIGKILL.
            process = psutil.Process(os.getpid())
            try:
                self.watchdog = subprocess.Popen(
                    [sys.executable, str(Path(__file__).with_name('pb8_convergence_watchdog.py')),
                     str(process.pid), str(process.create_time()), str(self.state_path.with_suffix('.forced.json'))],
                    stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                )
            except OSError as exc:
                self.state.pop('stop_requested', None)
                self.unavailable = 'Stop watchdog unavailable: ' + str(exc)
                self.state.update(phase='unavailable', reason=self.unavailable)
                self.log(self.unavailable, 'WARNING')
                self.persist()
                return
            self.stop_sent = True
        if self.backend == 'pymoo':
            raise self.interrupted_class(pool=owned_pool, pool_terminated=False)
        os.kill(os.getpid(), signal.SIGINT)

    def finish(self, returncode, error):
        """A request plus native code 130 is insufficient without successful cleanup."""
        if self.recorder is not None:
            exact = getattr(self.recorder.store, 'n_iters', None)
            if type(exact) is int and exact >= 0:
                self.evaluations = exact
        self.done.set()
        if self.watchdog is not None:
            try:
                self.watchdog.communicate(input=b'done', timeout=5)
                if self.watchdog.returncode != 0:
                    self.shutdown['errors'] = True
            except (OSError, subprocess.TimeoutExpired):
                self.watchdog.kill()
                self.watchdog.communicate()
                self.shutdown['errors'] = True
        if self.state_path.with_suffix('.forced.json').exists():
            self.shutdown['forced'] = True
        self.shutdown['complete'] = bool(
            self.stop_sent and returncode == 130 and not error
            and self.shutdown.get('native_complete') and self.shutdown.get('flush')
            and self.shutdown.get('close') and self.shutdown.get('terminated')
            and self.shutdown.get('joined') and not self.shutdown['errors']
            and not self.shutdown['forced'])
        self.restore()
        try:
            self.persist()
        except (OSError, ValueError, TypeError) as exc:
            self.shutdown['complete'] = False
            if self.stop_sent:
                self.shutdown['errors'] = True
            self.state['reason'] = 'Final convergence persistence failed: ' + str(exc)
            self.log(self.state['reason'], 'WARNING')
        payload = {'run_id': self.run_id, 'convergence': self.state,
                   'evaluations': self.evaluations, 'shutdown': self.shutdown}
        if self.shutdown['complete']:
            payload['completion_reason'] = 'convergence'
            self.log('Stopped on stagnation; native graceful shutdown verified')
        elif self.stop_sent:
            self.log('Stop requested, but graceful completion could not be verified', 'ERROR')
        return payload

    def restore(self):
        """Release temporary hooks and ownership even after partial installation."""
        for obj, name, original, owned_attribute in reversed(self.restores):
            if owned_attribute:
                setattr(obj, name, original)
            else:
                delattr(obj, name)
        self.restores.clear()
        self.pool_by_recorder.clear()
        if self.handler is not None:
            logging.getLogger().removeHandler(self.handler)
            self.handler.close()
            self.handler = None
