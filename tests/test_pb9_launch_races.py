"""Native launch/deactivation serialization, with process creation always mocked."""
import json
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

import PBRun as module
import pb9_config
from file_lock import advisory_file_lock
from tests.test_pbrun_v9 import runner


@pytest.fixture
def launch(runner, monkeypatch):
    """Keep real launch gates while isolating native validation, logs and Popen."""
    launches = []
    monkeypatch.setattr(module, '_log', lambda *a, **k: None)
    monkeypatch.setattr(runner, '_runtime_ready', lambda: True)
    monkeypatch.setattr(pb9_config, 'validate_pb9_live_config', lambda *a, **k: {'valid': True})
    monkeypatch.setattr(runner, 'is_running', lambda: bool(launches))
    def popen(command, **kwargs):
        """Record a synthetic child; no executable is started."""
        launches.append(command)
        return SimpleNamespace(stdout=None)
    monkeypatch.setattr(module.subprocess, 'Popen', popen)
    monkeypatch.setattr(module, '_ts_wrap_stderr', lambda *a: None)
    return SimpleNamespace(runner=runner, launches=launches,
                           start=lambda reload=True: module.RunV8.start.__wrapped__(runner, reload_config=reload))


@pytest.mark.parametrize('change', ['disable', 'delete', 'config', 'metadata', 'symlink', 'directory'])
@pytest.mark.parametrize('during_validation', [False, True])
def test_native_launch_rejects_replaced_bundle(launch, monkeypatch, change, during_validation):
    """Watch and default starts cannot publish a stale native document."""
    value = launch.runner
    assert value.load()
    def mutate():
        """Model an atomic API or Cluster publication before process creation."""
        if change == 'delete':
            value.config_path.unlink()
        elif change in ('symlink', 'directory'):
            value.config_path.unlink()
            if change == 'directory':
                value.config_path.mkdir()
            else:
                outside = value.config_path.with_suffix('.other')
                outside.write_text('{}')
                value.config_path.symlink_to(outside)
        elif change == 'config':
            config = json.loads(value.config_path.read_text())
            config['bot']['long']['enabled'] = False
            value.config_path.write_text(json.dumps(config))
        else:
            metadata = json.loads(value.sidecar_path.read_text())
            metadata['enabled_on' if change == 'disable' else 'version'] = 'disabled' if change == 'disable' else 4
            value.sidecar_path.write_text(json.dumps(metadata))
    if during_validation:
        def runtime_ready():
            """Pause-free deterministic interleaving after the initial assignment gate."""
            mutate()
            return True
        monkeypatch.setattr(value, '_runtime_ready', runtime_ready)
    else:
        mutate()
    assert launch.start(during_validation) is False
    assert launch.launches == []
    assert value._crash_count == 0


def test_native_launch_uses_writer_then_account_lock(launch, monkeypatch):
    """Real writer contention postpones launch; after release the next cycle succeeds."""
    value = launch.runner
    acquired, release = threading.Event(), threading.Event()
    def writer():
        """Own the actual API/Cluster transaction lock from a second thread."""
        with advisory_file_lock(Path(value.path).parent / '.write'):
            acquired.set()
            assert release.wait(5)
    worker = threading.Thread(target=writer)
    worker.start()
    try:
        assert acquired.wait(5)
        assert launch.start() is False
        assert launch.launches == []
        assert value._crash_count == 0
    finally:
        release.set()
        worker.join(5)
    assert not worker.is_alive()
    assert launch.start() is True
    assert len(launch.launches) == 1


def test_native_assignment_is_rechecked_after_validation(launch, monkeypatch):
    """An unchanged local bundle cannot bypass a newer Cluster stop decision."""
    calls = []
    def gate():
        """Model a newly stopped desired-state manifest."""
        calls.append(True)
        return {'ok': len(calls) == 1, 'reason': 'Desired state changed'}
    monkeypatch.setattr(launch.runner, '_cluster_gate_result', gate)
    assert launch.start() is False
    assert launch.launches == []
    assert launch.runner.cluster_blocked is True
    assert launch.runner.cluster_blocked_reason == 'Desired state changed'


def test_process_limit_is_a_launch_failure_not_writer_contention(launch, monkeypatch):
    """EAGAIN from Popen must retain normal crash backoff and diagnostics."""
    def unavailable(*args, **kwargs):
        """No real child is created when the process limit is reached."""
        raise BlockingIOError(11, 'Synthetic process limit')
    monkeypatch.setattr(module.subprocess, 'Popen', unavailable)
    assert launch.start() is False
    assert launch.runner._crash_count == 1


def test_native_validation_cache_binds_replaced_file(runner, monkeypatch):
    """Same-size/timestamp replacement must not reuse the previous config's validation."""
    import os
    assert runner.load()
    calls = []
    monkeypatch.setattr(pb9_config, 'validate_pb9_live_config', lambda config, *a, **k: calls.append(config))
    assert runner._config_ready()
    assert len(calls) == 1
    info = runner.config_path.stat()
    replacement = runner.config_path.with_suffix('.new')
    replacement.write_bytes(runner.config_path.read_bytes())
    os.utime(replacement, ns=(info.st_atime_ns, info.st_mtime_ns))
    os.replace(replacement, runner.config_path)
    assert runner.load()
    assert runner._config_ready()
    assert len(calls) == 2
