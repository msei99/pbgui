"""Offline utilization probes with synthetic cgroup and NVIDIA responses."""

import json
import subprocess
import sys
import time
from types import SimpleNamespace

import pytest

from vast_runtime_metrics import (
    OPTIMIZER_PROBE,
    PROBE,
    normalize_optimizer_observation,
    normalize_sample,
    observe_optimizer,
    sample_metrics,
)


def test_cpu_usage_is_relative_to_allocated_cores():
    """One busy core out of four is 25 percent, independent of host core count."""
    first = normalize_sample({'clock': 100, 'cpu_seconds': 10, 'cpu_cores': 4}, {}, 1000)
    assert 'cpu_percent' not in first
    second = normalize_sample({'clock': 110, 'cpu_seconds': 20, 'cpu_cores': 4}, first, 1010)
    assert second['cpu_percent'] == 25
    assert 'cpu_percent' not in normalize_sample({'clock': 120, 'cpu_seconds': 1, 'cpu_cores': 4}, second, 1020)
    assert 'cpu_percent' not in normalize_sample({'clock': 120, 'cpu_seconds': 30, 'cpu_cores': 8}, second, 1020)


def test_probe_uses_container_limits_and_gpu_memory(monkeypatch, capsys):
    """Execute the actual remote probe against synthetic files and commands only."""
    from pathlib import Path
    import os
    import subprocess
    files = {'/sys/fs/cgroup/cpu.stat': 'usage_usec 10000000\n',
             '/sys/fs/cgroup/cpu.max': '400000 100000',
             '/sys/fs/cgroup/memory.current': '1073741824',
             '/sys/fs/cgroup/memory.max': '4294967296',
             '/proc/meminfo': 'MemTotal: 8388608 kB\n'}
    monkeypatch.setattr(Path, 'exists', lambda p: str(p) in files)
    monkeypatch.setattr(Path, 'read_text', lambda p: files[str(p)])
    monkeypatch.setattr(os, 'sched_getaffinity', lambda _: set(range(64)))
    monkeypatch.setattr(subprocess, 'run', lambda *a, **kw: SimpleNamespace(returncode=0, stdout='75, 1024, 24576, 89.5, 170\n'))
    exec(PROBE, {})
    data = json.loads(capsys.readouterr().out)
    assert data['cpu_cores'] == 4
    assert data['cpu_seconds'] == 10
    assert data['ram_used_bytes'] == 1073741824
    assert data['ram_total_bytes'] == 4294967296
    assert data['gpu_percent'] == 75
    assert data['vram_total_bytes'] == 24 * 1073741824
    assert data['gpu_power_watts'] == 89.5
    assert data['gpu_power_limit_watts'] == 170


def test_unavailable_sample_does_not_fail_job(monkeypatch):
    """Telemetry errors retain old data as unavailable without changing job status."""
    from vast_provider import VastError
    monkeypatch.setattr('vast_runtime_metrics._log', lambda *args, **kwargs: None)
    class Store:
        """A state-only fake store."""
        def __init__(self):
            self.row = {'status': 'running', 'runtime_metrics': {'available': True, 'sampled_at': 1, 'gpu_percent': 50}}
        def read(self, identifier):
            return self.row
        def update(self, identifier, **values):
            self.row.update(values)
    def command(*args, **kwargs):
        """Simulate a transient SSH timeout."""
        raise VastError('timeout')
    store = Store()
    sample_metrics(SimpleNamespace(command=command), store, 'a'*32)
    assert store.row['status'] == 'running'
    assert not store.row['runtime_metrics']['available']
    assert store.row['runtime_metrics']['sampled_at'] == 1


def test_nonfinite_and_private_fields_are_not_exposed():
    """Only finite, allowlisted measurement fields can reach the browser."""
    data = normalize_sample({'gpu_percent': float('nan'), 'ram_used_bytes': -1,
                             'gpu_power_watts': 89.5, 'gpu_power_limit_watts': 170,
                             'secret': 'unused'}, {}, 10)
    assert 'gpu_percent' not in data and 'ram_used_bytes' not in data and 'secret' not in data
    assert data['gpu_power_watts'] == 89.5 and data['gpu_power_limit_watts'] == 170


def test_optimizer_observation_keeps_safe_progress_and_local_log(tmp_path):
    """An external optimizer becomes visible without exposing its remote paths."""
    class Store:
        """Minimal worker state with the production log-root layout."""
        root = tmp_path / 'vast'

        def __init__(self):
            self.row = {}

        def read(self, _identifier):
            return self.row

        def update(self, _identifier, **values):
            self.row.update(values)
            return self.row

    log = ('2026-09-20T20:04:17Z INFO GPU proxy dispatch progress | '
           'strategy=ema_anchor chunks=1/2 candidates=4096/8192 elapsed=80.0s eta=80.0s\n'
           'Iter: 31\n')
    payload = json.dumps({'running': True, 'pid': 16314, 'name': 'perf-halving', 'log': log}).encode()
    commands = []
    def command(value, **_kwargs):
        """Capture the fixed remote probe invocation."""
        commands.append(value)
        return payload
    connection = SimpleNamespace(command=command)
    store = Store()
    observed = observe_optimizer(connection, store, 'a' * 32)
    assert commands and '/usr/local/bin/python -c ' in commands[0]
    assert observed['pid'] == 16314
    assert observed['name'] == 'perf-halving'
    assert observed['exact_completed'] == 31
    assert observed['activity'].endswith('elapsed=80.0s eta=80.0s')
    assert set(observed) == {'running', 'pid', 'name', 'sampled_at', 'activity',
                             'exact_completed', 'gpu_candidates'}
    assert (tmp_path / 'logs/optimizes_v8' / ('vast_' + 'a' * 32 + '.log')).read_text() == log


def test_optimizer_observation_waits_neutrally_for_native_progress():
    """A stock PB8 log remains observable before its first progress message."""
    raw = {
        'running': True,
        'pid': 16314,
        'name': 'stock-pb8',
        'log': (
            '2026-09-20T20:04:17Z INFO GPU optimizer generated reproducible seed 7\n'
            '2026-09-20T20:04:18Z INFO No starting configs provided; '
            'population will be random-initialized\n'
        ),
    }

    observed, log = normalize_optimizer_observation(raw, 123.0)

    assert observed == {
        'running': True,
        'pid': 16314,
        'name': 'stock-pb8',
        'sampled_at': 123.0,
        'activity': 'Optimizer process detected',
        'exact_completed': 0,
        'gpu_candidates': 0,
    }


def test_optimizer_observation_accepts_current_pb8_temporal_replay():
    """Current upstream PB8 replay messages drive the rental-card activity."""
    raw = {
        'running': True,
        'pid': 16314,
        'name': 'current-pb8',
        'log': (
            '2026-09-21T10:00:00Z INFO GPU temporal replay progress | '
            'suite_pass=3/7 scenarios=train_03 exchange=combined history=50.0% '
            'replay=13 candidates=4096 bars=500000/1000000 elapsed=80.0s\n'
        ),
    }

    observed, log = normalize_optimizer_observation(raw, 123.0)

    assert observed['activity'].startswith('GPU temporal replay progress |')
    assert observed['gpu_candidates'] == 4096
    assert log == raw['log']


def test_optimizer_probe_resolves_relative_config_from_process_cwd(tmp_path, capsys):
    """A directly started optimizer may pass optimize.json relative to its own cwd."""
    worker_root = tmp_path / 'pbgui'
    run_dir = worker_root / 'perf-relative'
    output_dir = run_dir / 'output'
    output_dir.mkdir(parents=True)
    (run_dir / 'optimize.json').write_text('{}\n', encoding='utf-8')
    optimize_script = run_dir / 'optimize.py'
    optimize_script.write_text('import time\ntime.sleep(30)\n', encoding='utf-8')
    log_path = output_dir / 'optimizer.log'
    with log_path.open('wb') as log_stream:
        process = subprocess.Popen(
            [sys.executable, str(optimize_script), 'optimize.json'],
            cwd=run_dir,
            stdout=log_stream,
            stderr=subprocess.STDOUT,
        )
        try:
            time.sleep(0.05)
            source = OPTIMIZER_PROBE.replace(
                "Path('/work/pbgui').resolve()",
                f"Path({str(worker_root)!r}).resolve()",
            )
            exec(source, {})
        finally:
            process.terminate()
            process.wait(timeout=5)
    observed = json.loads(capsys.readouterr().out)
    assert observed['running'] is True
    assert observed['pid'] == process.pid
    assert observed['name'] == 'perf-relative'


@pytest.mark.parametrize('raw', [
    {'running': False},
    {'running': True, 'pid': 0, 'name': 'bad', 'log': ''},
    {'running': True, 'pid': 1, 'name': '', 'log': ''},
])
def test_optimizer_observation_rejects_invalid_public_state(raw):
    """Missing and malformed process records cannot reach the browser."""
    if raw.get('running') is False:
        assert normalize_optimizer_observation(raw, 1) == (None, None)
    else:
        with pytest.raises(ValueError):
            normalize_optimizer_observation(raw, 1)


def test_failed_optimizer_probe_expires_stale_observation(monkeypatch):
    """A dead observer cannot leave a stopped optimizer displayed indefinitely."""
    from vast_provider import VastError
    monkeypatch.setattr('vast_runtime_metrics._log', lambda *args, **kwargs: None)
    monkeypatch.setattr('vast_runtime_metrics.time.time', lambda: 100)

    class Store:
        """Minimal persistent worker state."""
        def __init__(self):
            self.row = {'observed_optimizer': {'running': True, 'sampled_at': 1}}

        def read(self, _identifier):
            return self.row

        def update(self, _identifier, **values):
            self.row.update(values)
            return self.row

    store = Store()
    connection = SimpleNamespace(command=lambda *args, **kwargs: (_ for _ in ()).throw(VastError('timeout')))
    assert observe_optimizer(connection, store, 'a' * 32) is None
    assert store.row['observed_optimizer'] is None
