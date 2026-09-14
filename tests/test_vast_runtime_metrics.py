"""Offline utilization probes with synthetic cgroup and NVIDIA responses."""

import json
from types import SimpleNamespace

import pytest

from vast_runtime_metrics import PROBE, normalize_sample, sample_metrics


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
    monkeypatch.setattr(subprocess, 'run', lambda *a, **kw: SimpleNamespace(returncode=0, stdout='75, 1024, 24576\n'))
    exec(PROBE, {})
    data = json.loads(capsys.readouterr().out)
    assert data['cpu_cores'] == 4
    assert data['cpu_seconds'] == 10
    assert data['ram_used_bytes'] == 1073741824
    assert data['ram_total_bytes'] == 4294967296
    assert data['gpu_percent'] == 75
    assert data['vram_total_bytes'] == 24 * 1073741824


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
    data = normalize_sample({'gpu_percent': float('nan'), 'ram_used_bytes': -1, 'secret': 'unused'}, {}, 10)
    assert 'gpu_percent' not in data and 'ram_used_bytes' not in data and 'secret' not in data
