"""Isolated regressions for runtime availability and direct job status reads."""

import json
from pathlib import Path

import pytest
from fastapi import HTTPException

from api import backtest_v8, jobs, pb7_bridge, strategy_explorer_v8
from pb7_config import PB7ConfigurationError
import pb8_strategy_explorer as explorer
import task_queue


@pytest.mark.parametrize('error', [ImportError('missing dependency'), ModuleNotFoundError('passivbot_rust')])
def test_pb7_import_failure_uses_configuration_contract(monkeypatch, error):
    """Missing import dependencies retain an actionable typed error and its cause."""
    monkeypatch.setattr(pb7_bridge, 'ensure_pb7_src_importable', lambda: None)
    monkeypatch.setattr(pb7_bridge, 'pb7dir', lambda: '/isolated/pb7')
    monkeypatch.setattr(pb7_bridge, '_log', lambda *a, **k: None)

    def fail(_name):
        """Simulate the missing runtime dependency without importing PB7."""
        raise error

    monkeypatch.setattr(pb7_bridge.importlib, 'import_module', fail)
    with pytest.raises(PB7ConfigurationError, match='config.coerce') as caught:
        pb7_bridge.get_hsl_signal_modes()
    assert caught.value.__cause__ is error
    assert str(error) in str(caught.value)


def test_pb7_unexpected_bug_is_not_hidden_as_missing_runtime(monkeypatch):
    """Programming errors must retain their original failure type."""
    monkeypatch.setattr(pb7_bridge, 'ensure_pb7_src_importable', lambda: None)

    def fail(_name):
        """Simulate a programming error within the module import."""
        raise RuntimeError('unexpected bug')

    monkeypatch.setattr(pb7_bridge.importlib, 'import_module', fail)
    with pytest.raises(RuntimeError, match='unexpected bug'):
        pb7_bridge.get_hsl_signal_modes()


def test_pb8_settings_missing_runtime_is_service_unavailable(monkeypatch):
    """Settings surface missing runtime information without reading real settings."""
    monkeypatch.setattr(backtest_v8, '_log', lambda *a, **k: None)

    def fail():
        """Simulate an unconfigured runtime."""
        raise backtest_v8.PB8ConfigurationError('PB8 Rust extension is missing')

    monkeypatch.setattr(backtest_v8, 'get_pb8_exchange_metadata', fail)
    monkeypatch.setattr(backtest_v8, 'load_ini_section', lambda *a: pytest.fail('must not read settings'))
    with pytest.raises(HTTPException) as caught:
        backtest_v8.get_settings(None)
    assert caught.value.status_code == 503
    assert 'Rust extension is missing' in caught.value.detail


@pytest.mark.parametrize('errors,expected', [(['missing Rust', 'missing CLI'], 'missing Rust; missing CLI'), ([], 'PB8 runtime is not ready')])
def test_pb8_explorer_unavailable_runtime_maps_to_503(monkeypatch, errors, expected):
    """The API funnel preserves availability diagnostics rather than returning 422."""
    monkeypatch.setattr(explorer, 'pb8_runtime_status', lambda: {'ready': False, 'errors': errors})
    monkeypatch.setattr(strategy_explorer_v8, '_log', lambda *a, **k: None)
    with pytest.raises(HTTPException) as caught:
        strategy_explorer_v8._call('Runtime', explorer._runtime)
    assert caught.value.status_code == 503
    assert caught.value.detail == expected


def test_pb8_explorer_validation_still_maps_to_422(monkeypatch):
    """Request validation must remain distinguishable from runtime availability."""
    monkeypatch.setattr(strategy_explorer_v8, '_log', lambda *a, **k: None)

    def invalid():
        """Raise a normal validation failure."""
        raise explorer.PB8StrategyExplorerError('invalid config')

    with pytest.raises(HTTPException) as caught:
        strategy_explorer_v8._call('Validation', invalid)
    assert caught.value.status_code == 422


@pytest.mark.parametrize('state', ['pending', 'running', 'done', 'failed'])
def test_single_job_status_never_scans_history(monkeypatch, tmp_path, state):
    """Any state is directly addressable without globbing or applying a history limit."""
    monkeypatch.setattr(task_queue, 'get_market_data_root_dir', lambda: tmp_path)
    path = tmp_path / '_tasks' / state / '100-job.json'
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({'id': '100-job', 'status': state}))
    monkeypatch.setattr(Path, 'glob', lambda *a, **k: pytest.fail('no directory scan allowed'))
    monkeypatch.setattr(jobs, 'list_jobs', lambda **k: pytest.fail('no list query allowed'))
    result = jobs.get_job('100-job', None)
    assert result == {'id': '100-job', 'status': state, '_path': str(path)}


@pytest.mark.parametrize('job_id', ['', '.', '..', '../outside', '/tmp/outside', 'a/b', 'a\\b', 'job\x00', 'job\n', 'job\x7f'])
def test_direct_job_lookup_rejects_unsafe_ids_before_filesystem(monkeypatch, job_id):
    """Unsafe IDs must never reach a filesystem operation."""
    monkeypatch.setattr(task_queue, 'get_tasks_root_dir', lambda: pytest.fail('unsafe path accessed'))
    assert task_queue.get_job_by_id(job_id) is None


@pytest.mark.parametrize('content', ['{broken', '[]', '{"id":"different"}'])
def test_direct_job_lookup_rejects_invalid_payload(monkeypatch, tmp_path, content):
    """Malformed records and mismatched IDs are not returned as valid jobs."""
    monkeypatch.setattr(task_queue, 'get_market_data_root_dir', lambda: tmp_path)
    monkeypatch.setattr(task_queue, '_log', lambda *a, **k: None)
    path = tmp_path / '_tasks' / 'done' / '100-job.json'
    path.parent.mkdir(parents=True)
    path.write_text(content)
    with pytest.raises(HTTPException) as caught:
        jobs.get_job('100-job', None)
    assert caught.value.status_code == 404


def test_direct_job_lookup_does_not_follow_symlinks(monkeypatch, tmp_path):
    """A persisted job name cannot escape the task root through a symlink."""
    monkeypatch.setattr(task_queue, 'get_market_data_root_dir', lambda: tmp_path)
    monkeypatch.setattr(task_queue, '_log', lambda *a, **k: None)
    outside = tmp_path / 'outside.json'
    outside.write_text('{"id":"100-job"}')
    path = tmp_path / '_tasks' / 'done' / '100-job.json'
    path.parent.mkdir(parents=True)
    path.symlink_to(outside)
    assert task_queue.get_job_by_id('100-job') is None
