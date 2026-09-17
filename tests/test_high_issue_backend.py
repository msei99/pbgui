"""Isolated backend regressions for High issues #341 and #359."""

import json
import sys
import threading
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import pb8_config
from api import backtest_v8, optimize_v8
from api.auth import require_auth


@pytest.mark.parametrize('path,params,target,result', [
    ('/scenario-templates', {}, 'get_scenario_templates', {'templates': []}),
    ('/scenario-templates/sources', {'exchange': 'binance'}, 'scenario_chart_sources', {'sources': []}),
    ('/scenario-templates/chart', {'exchange': 'binance', 'dataset': 'x', 'coin': 'BTC', 'start': '2024-01-01', 'end': '2024-02-01'}, 'scenario_chart_data', {'candles': []}),
])
def test_backtest_scenario_routes_use_shared_services(monkeypatch, path, params, target, result):
    """Backtest URLs resolve to the authenticated common services, not 404 (#341)."""
    monkeypatch.setattr(optimize_v8, target, lambda *args, **kwargs: result)
    app = FastAPI()
    app.include_router(backtest_v8.router, prefix='/api/backtest-v8')
    app.dependency_overrides[require_auth] = lambda: None
    with TestClient(app) as client:
        response = client.get('/api/backtest-v8' + path, params=params)
    assert response.status_code == 200
    assert response.json() == result


def test_backtest_scenario_preview_preserves_validation_errors(monkeypatch):
    """Both valid and invalid window previews retain shared 422 semantics."""
    from fastapi import HTTPException

    def preview(body, session=None):
        """Model shared validation without runtime data."""
        if not body.get('windows'):
            raise HTTPException(422, 'Invalid windows')
        return {'training_scenarios': body['windows']}

    monkeypatch.setattr(optimize_v8, 'preview_scenario_template', preview)
    app = FastAPI()
    app.include_router(backtest_v8.router)
    app.dependency_overrides[require_auth] = lambda: None
    with TestClient(app) as client:
        assert client.post('/scenario-templates/preview', json={}).status_code == 422
        assert client.post('/scenario-templates/preview', json={'windows': [{'label': 'test'}]}).json() == {'training_scenarios': [{'label': 'test'}]}


def test_local_config_calls_reuse_one_owned_process_and_propagate_errors(tmp_path, monkeypatch):
    """Repeated configs reuse a process; invalid input never triggers a cold retry (#359)."""
    helper = tmp_path / 'pb8_config_helper.py'
    helper.write_text('''import json, os, sys
for line in sys.stdin:
    request = json.loads(line)
    result = {"config": {"pid": os.getpid(), "operation": request["operation"]}}
    response = {"ok": True, "result": result}
    if request.get("config", {}).get("invalid"):
        response = {"ok": False, "detail": "Invalid test config"}
    print(json.dumps(response), flush=True)
''')
    monkeypatch.setattr(pb8_config, '__file__', str(tmp_path / 'pb8_config.py'))
    monkeypatch.setattr(pb8_config, '_runtime', lambda: {'pb8dir': str(tmp_path), 'pb8venv': sys.executable})
    monkeypatch.setattr(pb8_config, '_runtime_fingerprint', lambda *_args: ('test',))
    monkeypatch.setattr(pb8_config, 'acquire_master_runtime_lock', lambda _: SimpleNamespace(release=lambda: None))
    for attr in ('_migration_helper_process', '_migration_helper_reader_thread', '_migration_helper_fingerprint', '_migration_helper_responses'):
        monkeypatch.setattr(pb8_config, attr, None)
    monkeypatch.setattr(pb8_config, '_migration_helper_shutdown', threading.Event())
    try:
        first = pb8_config.prepare_pb8_config({})
        proc = pb8_config._migration_helper_process
        second = pb8_config.prepare_pb8_config({})
        assert first['pid'] == second['pid'] == proc.pid
        with pytest.raises(pb8_config.PB8ConfigurationError, match='Invalid test config'):
            pb8_config.prepare_pb8_config({'invalid': True})
        assert pb8_config.prepare_pb8_config({})['pid'] == proc.pid
        pb8_config.interrupt_pb8_migration_helper()
        pb8_config.shutdown_pb8_migration_helper()
        assert proc.poll() is not None
        assert pb8_config._migration_helper_reader_thread is None
        with pytest.raises(pb8_config.PB8ConfigurationError, match='shutting down'):
            pb8_config.prepare_pb8_config({})
    finally:
        pb8_config.shutdown_pb8_migration_helper()


@pytest.mark.parametrize('operation', ['prepare', 'load', 'validate_overrides', 'validate_optimizer_overrides'])
def test_only_local_config_operations_use_warm_helper(monkeypatch, operation):
    """All save-related checks use the warm serialized helper."""
    calls = []
    monkeypatch.setattr(pb8_config, '_call_migration_helper', lambda op, **kw: calls.append((op, kw)) or {'valid': True})
    assert pb8_config._call_helper(operation, config={}) == {'valid': True}
    assert calls == [(operation, {'config': {}})]
