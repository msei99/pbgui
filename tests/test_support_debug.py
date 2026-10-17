"""Universal support export is offline, owner-bound, bounded and credential-free."""
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import support_debug as debug
from api import support_debug as api
from api.auth import require_auth

OWNER = 'a' * 32
OTHER = 'b' * 32
LOOP = 'c' * 32


def put(root, name, value):
    """Create an isolated operational fixture, never a real user file."""
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))
    return path


@pytest.fixture
def fixture_root(tmp_path, monkeypatch):
    """Provide data from all families and force every optional hardware probe offline."""
    monkeypatch.setattr(debug.subprocess, 'run', lambda *a, **k: SimpleNamespace(returncode=0, stdout='GPU fixture, 123, 8192, 12'))
    put(tmp_path, f'data/loop_optimizer/{OWNER}/{LOOP}.json', {
        'owner': OWNER, 'status': 'failed', 'phase': 'evaluate', 'round': 0,
        'reason': 'No comparable validation', 'settings': {'execution': 'gpu', 'run_proxy': 100000},
        'initial_config': {'api_key': 'CONFIG_SECRET'},
        'jobs': [{'status': 'failed', 'error': 'password=HIDDEN native failed', 'runner': {'returncode': 1}}],
        'rejected_observations': [{'exact': True, 'assessment': {'simulation_complete': False,
            'comparable': False, 'goals': [{'metric': 'gain_strategy_eq', 'value': None}]},
            'per_report': [{'backtest_completion_ratio': .1, 'gain_strategy_eq': 2}]}]})
    put(tmp_path, f'data/loop_optimizer/{OTHER}/{LOOP}.json', {'owner': OTHER, 'reason': 'OTHER_PRIVATE'})
    put(tmp_path, f'data/ai/conversations/{OWNER}/{LOOP}.json', {'status': 'failed', 'messages': [{'content': 'CHAT_SECRET'}]})
    for name in ['bt_v7_queue', 'bt_v8_queue', 'opt_v7_queue', 'opt_v8_queue', 'tasks', 'cluster', 'monitor']:
        put(tmp_path, f'data/{name}/job.json', {'status': 'failed', 'error': 'sample error', 'config': {'secret': 'CONFIG_SECRET'}})
    for name in ['vps', 'dashboards', 'transfers', 'profit_sweep', 'custom_new_feature']:
        put(tmp_path, f'data/{name}/status.json', {'status': 'error', 'counter': 7, 'diagnostics': {'error': 'source failure'}})
    put(tmp_path, 'data/monitor_agent/host_meta.json', {'cpu_percent': 60, 'health': {'status': 'ok'}})
    put(tmp_path, 'data/ohlcv/_tasks/failed/job.json', {'status': 'failed', 'error': 'preload error'})
    put(tmp_path, 'data/vast/jobs/abc/job.json', {'status': 'failed', 'owner': OWNER, 'token': 'VAST_SECRET'})
    put(tmp_path, 'data/auth/state.json', {'status': 'AUTH_SECRET'})
    (tmp_path / 'pbgui.ini').write_text('[main]\npassword=INI_SECRET\n')
    logs = tmp_path / 'data/logs'; logs.mkdir(exist_ok=True)
    (logs / 'PBGui.log').write_text('native failure password=LOG_SECRET\nhttps://host/?api_key=QUERY_SECRET\n')
    (logs / 'Auth.log').write_text('AUTH_LOG_SECRET')
    (logs / 'PBGui.log.1').write_text('Previous diagnostic failure')
    (logs / 'future-service.log').write_text('Future service failure\n{"messages":[{"content":"PRIVATE_CHAT"}]}\n')
    dbfile = tmp_path / 'data/history.sqlite'
    with sqlite3.connect(dbfile) as db:
        db.execute('CREATE TABLE fills(value TEXT)'); db.execute("INSERT INTO fills VALUES('ROW_SECRET')")
    return tmp_path


def test_universal_collection_and_loop_validation(fixture_root):
    """All families and future status/log namespaces participate without page-specific adapters."""
    result = debug.collect(fixture_root, OWNER, ['/api/custom-new-feature', '/api/debug'], loop_id=LOOP)
    encoded = json.dumps(result)
    for secret in ['CONFIG_SECRET', 'CHAT_SECRET', 'INI_SECRET', 'AUTH_SECRET', 'LOG_SECRET',
                   'QUERY_SECRET', 'AUTH_LOG_SECRET', 'VAST_SECRET', 'ROW_SECRET', 'PRIVATE_CHAT', 'OTHER_PRIVATE']:
        assert secret not in encoded
    for family in ['bt_v7_queue', 'bt_v8_queue', 'opt_v7_queue', 'opt_v8_queue', 'market_tasks',
                   'custom_new_feature', 'monitor_agent', 'vast', 'cluster']:
        assert any(family in key for key in result['records'])
    selected = result['records']['selected_loop']
    assert selected['settings']['execution'] == 'gpu'
    assert selected['rejected_observations'][0]['assessment']['simulation_complete'] is False
    assert selected['rejected_observations'][0]['assessment']['goals'][0]['value'] is None
    assert selected['rejected_observations'][0]['per_report'][0]['backtest_completion_ratio'] == .1
    assert result['databases'][0]['quick_check'] == ['ok']
    assert 'fills' in result['databases'][0]['tables']
    assert result['system']['gpu']['status'] == 'available'
    assert '/api/custom-new-feature' in result['coverage']['registered_routes']
    assert 'PBGui.log.1' in result['logs']


def test_owner_context_and_symlinks_are_rejected(fixture_root, tmp_path):
    """An authenticated user cannot select another owner's Loop or follow file/directory links."""
    with pytest.raises(FileNotFoundError):
        debug.collect(fixture_root, OTHER, [], loop_id='d' * 32)
    outside = tmp_path / 'outside.log'; outside.write_text('OUTSIDE_SECRET')
    (fixture_root / 'data/logs/escape.log').symlink_to(outside)
    (fixture_root / 'data/logs/escape_dir').symlink_to(tmp_path, target_is_directory=True)
    assert 'OUTSIDE_SECRET' not in json.dumps(debug.collect(fixture_root, OWNER, []))
    with pytest.raises(ValueError):
        debug.collect(fixture_root, '../escape', [])


def test_limits_invalid_json_native_reports_and_no_mutation(fixture_root, monkeypatch):
    """Bounds and corrupt sources are explicit; collection leaves every original file untouched."""
    put(fixture_root, 'data/bt_v8_queue/state/bad.json', {}) .write_text('{')
    bot = fixture_root / 'pb8'
    put(bot, 'backtests/pbgui/candidate/run/analysis.json', {'gain_strategy_eq': 2, 'backtest_completion_ratio': 1, 'config': {'api_key': 'BAD'}})
    before = {str(p): (p.stat().st_mtime_ns, p.read_bytes()) for p in fixture_root.rglob('*') if p.is_file()}
    result = debug.collect(fixture_root, OWNER, [], runtime_roots={'pb8': bot})
    assert any('analysis.json' in key for key in result['records'])
    assert any(item['reason'] == 'invalid JSON' for item in result['coverage']['omitted'])
    after = {str(p): (p.stat().st_mtime_ns, p.read_bytes()) for p in fixture_root.rglob('*') if p.is_file()}
    assert before == after
    monkeypatch.setattr(debug, 'MAX_FILES', 1)
    result = debug.collect(fixture_root, OWNER, [])
    assert len(result['coverage']['sources']) <= 1
    assert any('budget' in item['reason'] or item['reason'] == 'collection limit' for item in result['coverage']['omitted'])
    assert debug.scrub({'jobs': [{'status': 'old'}] * 150})['jobs'][0]['debug_truncated_items'] == 50


@pytest.fixture
def client(fixture_root, monkeypatch):
    """Mount only the new API with synthetic authentication and isolated roots."""
    monkeypatch.setattr(api, 'PBGDIR', fixture_root)
    monkeypatch.setattr(api, 'pb7dir', lambda: None)
    monkeypatch.setattr(api, 'pb8dir', lambda: None)
    monkeypatch.setattr(api, 'owner_key', lambda value: OWNER)
    app = FastAPI(); app.include_router(api.router, prefix='/api/debug')
    app.dependency_overrides[require_auth] = lambda: SimpleNamespace(user_id='fixture')
    return TestClient(app)


def test_api_owner_validation_and_mount_rendering(client):
    """Downloads require auth, IDs never become paths, and page URLs preserve ASGI mounts."""
    response = client.post('/api/debug/collect', json={'loop_id': LOOP})
    assert response.status_code == 200 and response.headers['cache-control'] == 'no-store'
    assert client.post('/api/debug/collect', json={'loop_id': '../x'}).status_code == 422
    assert client.post('/api/debug/collect', json={'root': '/etc'}).status_code == 422
    assert client.post('/api/debug/collect', json={'loop_id': 'd' * 32}).status_code == 404
    bare = FastAPI(); bare.include_router(api.router, prefix='/api/debug')
    assert TestClient(bare).post('/api/debug/collect', json={}).status_code == 401
    from starlette.requests import Request
    page = api.main_page(Request({'type':'http','root_path':'/nested','path':'/api/debug/main_page','headers':[]}), object()).body.decode()
    assert '"/nested/api/debug"' in page and 'src="/nested/app/js/support_debug.js?v=5"' in page
    from pbgui_purefunc import PBGUI_VERSION, PBGUI_SERIAL
    assert 'window.PBGUI_VERSION = ' + json.dumps(PBGUI_VERSION) in page
    assert 'window.PBGUI_SERIAL = ' + json.dumps(PBGUI_SERIAL) in page
    assert '%%' not in page


def test_invalid_unicode_in_existing_diagnostics_remains_downloadable(fixture_root):
    """Malformed old JSON strings and filesystem names cannot break UTF-8 JSON responses."""
    from fastapi.responses import JSONResponse
    put(fixture_root, 'data/bt_v8_queue/unicode.json', {'error': 'invalid unicode \ud800'})
    (fixture_root / 'data/logs' / 'bad\udcff.log').write_bytes(b'old log')
    result = debug.collect(fixture_root, OWNER, [])
    rendered = JSONResponse(result).body
    assert b'invalid unicode' in rendered
    assert json.loads(rendered)['format'] == 'pbgui-support-debug-v1'


def test_noisy_area_cannot_displace_other_areas_or_logs(fixture_root, monkeypatch):
    """Fair per-area byte/file limits keep universal diagnostics useful under load."""
    monkeypatch.setattr(debug, 'MAX_TOTAL', 80000)
    for index in range(60):
        put(fixture_root, f'data/bt_v8_queue/noisy-{index:02}.json', {'status':'failed','error':'x' * 7000})
    result = debug.collect(fixture_root, OWNER, [])
    assert 'PBGui.log' in result['logs']
    assert any('custom_new_feature' in key for key in result['records'])
    assert any('budget' in item['reason'] or 'limit' in item['reason'] for item in result['coverage']['omitted'])


def test_error_context_is_redacted_owner_bound_expiring_and_bounded(client, monkeypatch):
    """Error handoff stores no raw secrets and cannot cross users or survive its TTL."""
    monkeypatch.setattr(api, '_error_contexts', {})
    clock = [100.0]
    monkeypatch.setattr(api.time, 'monotonic', lambda: clock[0])
    result = client.post('/api/debug/error-context', json={'area':'v8_optimize', 'loop_id':LOOP,
        'message':'Failure password=TOP_SECRET', 'detail':'https://host/?token=URL_SECRET'} )
    assert result.status_code == 200 and result.headers['cache-control'] == 'no-store'
    identifier = result.json()['context_id']
    response = client.post('/api/debug/collect', json={'context_id':identifier})
    assert response.status_code == 200
    context = response.json()['error_context']
    assert context['loop_id'] == LOOP and context['area'] == 'v8_optimize'
    assert 'TOP_SECRET' not in json.dumps(context) and 'URL_SECRET' not in json.dumps(context)
    monkeypatch.setattr(api, 'owner_key', lambda value: OTHER)
    assert client.post('/api/debug/collect', json={'context_id':identifier}).status_code == 404
    monkeypatch.setattr(api, 'owner_key', lambda value: OWNER)
    clock[0] += api.CONTEXT_TTL
    assert client.post('/api/debug/collect', json={'context_id':identifier}).status_code == 404
    for _ in range(api.MAX_CONTEXTS + 2):
        assert client.post('/api/debug/error-context', json={'message':'fixture'}).status_code == 200
    assert len(api._error_contexts) == api.MAX_CONTEXTS
    bare = FastAPI(); bare.include_router(api.router, prefix='/api/debug')
    assert TestClient(bare).post('/api/debug/error-context', json={}).status_code == 401
