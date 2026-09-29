"""Managed Run/Backtest reads expose trading parameters but never credential stores."""
import asyncio
import copy
from pathlib import Path
from unittest.mock import Mock

import pytest

from ai_capabilities import AICapabilityError, AICapabilityService

OWNER = 'a' * 32


def config_payload():
    """Build representative public parameters interleaved with synthetic secrets."""
    return {'config': {
        'live': {'approved_coins': {'long': ['BTC', 'ETH', 'HYPE'], 'short': ['SOL']},
                 'ignored_coins': {'long': ['DOGE'], 'short': []}, 'api_key': 'SECRET_A'},
        'bot': {'long': {'n_positions': 3, 'total_wallet_exposure_limit': 7.13}},
        'backtest': {'start_date': '2025-01-01', 'starting_balance': 1000},
        'pbgui': {'enabled_on': 'test-host', 'note': 'Public strategy note', 'password': 'SECRET_B'},
        'nested': [{'privateKey': 'SECRET_C', 'api-key': 'SECRET_D', 'value': 12}],
        'credentials': {'key': 'SECRET_E'}, 'sessionToken': 'SECRET_F', 'source_path': '/private/path',
    }, 'override_configs': {'BTC': {'bot': {'long': {'n_positions': 1}}, 'password': 'SECRET_G'}}}


@pytest.mark.parametrize('version', ['v7', 'v8'])
@pytest.mark.parametrize('kind', ['run', 'backtest'])
def test_config_read_uses_exact_managed_source_and_redacts(tmp_path, monkeypatch, version, kind):
    """Both generations and config kinds return actual approved coins through dispatch."""
    import ai_capabilities
    from api import v7_instances, v8_instances, backtest_v7, backtest_v8
    monkeypatch.setattr(ai_capabilities, 'PBGDIR', str(tmp_path))
    root = tmp_path / 'data' / f"{'run' if kind == 'run' else 'bt'}_{version}" / 'chosen'
    root.mkdir(parents=True)
    (root / ('config.json' if kind == 'run' else 'backtest.json')).write_text('{}')
    payload = config_payload()
    original = copy.deepcopy(payload)
    getter = Mock(return_value=payload)
    module, name = ({('run', 'v7'): (v7_instances, 'get_instance_config'),
                     ('run', 'v8'): (v8_instances, 'get_v8_instance_config'),
                     ('backtest', 'v7'): (backtest_v7, 'get_config'),
                     ('backtest', 'v8'): (backtest_v8, 'get_config')})[(kind, version)]
    monkeypatch.setattr(module, name, getter)
    service = AICapabilityService(tmp_path / 'capabilities')
    result = asyncio.run(service.dispatch(OWNER, 'c' * 32, f'get_{kind}_config', {'version': version, 'name': 'chosen'}))
    assert getter.call_args.args == ('chosen',)
    assert result['config']['live']['approved_coins']['long'] == ['BTC', 'ETH', 'HYPE']
    assert result['config']['live']['approved_coins']['short'] == ['SOL']
    assert result['config']['bot']['long']['total_wallet_exposure_limit'] == 7.13
    assert result['config']['backtest']['starting_balance'] == 1000
    assert result['config']['pbgui']['enabled_on'] == 'test-host'
    assert result['override_configs']['BTC']['bot']['long']['n_positions'] == 1
    assert result['config']['nested'][0] == {'value': 12}
    assert 'SECRET_' not in str(result) and '/private/path' not in str(result)
    assert payload == original


def test_config_discovery_pages_without_opening_secret_files(tmp_path, monkeypatch):
    """Lists enumerate only managed config bundles and support retrieving later names."""
    import ai_capabilities
    monkeypatch.setattr(ai_capabilities, 'PBGDIR', str(tmp_path))
    root = tmp_path / 'data/run_v8'
    for name in ('Alpha', 'Beta', 'Gamma'):
        folder = root / name
        folder.mkdir(parents=True)
        (folder / 'config.json').write_text('{}')
    (root / 'api_keys.json').write_text('{"password":"MUST_NOT_READ"}')
    service = AICapabilityService(tmp_path / 'capabilities')
    first = service._list_managed_configs('run', {'version': 'v8', 'limit': 2})
    assert [item['name'] for item in first['configs']] == ['Alpha', 'Beta']
    assert first['next_offset'] == 2 and first['total'] == 3
    second = service._list_managed_configs('run', {'version': 'v8', 'limit': 2, 'offset': 2})
    assert second['configs'][0]['name'] == 'Gamma' and second['next_offset'] is None
    matched = service._list_managed_configs('run', {'version': 'v8', 'query': 'BETA'})
    assert [item['name'] for item in matched['configs']] == ['Beta']


@pytest.mark.parametrize('name', ['../outside', '/tmp/outside', 'bad/name', 'bad\\name', '..', 'bad\x00name'])
def test_config_read_rejects_paths(tmp_path, monkeypatch, name):
    """Untrusted page/entity names cannot become filesystem paths."""
    import ai_capabilities
    monkeypatch.setattr(ai_capabilities, 'PBGDIR', str(tmp_path))
    service = AICapabilityService(tmp_path / 'capabilities')
    with pytest.raises(AICapabilityError):
        service._get_managed_config('run', {'version': 'v8', 'name': name})


@pytest.mark.parametrize('link_kind', ['root', 'directory', 'file'])
def test_config_read_rejects_symlinks_before_loading(tmp_path, monkeypatch, link_kind):
    """No canonical config loader is invoked through a symlink to other data."""
    import ai_capabilities
    from api import v7_instances
    monkeypatch.setattr(ai_capabilities, 'PBGDIR', str(tmp_path))
    getter = Mock(side_effect=AssertionError('Must not read'))
    monkeypatch.setattr(v7_instances, 'get_instance_config', getter)
    outside = tmp_path / 'outside'
    outside.mkdir()
    (outside / 'config.json').write_text('{"password":"SECRET"}')
    data = tmp_path / 'data'
    data.mkdir()
    root = data / 'run_v7'
    if link_kind == 'root':
        root.symlink_to(outside, target_is_directory=True)
    else:
        root.mkdir()
        if link_kind == 'directory':
            (root / 'chosen').symlink_to(outside, target_is_directory=True)
        else:
            (root / 'chosen').mkdir()
            (root / 'chosen/config.json').symlink_to(outside / 'config.json')
    service = AICapabilityService(tmp_path / 'capabilities')
    with pytest.raises(AICapabilityError, match='symlink'):
        service._get_managed_config('run', {'version': 'v7', 'name': 'chosen'})
    getter.assert_not_called()


@pytest.mark.parametrize('version', ['v7', 'v8'])
def test_backtest_result_config_uses_listed_resource(tmp_path, monkeypatch, version):
    """Executed configs are resolved by existing opaque resource checks, not caller paths."""
    from api import backtest_v7, backtest_v8
    service = AICapabilityService(tmp_path / 'capabilities')
    resource = service._virtual_uri('backtest', version, 'managed-result')
    resolve = Mock(return_value={'path': 'managed-result'})
    monkeypatch.setattr(service, '_resolve_listed_resource', resolve)
    module = backtest_v8 if version == 'v8' else backtest_v7
    getter = Mock(return_value=config_payload()['config'])
    monkeypatch.setattr(module, 'get_result_config', getter)
    result = service._get_backtest_result_config({'version': version, 'resource': resource})
    resolve.assert_called_once_with('backtest', version, resource)
    assert getter.call_args.args == ('managed-result',)
    assert result['config']['live']['approved_coins']['long'] == ['BTC', 'ETH', 'HYPE']
    assert 'SECRET_' not in str(result)
