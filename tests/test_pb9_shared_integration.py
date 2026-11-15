"""Offline coverage for PB9 integration with shared mapping and credential paths."""
import configparser
import json
from pathlib import Path

import pytest
from fastapi import HTTPException

from api.editor_market_data import exact_market_catalog


def _mapping(tmp_path, rows):
    """Publish an isolated public mapping fixture."""
    directory = tmp_path / 'data/coindata/bybit'
    directory.mkdir(parents=True)
    (directory / 'mapping.json').write_text(json.dumps(rows))


def _row(symbol, coin='PEPE', **fields):
    """Return an eligible exact mapping row."""
    return dict(symbol=symbol, ccxt_symbol=symbol + '/USDT:USDT', coin=coin,
                quote='USDT', swap=True, linear=True, active=True, **fields)


def test_exact_catalog_retains_scaled_market_collisions(tmp_path):
    """Coin aliases cannot silently pick one of two independently traded markets."""
    _mapping(tmp_path, [_row('PEPEUSDT'), _row('1000PEPEUSDT')])
    result = exact_market_catalog(tmp_path, ['bybit'], ['PEPE', 'bybit::1000PEPEUSDT'])
    assert result['symbols'] == ['bybit::1000PEPEUSDT', 'bybit::PEPEUSDT']
    assert result['statuses']['PEPE']['reason'] == 'ambiguous'
    assert result['statuses']['bybit::1000PEPEUSDT']['status'] == 'valid'


def test_catalog_filters_and_refuses_symlink(tmp_path):
    """Non-USDT and non-swap rows never enter the Run selection."""
    rows = [_row('BTCUSDT', 'BTC'), dict(_row('ETHUSDT', 'ETH'), swap=False),
            dict(_row('SOLUSDC', 'SOL'), quote='USDC')]
    _mapping(tmp_path, rows)
    assert exact_market_catalog(tmp_path, ['bybit'])['symbols'] == ['bybit::BTCUSDT']
    path = tmp_path / 'data/coindata/bybit/mapping.json'
    target = tmp_path / 'outside.json'
    path.rename(target)
    path.symlink_to(target)
    with pytest.raises(HTTPException) as error:
        exact_market_catalog(tmp_path, ['bybit'])
    assert error.value.status_code == 503


@pytest.mark.parametrize('exchanges', [['../x'], ['bybit/../x'], [], ['BYBIT']])
def test_catalog_rejects_untrusted_exchange_paths(tmp_path, exchanges):
    """A caller cannot choose a filesystem path as an exchange."""
    with pytest.raises(HTTPException) as error:
        exact_market_catalog(tmp_path, exchanges)
    assert error.value.status_code == 422


def test_pb9_credential_projection_uses_separate_verified_slot(tmp_path, monkeypatch):
    """PB9 receives its own projection only after static source verification."""
    import cluster_sync_command as sync
    import pbgui_purefunc
    import passivbot_runtime
    root = tmp_path / 'pbgui'
    source = root / 'data/passivbot_v9'
    source.mkdir(parents=True)
    snapshot = configparser.ConfigParser()
    snapshot['main'] = {'pb9dir': str(source), 'pb9venv': str(root / 'data/venv_pb9/bin/python')}
    monkeypatch.setattr(sync, 'PBGDIR', root)
    monkeypatch.setattr(sync, 'pb7dir', lambda: str(tmp_path / 'pb7'))
    monkeypatch.setattr(sync, 'pb8dir', lambda: '')
    monkeypatch.setattr(pbgui_purefunc, 'load_ini_snapshot', lambda: snapshot)
    monkeypatch.setattr(passivbot_runtime, 'classify_passivbot_source',
                        lambda path: {'generation': 'pb9', 'errors': []})
    targets = sync._api_keys_projection_targets(root / 'data/cluster')
    assert [row[0] for row in targets] == ['pb7', 'pb9']
    assert targets[1][1] == source / 'api-keys.json'
    assert targets[1][2].name == 'pb9_projection.json'
    monkeypatch.setattr(passivbot_runtime, 'classify_passivbot_source',
                        lambda path: {'generation': 'pb8', 'errors': []})
    assert len(sync._api_keys_projection_targets(root / 'data/cluster')) == 1


def test_shared_log_panel_keeps_pb9_paths_separate(tmp_path, monkeypatch):
    """Logical PB9 log aliases resolve only into PB9 sources and live bundles."""
    from master import async_logs
    import pbgui_purefunc
    snapshot = configparser.ConfigParser()
    snapshot['main'] = {'pb9dir': str(tmp_path / 'native9')}
    monkeypatch.setattr(pbgui_purefunc, 'load_ini_snapshot', lambda: snapshot)
    monkeypatch.setattr(async_logs, '_project_root', lambda: tmp_path)
    assert async_logs.resolve_local_log_path('Bot:fixture:9') == tmp_path / 'native9/logs/fixture.log'
    assert async_logs.resolve_local_log_path('BotErr:fixture:9') == tmp_path / 'data/run_v9/fixture/passivbot_err.log'
    assert async_logs.resolve_local_log_path('BotBackup:fixture:9:1:passivbot.log') == tmp_path / 'data/backup/v9/fixture/1/passivbot.log'
    assert async_logs.resolve_local_log_path('BotBackup:fixture:9:../2:passivbot.log') is None
    with pytest.raises(ValueError):
        async_logs.resolve_local_log_path('Bot:../fixture:9')


def test_hyperliquid_hip3_catalog_preserves_numeric_native_ids(tmp_path):
    """Native USDC venues retain namespace labels and exact numeric market IDs."""
    directory = tmp_path / 'data/coindata/hyperliquid'
    directory.mkdir(parents=True)
    (directory / 'mapping.json').write_text(json.dumps([
        {'symbol': '1234', 'ccxt_symbol': 'XYZ-TSLA/USDC:USDC', 'coin': 'XYZ-TSLA',
         'quote': 'USDC', 'swap': True, 'linear': True, 'active': True, 'is_hip3': True},
        {'symbol': '5678', 'ccxt_symbol': 'PARA-TSLA/USDC:USDC', 'coin': 'PARA-TSLA',
         'quote': 'USDC', 'swap': True, 'linear': True, 'active': True, 'is_hip3': True}]))
    result = exact_market_catalog(tmp_path, ['hyperliquid'], ['hyperliquid::1234'])
    assert result['symbols'] == ['hyperliquid::1234', 'hyperliquid::5678']
    assert result['statuses']['hyperliquid::1234']['resolutions'] == [
        {'exchange': 'hyperliquid', 'symbol': 'XYZ-TSLA/USDC:USDC'}]
