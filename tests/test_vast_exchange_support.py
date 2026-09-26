"""Offline end-to-end export and metadata coverage for all supported cloud venues."""
import asyncio
import hashlib
import json
import shlex
import subprocess
import sys
from types import SimpleNamespace

import pytest

from vast_exchanges import CCXT_EXCHANGES, SUPPORTED_EXCHANGES, quote_currency
from vast_config_validation import validate_cloud_config
from vast_inception import required_markets, CACHE_FILES
from vast_market_cache import fetch_markets, stage_public_markets
from vast_provider import VastError
from setup.vast_gpu_benchmark.prepare import select_shards
from setup.vast_gpu_benchmark.inception_cache import install
from setup.vast_gpu_benchmark.cloud_worker import safe_path


@pytest.fixture(autouse=True)
def native_warmup(monkeypatch):
    """Provide native warmup results without reading local runtime configuration."""
    monkeypatch.setattr('pb8_config._call_helper', lambda operation, **payload:
                        {'minutes': [0] * len(payload['configs'])})


@pytest.mark.parametrize('exchange', SUPPORTED_EXCHANGES)
def test_export_mapping_and_receiver_agree(tmp_path, exchange):
    """Export and both cache schemas preserve standard names and the correct quote."""
    quote = quote_currency(exchange)
    symbol = f'BTC/{quote}:{quote}'
    mapping = tmp_path / 'mappings' / exchange
    mapping.mkdir(parents=True)
    (mapping / 'mapping.json').write_text(json.dumps([
        {'coin': 'BTC', 'quote': quote, 'swap': True, 'linear': True, 'ccxt_symbol': symbol},
        {'coin': 'BTC', 'quote': quote, 'swap': False, 'linear': False, 'ccxt_symbol': f'BTC/{quote}'}]))
    source = tmp_path / 'raw' / CCXT_EXCHANGES[exchange] / '1m' / symbol.replace('/', '_') / '2024-01-01.npy'
    source.parent.mkdir(parents=True)
    source.write_bytes(b'fixture')
    config = {'backtest': {'exchanges': [exchange], 'start_date': '2024-01-01', 'end_date': '2024-01-01'}, 'live': {'approved_coins': {'long': ['BTC'], 'short': []}}}
    shards = select_shards(config, tmp_path / 'raw', tmp_path / 'mappings')
    assert len(shards) == 1 and shards[0][0] == source
    assert shards[0][1].parts[0] == exchange
    manifest = {'files': [{'path': 'ohlcv/' + str(shards[0][1])}]}
    assert required_markets(manifest, tmp_path / 'mappings') == {exchange: {'BTC': symbol}}
    identifier = 'binanceusdm' if exchange == 'binance' else exchange
    data = {'version': 2, 'files': dict(zip(CACHE_FILES, (
        {'BTC': 1600041600000}, {'BTC': {identifier: 1600041600000}}, {'BTC': {identifier: symbol}})))}
    raw = json.dumps(data).encode()
    version = tmp_path / 'utils.py'
    version.write_text('FIRST_OHLCV_TIMESTAMPS_CACHE_VERSION = 2\n')
    install(tmp_path, raw, hashlib.sha256(raw).hexdigest(), safe_path, version)
    assert json.loads((tmp_path / 'output/caches' / CACHE_FILES[2]).read_text()) == {'BTC': {identifier: symbol}}


@pytest.mark.parametrize('exchange', SUPPORTED_EXCHANGES)
def test_local_public_metadata_staging(tmp_path, monkeypatch, exchange):
    """Each venue stages local linear perpetual metadata without a CCXT client."""
    root = tmp_path / 'coindata'
    source = root / exchange / 'ccxt_markets.json'
    source.parent.mkdir(parents=True)
    quote = quote_currency(exchange)
    symbol = f'BTC/{quote}:{quote}'
    source.write_text(json.dumps({
        symbol: {'symbol': symbol, 'quote': quote, 'swap': True, 'linear': True},
        'BTC/USD': {'symbol': 'BTC/USD', 'quote': 'USD', 'swap': False, 'linear': False},
    }))
    monkeypatch.setattr('vast_market_cache.MARKET_ROOT', root)
    snapshots = asyncio.run(fetch_markets([exchange]))
    assert list(snapshots[exchange]['markets']) == [symbol]

    def command(value, stdin, **kwargs):
        """Run only the generated staging program against pytest's temporary root."""
        subprocess.run([sys.executable, '-c', shlex.split(value)[2]], stdin=stdin, check=True)

    connection = SimpleNamespace(identifier='test', remote_root=str(tmp_path), command=command,
        store=SimpleNamespace(read=lambda _: {'exchanges': [exchange]}))
    stage_public_markets(connection)
    assert json.loads((tmp_path / 'output/caches' / exchange / 'markets.json').read_text()) == snapshots[exchange]['markets']


def test_validation_identifies_exchange_and_each_scenario():
    """Errors identify exact exchange values and genuinely unknown scenario fields."""
    config = {'live': {'strategy_kind': 'ema_anchor', 'approved_coins': {'long': ['BTC'], 'short': []}},
        'bot': {'long': {}, 'short': {}},
        'backtest': {'exchanges': list(SUPPORTED_EXCHANGES)},
        'optimize': {'iters': 512, 'n_cpus': 4, 'scoring': [{'metric': 'adg_strategy_eq', 'goal': 'max'}]}}
    assert validate_cloud_config(config) == []
    config['backtest'].update(exchanges=['bybit', '../bad'], suite_enabled=True, scenarios=[
        {'label': 'Bull', 'start_date': '2024-01-01', 'exchanges': ['bybit'], 'unexpected': {}},
        {'label': '<img src=x onerror=alert(1)>', 'end_date': '2025-01-01', 'exchanges': ['bybit'], 'coin_overrides': {}}])
    errors = validate_cloud_config(config)
    assert len(errors) == 3
    assert errors[0]['path'] == 'backtest.exchanges.1'
    assert '../bad' in errors[0]['message'] and 'hyperliquid' in errors[0]['message']
    assert "Scenario 1 'Bull': unknown fields: unexpected" in errors[1]['message']
    assert 'Scenario 2' in errors[2]['message'] and 'coin_overrides' in errors[2]['message']
    assert 'Allowed fields:' in errors[2]['message']


@pytest.mark.parametrize('exchanges', [[], None, 'binance', ['../escape'], [{}]])
def test_missing_or_malformed_exchanges_are_actionable(exchanges):
    """Invalid exchange selection is caught before export or paid rental startup."""
    errors = validate_cloud_config({'backtest': {'exchanges': exchanges}})
    exchange_errors = [error for error in errors if error['path'].startswith('backtest.exchanges')]
    assert exchange_errors and all('binance' in error['message'] for error in exchange_errors)
    assert all(error['suggestions'] for error in exchange_errors)
