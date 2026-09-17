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
from vast_inception import required_markets, fetch_inception, bitget_first_candles, CACHE_FILES
from vast_market_cache import fetch_markets, stage_public_markets
from vast_provider import VastError
from setup.vast_gpu_benchmark.prepare import select_shards
from setup.vast_gpu_benchmark.inception_cache import install
from setup.vast_gpu_benchmark.cloud_worker import safe_path


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
    config = {'backtest': {'exchanges': [exchange]}, 'live': {'approved_coins': {'long': ['BTC'], 'short': []}}}
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
def test_public_metadata_client_and_staging(tmp_path, monkeypatch, exchange):
    """Use the correct futures client and execute real cache staging in a temp folder."""
    import ccxt.async_support as ccxt
    closed = []

    class Client:
        """Offline market client used by the production fetcher."""
        def __init__(self, options):
            """Accept a bounded metadata request configuration."""
            assert options['timeout'] == 30000

        async def load_markets(self, reload):
            """Return synthetic public linear-perpetual metadata."""
            return {'BTC': {'swap': True}}

        async def close(self):
            """Track deterministic client cleanup."""
            closed.append(True)

    monkeypatch.setattr(ccxt, CCXT_EXCHANGES[exchange], Client)
    assert set(asyncio.run(fetch_markets([exchange]))) == {exchange}

    def command(value, stdin, **kwargs):
        """Run only the generated staging program against pytest's temporary root."""
        subprocess.run([sys.executable, '-c', shlex.split(value)[2]], stdin=stdin, check=True)

    connection = SimpleNamespace(identifier='test', remote_root=str(tmp_path), command=command,
        store=SimpleNamespace(read=lambda _: {'exchanges': [exchange]}))
    stage_public_markets(connection)
    assert json.loads((tmp_path / 'output/caches' / exchange / 'markets.json').read_text())['BTC']['swap']
    assert closed == [True, True]


@pytest.mark.parametrize('exchange,timeframe,since', [
    ('okx', '1M', 1514764800000), ('hyperliquid', '1w', 1609459200000),
    ('kucoin', '1d', 1514764800000)])
def test_inception_request_matches_pinned_worker(monkeypatch, exchange, timeframe, since):
    """New venues keep PB8's listing-history horizons and KuCoin end bound."""
    import ccxt.async_support as ccxt
    calls, closed = [], []

    class Client:
        """Offline client recording the exact request contract."""
        def __init__(self, options):
            """Accept production constructor settings."""

        def milliseconds(self):
            """Return a stable test clock."""
            return 1700000000000

        async def fetch_ohlcv(self, symbol, **kwargs):
            """Return an authoritative synthetic listing candle."""
            calls.append(kwargs)
            return [[1600041600000, 1, 1, 1, 1, 1]]

        async def close(self):
            """Record resource release."""
            closed.append(True)

    monkeypatch.setattr(ccxt, CCXT_EXCHANGES[exchange], Client)
    result = asyncio.run(fetch_inception({exchange: {'BTC': 'BTC/USDT:USDT'}}))
    expected = {'since': since, 'timeframe': timeframe}
    if exchange == 'kucoin':
        expected.update(limit=1, params={'to': 1700000000000})
    assert calls == [expected] and closed == [True]
    assert result['files'][CACHE_FILES[1]] == {'BTC': {exchange: 1600041600000}}


@pytest.mark.parametrize('stuck', [False, True])
def test_bitget_backward_history_and_daily_refinement(stuck):
    """Find older listing months; never accept a server ignoring backward pagination."""
    month = 30 * 86400000
    listing = 1600041600000

    class Client:
        """Monthly history server capped to one candle per page."""
        def milliseconds(self):
            """Use a fixed clock beyond the listing date."""
            return listing + 4 * month

        async def fetch_ohlcv(self, symbol, timeframe, params):
            """Model until semantics and a distinct exact daily listing time."""
            if timeframe == '1d':
                return [[listing + 86400000, 1, 1, 1, 1, 1]]
            until = params.get('until', listing + 2 * month)
            if stuck:
                return [[listing + 2 * month, 1, 1, 1, 1, 1]]
            available = [stamp for stamp in (listing, listing + month, listing + 2 * month) if stamp <= until]
            return [[max(available), 1, 1, 1, 1, 1]] if available else []

    if stuck:
        with pytest.raises(VastError, match='did not advance'):
            asyncio.run(bitget_first_candles(Client(), 'BTC/USDT:USDT'))
    else:
        assert asyncio.run(bitget_first_candles(Client(), 'BTC/USDT:USDT'))[0][0] == listing + 86400000


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
