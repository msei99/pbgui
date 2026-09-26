"""Offline coverage for public metadata staging on existing queued jobs."""

import asyncio
import json
import os
import shlex
import time
from types import SimpleNamespace

import pytest

import vast_market_cache as cache
from vast_provider import VastError


def test_stage_fresh_metadata(tmp_path, monkeypatch):
    """Install verified snapshots separately from an old immutable input bundle."""
    stamp = time.time() - 1

    async def fetch(exchanges):
        """Return synthetic public metadata without network calls."""
        assert exchanges == ['binance', 'bybit']
        return {ex: {'markets': {'ETH/USDT:USDT': {'swap': True}}, 'fetched_at': stamp} for ex in exchanges}

    def command(value, stdin, **kwargs):
        """Run the staging script against an isolated directory."""
        import subprocess
        import sys
        subprocess.run([sys.executable, '-c', shlex.split(value)[2]], stdin=stdin, check=True)

    monkeypatch.setattr(cache, 'fetch_markets', fetch)
    connection = SimpleNamespace(identifier='test', remote_root=str(tmp_path), command=command,
        store=SimpleNamespace(read=lambda identifier: {'exchanges': ['binance', 'bybit']}))
    cache.stage_public_markets(connection)
    for ex in ('binance', 'bybit'):
        target = tmp_path / 'output/caches' / ex / 'markets.json'
        assert json.loads(target.read_text())['ETH/USDT:USDT']['swap']
        assert abs(target.stat().st_mtime - stamp) < .01
    assert not (tmp_path / 'input').exists()


def test_local_metadata_snapshot_filters_perpetuals_and_keeps_timestamp(tmp_path, monkeypatch):
    """Only local linear perpetuals reach PB8 with the source file's real age."""
    root = tmp_path / 'coindata'
    source = root / 'bybit/ccxt_markets.json'
    source.parent.mkdir(parents=True)
    source.write_text(json.dumps({
        'BTC/USDT:USDT': {'symbol': 'BTC/USDT:USDT', 'quote': 'USDT', 'swap': True, 'linear': True},
        'BTC/USDT': {'symbol': 'BTC/USDT', 'quote': 'USDT', 'spot': True, 'swap': False},
    }))
    stamp = time.time() - 60
    os.utime(source, (stamp, stamp))
    monkeypatch.setattr(cache, 'MARKET_ROOT', root)
    result = asyncio.run(cache.fetch_markets(['bybit']))
    assert list(result['bybit']['markets']) == ['BTC/USDT:USDT']
    assert abs(result['bybit']['fetched_at'] - stamp) < 0.01
    os.utime(source, (stamp - 86400, stamp - 86400))
    with pytest.raises(VastError, match='older than 24 hours'):
        asyncio.run(cache.fetch_markets(['bybit']))

def test_unknown_exchange_never_connects():
    """Reject invalid persisted exchange identifiers before network activity."""
    connection = SimpleNamespace(identifier='test',
        store=SimpleNamespace(read=lambda identifier: {'exchanges': ['../bad']}))
    with pytest.raises(VastError, match='unsupported'):
        cache.stage_public_markets(connection)
