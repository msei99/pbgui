"""Offline coverage for public metadata staging on existing queued jobs."""

import asyncio
import json
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


def test_metadata_failure_closes_client(monkeypatch):
    """A blocked exchange must still release its asynchronous resources."""
    import ccxt.async_support as ccxt
    closed = []

    class Client:
        """Synthetic exchange rejecting its market request."""
        def __init__(self, options):
            """Accept the production constructor options."""
        async def load_markets(self, reload):
            """Simulate a region-blocked public endpoint."""
            raise ValueError('blocked')
        async def close(self):
            """Record deterministic cleanup."""
            closed.append(True)

    monkeypatch.setattr(ccxt, 'bybit', Client)
    with pytest.raises(ValueError, match='blocked'):
        asyncio.run(cache.fetch_markets(['bybit']))
    assert closed == [True]


def test_unknown_exchange_never_connects():
    """Reject invalid persisted exchange identifiers before network activity."""
    connection = SimpleNamespace(identifier='test',
        store=SimpleNamespace(read=lambda identifier: {'exchanges': ['../bad']}))
    with pytest.raises(VastError, match='unsupported'):
        cache.stage_public_markets(connection)
