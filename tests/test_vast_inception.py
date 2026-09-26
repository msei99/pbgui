"""Offline regression coverage for the cloud worker's missing-inception failure."""

import gzip
import hashlib
import json
from pathlib import Path
import shlex
from types import SimpleNamespace

import pytest

import vast_inception as inception
from setup.vast_gpu_benchmark import inception_cache as receiver
from setup.vast_gpu_benchmark.cloud_worker import safe_path
from vast_provider import VastError
from vast_transfer import WorkerConnection


def snapshot():
    """Return a complete PB8 resolver-v2 snapshot for both selected venues."""
    return {'version': 2, 'files': dict(zip(inception.CACHE_FILES, (
        {'SOL': 1600041600000},
        {'SOL': {'binanceusdm': 1600041600000, 'bybit': 1634256000000}},
        {'SOL': {'binanceusdm': 'SOL/USDT:USDT', 'bybit': 'SOL/USDT:USDT'}},
    )))}


def test_required_markets_uses_only_exported_mapped_perpetuals(tmp_path):
    """Export paths select the existing mapping, never ad-hoc symbol guessing."""
    folder = tmp_path / 'binance'
    folder.mkdir()
    (folder / 'mapping.json').write_text(json.dumps([
        {'coin': 'SOL', 'ccxt_symbol': 'SOL/USDT:USDT', 'quote': 'USDT', 'swap': True, 'linear': True},
        {'coin': 'SOL', 'ccxt_symbol': 'SOL/USDT', 'quote': 'USDT', 'swap': False, 'linear': False}]))
    manifest = {'files': [{'path': 'ohlcv/binance/1m/SOL_USDT:USDT/2024-01-01.npy'},
                          {'path': 'ohlcv/binance/1m/SOL_USDT:USDT/2024-01-02.npy'}]}
    assert inception.required_markets(manifest, tmp_path) == {'binance': {'SOL': 'SOL/USDT:USDT'}}
    (folder / 'mapping.json').write_text('[]')
    with pytest.raises(VastError, match='Missing or ambiguous'):
        inception.required_markets(manifest, tmp_path)


def test_receiver_installs_all_cache_files_and_version(tmp_path):
    """The unified cache alone is insufficient; exchange and symbol caches accompany it."""
    source = tmp_path / 'utils.py'
    source.write_text('FIRST_OHLCV_TIMESTAMPS_CACHE_VERSION = 2\n')
    data = snapshot()
    raw = json.dumps(data).encode()
    receiver.install(tmp_path, raw, hashlib.sha256(raw).hexdigest(), safe_path, source)
    cache = tmp_path / 'output/caches'
    for name, content in data['files'].items():
        assert json.loads((cache / name).read_text()) == content
        assert (cache / name).stat().st_mode & 0o777 == 0o600
    assert (cache / 'first_ohlcv_timestamps_unified.version').read_text() == '2'


@pytest.mark.parametrize('failure', ['version', 'checksum', 'symbols', 'timestamp'])
def test_invalid_snapshot_is_rejected_before_writing(tmp_path, failure):
    """Incomplete or incompatible metadata must not publish a usable cache version."""
    source = tmp_path / 'utils.py'
    source.write_text('FIRST_OHLCV_TIMESTAMPS_CACHE_VERSION = 2\n')
    data = snapshot()
    if failure == 'version':
        data['version'] = 1
    if failure == 'symbols':
        data['files'][inception.CACHE_FILES[2]]['SOL'].pop('bybit')
    if failure == 'timestamp':
        data['files'][inception.CACHE_FILES[1]]['SOL']['bybit'] = 0
    raw = json.dumps(data).encode()
    with pytest.raises(ValueError):
        receiver.install(tmp_path, raw, 'bad' if failure == 'checksum' else hashlib.sha256(raw).hexdigest(), safe_path, source)
    assert not (tmp_path / 'output').exists()


def test_local_inception_requires_matching_cached_symbols(tmp_path, monkeypatch):
    """Use only matching local PB8 entries and reject stale symbol identities."""
    cache = tmp_path / 'caches'
    cache.mkdir()
    data = snapshot()
    (cache / 'first_ohlcv_timestamps_unified.version').write_text('2')
    for name, value in data['files'].items():
        (cache / name).write_text(json.dumps(value))
    monkeypatch.setattr(inception, 'pb8dir', lambda: str(tmp_path))
    markets = {'bybit': {'SOL': 'SOL/USDT:USDT'}}
    result = inception.load_local_inception(markets)
    assert result['files'][inception.CACHE_FILES[0]] == {'SOL': 1634256000000}
    assert result['files'][inception.CACHE_FILES[1]] == {'SOL': {'bybit': 1634256000000}}
    with pytest.raises(VastError, match='missing or outdated for bybit/SOL'):
        inception.load_local_inception({'bybit': {'SOL': 'OTHER/USDT:USDT'}})


def test_stage_transfers_verified_snapshot_without_changing_input(tmp_path, monkeypatch):
    """Old queued jobs receive a separate runtime cache rather than a rewritten bundle."""
    monkeypatch.setattr(inception, 'required_markets', lambda *args: {'bybit': {'SOL': 'SOL/USDT:USDT'}})
    monkeypatch.setattr(inception, 'load_local_inception', lambda markets: snapshot())
    source = tmp_path / 'utils.py'
    source.write_text('FIRST_OHLCV_TIMESTAMPS_CACHE_VERSION = 2\n')
    def command(value, stdin, **kwargs):
        """Validate the transmitted bytes with the real receiver function."""
        raw = gzip.decompress(stdin.read())
        receiver.install(tmp_path, raw, shlex.split(value)[-1], safe_path, source)
    connection = SimpleNamespace(identifier='a'*32, remote_root='/work/pbgui/jobs/' + 'a'*32,
        store=SimpleNamespace(read=lambda *args: {'files': []}), command=command)
    inception.stage_inception(connection)
    assert (tmp_path / 'output/caches/first_ohlcv_timestamps_unified.version').exists()
    assert not (tmp_path / 'input').exists()


@pytest.mark.parametrize('failure', [False, True])
def test_start_requires_inception_before_remote_launch(monkeypatch, failure):
    """A missing inception snapshot prevents the optimizer command from being sent."""
    connection = object.__new__(WorkerConnection)
    connection.identifier = 'a'*32
    connection.remote_root = '/work/pbgui/jobs/' + 'a'*32
    connection.store = SimpleNamespace(read=lambda *args: {})
    calls = []
    monkeypatch.setattr('vast_market_cache.stage_public_markets', lambda conn: calls.append('markets'))
    def prepare(conn):
        """Record inception staging and optionally fail before any remote optimizer starts."""
        calls.append('inception')
        if failure:
            raise VastError('Missing first candle', 422)
    monkeypatch.setattr(inception, 'stage_inception', prepare)
    connection.command = lambda *args, **kwargs: calls.append('run')
    if failure:
        with pytest.raises(VastError, match='Missing first candle'):
            connection.start()
        assert calls == ['markets', 'inception']
    else:
        connection.start()
        assert calls == ['markets', 'inception', 'run']
