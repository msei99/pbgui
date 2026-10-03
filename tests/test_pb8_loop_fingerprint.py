"""Offline content fingerprint, legacy admission and unchanged-file regressions."""
from __future__ import annotations

import os
from pathlib import Path

import pytest

from api import optimize_v8 as opt
from pb8_loop_backend import LoopBackend
from pb8_loop_store import LoopStore

OWNER = 'a' * 32


@pytest.fixture
def snapshot(tmp_path, monkeypatch):
    """Use one temporary candle shard and mock only runtime/shard resolution."""
    from setup.vast_gpu_benchmark import prepare
    import market_data
    source = tmp_path / 'market' / '2026-09-01.npz'
    source.parent.mkdir()
    source.write_bytes(b'unchanged-candles')
    monkeypatch.setattr(market_data, 'get_market_data_root_dir', lambda: source.parent)
    monkeypatch.setattr(prepare, 'select_shards', lambda *args: [(source, Path(source.name))])
    monkeypatch.setattr(opt, 'pb8_runtime_status', lambda: {'ready': True, 'pb8dir': str(tmp_path)})
    monkeypatch.setattr(opt, '_runtime_commit', lambda path: 'reviewed-pb8')
    store = LoopStore(tmp_path / 'loops')
    backend = LoopBackend(tmp_path, store)
    config = {'backtest': {}, 'optimize': {}, 'bot': {}, 'live': {}}
    fingerprint = backend.fingerprint(config)
    row = store.create(OWNER, {'hours': 12, 'max_runs': 20}, config, {}, fingerprint, [], {})
    monkeypatch.setattr(backend, 'poll', lambda *args: {'status': 'completed'})
    return source, backend, store, row


def test_identical_candle_rewrite_does_not_stop_a_loop(snapshot):
    """A new mtime with byte-identical data remains a comparable snapshot."""
    source, backend, store, row = snapshot
    before = row['fingerprint']
    stamp = source.stat().st_mtime_ns
    source.write_bytes(source.read_bytes())
    os.utime(source, ns=(stamp + 1000000, stamp + 1000000))
    after = backend.fingerprint(row['initial_config'])
    assert after['data'] == before['data']
    assert after['metadata_data'] != before['metadata_data']
    assert backend._start_locked(row, {})['status'] == 'completed'
    assert store.read(OWNER, row['id'])['status'] == row['status']


def test_actual_candle_change_still_blocks_before_launch(snapshot):
    """Different contents remain a hard comparison boundary even at the same mtime."""
    source, backend, store, row = snapshot
    stamp = source.stat().st_mtime_ns
    source.write_bytes(b'different-candle')
    os.utime(source, ns=(stamp, stamp))
    with pytest.raises(ValueError, match='input data changed'):
        backend._start_locked(row, {})
    assert store.read(OWNER, row['id'])['status'] == 'unconfirmed'


def test_unchanged_legacy_snapshot_upgrades_only_after_metadata_proof(snapshot):
    """Existing admitted runs upgrade without silently accepting changed inputs."""
    source, backend, store, row = snapshot
    old = row['fingerprint']
    legacy = {'pb8': old['pb8'], 'data': old['metadata_data'], 'data_status': old['data_status']}
    row = store.update(OWNER, row['id'], lambda current: current.update(fingerprint=legacy))
    assert backend._start_locked(row, {})['status'] == 'completed'
    upgraded = store.read(OWNER, row['id'])['fingerprint']
    assert upgraded['data_signature_version'] == 2 and upgraded['data'] == old['data']
    source.write_bytes(b'changed-content')
    with pytest.raises(ValueError, match='input data changed'):
        backend._start_locked(store.read(OWNER, row['id']), {})


def test_changed_legacy_metadata_is_not_blindly_rebased(snapshot):
    """A legacy snapshot cannot be upgraded merely because current files hash cleanly."""
    source, backend, store, row = snapshot
    old = row['fingerprint']
    row = store.update(OWNER, row['id'], lambda current: current.update(
        fingerprint={'pb8': old['pb8'], 'data': old['metadata_data'], 'data_status': old['data_status']}))
    stamp = source.stat().st_mtime_ns
    os.utime(source, ns=(stamp + 1000000, stamp + 1000000))
    with pytest.raises(ValueError, match='input data changed'):
        backend._start_locked(row, {})
