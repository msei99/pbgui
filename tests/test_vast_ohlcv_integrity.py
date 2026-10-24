"""Offline byte-bound admission, original-worker parity and controller regressions."""

import asyncio
import copy
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
import socket
import subprocess
import sys
from types import SimpleNamespace

import numpy as np
import pytest

import vast_ohlcv_integrity as integrity
from vast_history import integrity_requirements
from vast_inception import CACHE_FILES
from vast_jobs import JobStore, REVISION, digest, write_json
from vast_provider import VastError
from vast_scenarios import scenario_plan
from vast_direction_fixtures import direction_config
from pb8_ohlcv_reference import accepts, namespace, source_manager

MINUTE = 60_000
START = 1767225600000  # 2026-01-01 UTC.


def candles(path, timestamps, *, seconds=False, duplicates=False):
    """Write tiny valid synthetic shards exclusively under pytest tmp_path."""
    path.parent.mkdir(parents=True, exist_ok=True)
    ts = np.asarray(timestamps, dtype=float) / (1000 if seconds else 1)
    if duplicates:
        ts = np.concatenate([ts[::-1], ts[:2]])
    if path.suffix == '.npz':
        arr = np.ones(len(ts), dtype=[('ts', 'i8'), ('o', 'f4'), ('h', 'f4'), ('l', 'f4'), ('c', 'f4'), ('bv', 'f4')])
        arr['ts'] = ts
        np.savez_compressed(path, candles=arr)
    else:
        arr = np.ones((len(ts), 6))
        arr[:, 0] = ts
        np.save(path, arr)
    return path


def export(root, paths, *, start=START, end=START + 180 * MINUTE, tolerance=120, listing=START, coin='BTC'):
    """Use identical exported paths, config and listing inputs for every comparison."""
    shards = [(path, path.relative_to(root)) for path in paths]
    requests = [dict(label='bybit', exchanges=['bybit'], coins=[coin], start_ts=start, end_ts=end, tolerance=tolerance)]
    markets = {'bybit': {coin: coin + '/USDT:USDT'}}
    inception = {'files': {CACHE_FILES[1]: {coin: {'bybit': listing}}}}
    return integrity.validate_export(shards, requests, markets, inception, root / 'checksums.sqlite')


@pytest.mark.parametrize('gap,tolerance,expected', [(31, 30, False), (31, 31, True),
    (120, 119, False), (120, 120, True), (121, 120, False), (1, 0, True)])
def test_original_gap_tolerance_parity(tmp_path, gap, tolerance, expected):
    """The same bytes cross both configured boundaries with the original decision."""
    root = tmp_path / 'source'
    ts = START + np.array([0, 1, 1 + gap, 180]) * MINUTE
    # Fill every minute except the one intentional internal gap.
    ts = START + np.concatenate([np.arange(2), np.arange(1 + gap, 181)]) * MINUTE
    path = candles(root / 'bybit/1m/BTC_USDT:USDT/2026-01-01.npz', ts)
    original = tmp_path / 'original'
    shutil.copytree(root, original)
    assert accepts(original, START, START + 180 * MINUTE, tolerance, START) is expected
    if expected:
        assert export(root, [path], tolerance=tolerance)
    else:
        with pytest.raises(VastError, match=r'bybit/BTC 2026-01-01: .*missing minutes'):
            export(root, [path], tolerance=tolerance)


@pytest.mark.parametrize('offset,listing_offset,end_offset,expected', [
    (0, -1000, 0, True), (30, 30, 0, True), (30, 29, 0, True),
    (30, 28, 0, False), (0, 0, -1, False), (181, 181, 0, False)])
def test_original_listing_and_edges(tmp_path, offset, listing_offset, end_offset, expected):
    """Only authoritative listing permits a missing prefix; trailing minutes never do."""
    root = tmp_path / 'source'
    path = candles(root / 'bybit/1m/BTC_USDT:USDT/2026-01-01.npy',
                   START + np.arange(offset, 181 + end_offset) * MINUTE)
    original = tmp_path / 'original'
    shutil.copytree(root, original)
    listing = START + listing_offset * MINUTE
    assert accepts(original, START, START + 180 * MINUTE, 120, listing) is expected
    if expected:
        export(root, [path], listing=listing)
    else:
        with pytest.raises(VastError):
            export(root, [path], listing=listing)


@pytest.mark.parametrize('npz_mode,expected', [('healthy', True), ('gapped', False), ('broken', True), ('empty', True)])
def test_original_format_priority_and_normalization(tmp_path, npz_mode, expected):
    """Readable NPZ wins; broken/empty NPZ falls back to seconds-based NPY unchanged."""
    root = tmp_path / 'source'
    base = root / 'bybit/1m/BTC_USDT:USDT/2026-01-01'
    npy = candles(base.with_suffix('.npy'), START + np.arange(181) * MINUTE, seconds=True, duplicates=True)
    npz = candles(base.with_suffix('.npz'), START + np.arange(181) * MINUTE)
    if npz_mode == 'gapped':
        candles(npz, [START, START + 180 * MINUTE])
    elif npz_mode == 'broken':
        npz.write_bytes(b'not an archive')
    elif npz_mode == 'empty':
        candles(npz, [])
    hashes = [digest(p) for p in [npy, npz]]
    original = tmp_path / 'original'
    shutil.copytree(root, original)
    assert accepts(original, START, START + 180 * MINUTE, 120, START) is expected
    if expected:
        export(root, [npy, npz])
    else:
        with pytest.raises(VastError):
            export(root, [npy, npz])
    assert hashes == [digest(p) for p in [npy, npz]]


@pytest.mark.parametrize('missing_day,expected', [(False, True), (True, False)])
def test_original_day_boundaries_and_missing_day(tmp_path, missing_day, expected):
    """Carry gaps over day boundaries and reject an absent middle day."""
    root = tmp_path / 'source'
    paths = []
    for index in range(3):
        if missing_day and index == 1:
            continue
        day = datetime.fromtimestamp((START + index * 1440 * MINUTE) / 1000, timezone.utc).strftime('%Y-%m-%d')
        paths.append(candles(root / f'bybit/1m/BTC_USDT:USDT/{day}.npz', START + np.arange(index * 1440, (index + 1) * 1440) * MINUTE))
    end = START + 2 * 1440 * MINUTE
    original = tmp_path / 'original'
    shutil.copytree(root, original)
    assert accepts(original, START, end, 120, START) is expected
    if expected:
        export(root, paths, end=end)
    else:
        with pytest.raises(VastError, match='2026-01-02: 1440 missing minutes'):
            export(root, paths, end=end)


@pytest.mark.parametrize('coin,expected', [('BTC', False), ('xyz:NVDA', True)])
def test_original_tradfi_closure(tmp_path, coin, expected):
    """Preserve the original four-day stock-perp closure exception."""
    root = tmp_path / 'source'
    end = START + 3 * 1440 * MINUTE
    paths = [candles(root / f'bybit/1m/{coin}_USDT:USDT/2026-01-01.npz', [START]),
             candles(root / f'bybit/1m/{coin}_USDT:USDT/2026-01-04.npz', [end])]
    original = tmp_path / 'original'
    shutil.copytree(root, original)
    assert accepts(original, START, end, 120, START, coin) is expected
    if expected:
        export(root, paths, end=end, coin=coin)
    else:
        with pytest.raises(VastError):
            export(root, paths, end=end, coin=coin)


def test_range_clipping_cache_config_metadata_and_no_network(tmp_path, monkeypatch):
    """Cache file facts only; config/listing/window changes rerun admission offline."""
    root = tmp_path / 'source'
    path = candles(root / 'bybit/1m/BTC_USDT:USDT/2026-01-01.npz', START + np.array([0, 120, 121, 122]) * MINUTE)
    monkeypatch.setattr(socket, 'create_connection', lambda *a, **kw: pytest.fail('network attempted'))
    calls = []
    probe = integrity._probe
    monkeypatch.setattr(integrity, '_probe', lambda *a: calls.append(1) or probe(*a))
    export(root, [path], end=START + 122 * MINUTE)
    with pytest.raises(VastError):
        export(root, [path], end=START + 122 * MINUTE, tolerance=119)
    export(root, [path], start=START + 120 * MINUTE, end=START + 122 * MINUTE, tolerance=1)
    with pytest.raises(VastError, match='listing prefix'):
        export(root, [path], start=START - MINUTE, end=START + 122 * MINUTE, listing=START - 10 * MINUTE)
    export(root, [path], start=START - MINUTE, end=START + 122 * MINUTE, listing=START)
    assert len(calls) == 1
    before = path.stat()
    candles(path, START + np.arange(123) * MINUTE)
    import os
    os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
    export(root, [path], end=START + 122 * MINUTE, tolerance=1)
    assert len(calls) == 2


def test_unstable_read_retries_at_most_three(tmp_path, monkeypatch):
    """A concurrent writer never publishes a usable validation result."""
    path = candles(tmp_path / '2026-01-01.npy', [START])
    count = []
    def changing(path):
        """Return distinct synthetic file generations for each stat."""
        count.append(1)
        return (len(count),)
    monkeypatch.setattr(integrity, 'signature', changing)
    with pytest.raises(VastError, match='changed during integrity'):
        integrity.inspect_file(path, tmp_path / 'checksums.sqlite')
    assert len(count) == 6


@pytest.fixture
def preparation(tmp_path, monkeypatch):
    """An actual JobStore export using local synthetic markets and resolver caches."""
    import vast_inception
    import vast_market_cache
    config = direction_config()
    config['backtest'].update(start_date='2026-01-01', end_date='2026-01-02', exchanges=['bybit'], gap_tolerance_ohlcvs_minutes=120)
    config['live']['approved_coins'] = {'long': ['BTC'], 'short': []}
    mapping_root = tmp_path / 'mapping'
    mapping = mapping_root / 'bybit'
    mapping.mkdir(parents=True)
    symbol = 'BTC/USDT:USDT'
    (mapping / 'mapping.json').write_text(json.dumps([dict(coin='BTC', quote='USDT', swap=True, linear=True, ccxt_symbol=symbol)]))
    (mapping / 'ccxt_markets.json').write_text(json.dumps({symbol: dict(symbol=symbol, quote='USDT', swap=True, linear=True)}))
    cache = tmp_path / 'pb8/caches'
    cache.mkdir(parents=True)
    (cache / 'first_ohlcv_timestamps_unified.version').write_text('2')
    for filename, value in zip(CACHE_FILES, [{'BTC': START}, {'BTC': {'bybit': START}}, {'BTC': {'bybit': symbol}}]):
        (cache / filename).write_text(json.dumps(value))
    monkeypatch.setattr(vast_inception, 'pb8dir', lambda: str(cache.parent))
    monkeypatch.setattr(vast_inception, 'pb8venv', lambda: sys.executable)
    monkeypatch.setattr(vast_market_cache, 'MARKET_ROOT', mapping_root)
    monkeypatch.setattr('pb8_config._call_helper', lambda *a, **kw: {'minutes': [0] * len(kw['configs'])})
    monkeypatch.setattr('pb8_config.save_prepared_pb8_config', lambda config, path: write_json(path, config))
    root = tmp_path / 'raw'
    paths = [candles(root / 'bybit/1m/BTC_USDT:USDT/2026-01-01.npz', START + np.arange(1440) * MINUTE),
             candles(root / 'bybit/1m/BTC_USDT:USDT/2026-01-02.npz', [START + 1440 * MINUTE])]
    store = JobStore(tmp_path / 'vast')
    return SimpleNamespace(config=config, paths=paths, cache=cache, mapping=mapping, root=root, store=store,
        run=lambda: store.prepare('synthetic', config, 'source-hash', root, mapping_root, tmp_path / 'results', 512, 4, False))


def test_preparation_metadata_order_before_integrity_copy_archive(preparation, monkeypatch):
    """Resolve missing entries before the file gate, then copy/hash/archive and seal."""
    p = preparation
    events = []
    (p.cache / CACHE_FILES[1]).write_text('{}')
    def resolve(argv, **kwargs):
        """An isolated metadata resolver supplies exactly the missing entry."""
        events.append('resolve')
        assert kwargs['timeout'] == 300
        return SimpleNamespace(returncode=0, stdout=json.dumps({'bybit': {'BTC': {'timestamp': START, 'symbol': 'BTC/USDT:USDT'}}}))
    monkeypatch.setattr('vast_inception.subprocess.run', resolve)
    validate = integrity.validate_export
    def gate(*args):
        """Observe ordering while keeping the real file validator."""
        events.append('gate')
        assert events == ['resolve', 'gate']
        return validate(*args)
    monkeypatch.setattr(integrity, 'validate_export', gate)
    row = p.run()
    assert row['status'] == 'ready' and row['rental_state'] == 'none'
    manifest = integrity.require_manifest_proof(p.store, row['id'])
    assert manifest['metadata_snapshot']['inception']['files'][CACHE_FILES[1]]['BTC']['bybit'] == START
    assert manifest['ohlcv_integrity']['contract'] == integrity.CONTRACT
    assert manifest['pb8_revision'] == REVISION
    assert len(manifest['files']) == 2


@pytest.mark.parametrize('fault', ['gap', 'snapshot_missing', 'symbol_missing', 'stale_snapshot', 'invalid_cache', 'resolver_error', 'unresolved'])
def test_preparation_failure_never_creates_archive_or_rental(preparation, monkeypatch, fault):
    """Metadata and candle failures remain distinct and persist failed without packaging."""
    import os
    p = preparation
    if fault == 'gap':
        candles(p.paths[0], [START, START + 1439 * MINUTE])
    elif fault == 'snapshot_missing':
        (p.mapping / 'ccxt_markets.json').unlink()
    elif fault == 'symbol_missing':
        (p.mapping / 'ccxt_markets.json').write_text(json.dumps({'ETH/USDT:USDT': dict(symbol='ETH/USDT:USDT', quote='USDT', swap=True, linear=True)}))
    elif fault == 'stale_snapshot':
        os.utime(p.mapping / 'ccxt_markets.json', (0, 0))
    else:
        (p.cache / CACHE_FILES[1]).write_text('[]' if fault == 'invalid_cache' else '{}')
        def resolver(*args, **kwargs):
            """Inject an explicit failure without a network or second resolver round."""
            if fault == 'resolver_error':
                raise subprocess.TimeoutExpired('isolated resolver', 300)
            return SimpleNamespace(returncode=0, stdout='{}')
        monkeypatch.setattr('vast_inception.subprocess.run', resolver)
    with pytest.raises(VastError) as error:
        p.run()
    row = p.store.list()[0]
    assert row['status'] == 'failed' and row['rental_state'] == 'none'
    assert row['error'] == str(error.value)
    assert not (p.store.directory(row['id']) / 'input.tar.gz').exists()
    assert ('OHLCV integrity' in row['error']) is (fault == 'gap')


@pytest.mark.parametrize('phase', ['after_validation', 'during_copy'])
def test_file_change_never_archives_unchecked_bytes(preparation, monkeypatch, phase):
    """A writer between scan and copy or during copying cannot release a bundle."""
    p = preparation
    if phase == 'after_validation':
        validate = integrity.validate_export
        def changing(*args):
            """Change source bytes after the real scan returns its hash."""
            result = validate(*args)
            candles(p.paths[0], [START])
            return result
        monkeypatch.setattr(integrity, 'validate_export', changing)
    else:
        copier = shutil.copyfileobj
        def changing(src, dest, *args):
            """Copy valid bytes, then mutate the actual source before its post-stat."""
            result = copier(src, dest, *args)
            candles(p.paths[0], [START])
            return result
        monkeypatch.setattr(shutil, 'copyfileobj', changing)
    with pytest.raises(VastError, match='changed'):
        p.run()
    row = p.store.list()[0]
    assert row['status'] == 'failed'
    assert not (p.store.directory(row['id']) / 'input.tar.gz').exists()


@pytest.mark.parametrize('fault', ['proof_missing', 'proof_changed', 'requirements', 'metadata', 'config', 'archive', 'loose_file'])
def test_invalid_proof_blocks_callers(preparation, monkeypatch, fault):
    """Direct starts, clone, queue, dispatch and calibration reject changed evidence."""
    from vast_queue import preflight_gpu_snapshots, worker_compatible_jobs, CloudQueue
    from vast_calibration import configurable_calibration_plan
    p = preparation
    row = p.run()
    folder = p.store.directory(row['id'])
    manifest = p.store.read(row['id'], 'input/manifest.json')
    if fault == 'proof_missing':
        manifest.pop('ohlcv_integrity')
    elif fault == 'proof_changed':
        manifest['ohlcv_integrity']['sha256'] = '0' * 64
    elif fault == 'requirements':
        manifest['ohlcv_requirements'][0]['tolerance'] = 10000
    elif fault == 'metadata':
        manifest['metadata_snapshot']['inception']['files'][CACHE_FILES[1]]['BTC']['bybit'] += MINUTE
    elif fault == 'config':
        (folder / 'input/optimize.json').write_text('{}')
    elif fault == 'archive':
        (folder / 'input.tar.gz').write_bytes(b'changed')
    else:
        (folder / 'input' / manifest['files'][0]['path']).write_bytes(b'changed')
    write_json(folder / 'input/manifest.json', manifest)
    with pytest.raises(VastError):
        integrity.require_manifest_proof(p.store, row['id'])
    assert preflight_gpu_snapshots(p.store, [row]) == []
    from vast_jobs import SUPPORTED_RENTAL_IMAGE_REVISIONS, IMAGE
    old_image = next(image for image in SUPPORTED_RENTAL_IMAGE_REVISIONS if image != IMAGE)
    assert worker_compatible_jobs(p.store, [row], image=old_image, revision=SUPPORTED_RENTAL_IMAGE_REVISIONS[old_image]) == []
    p.store.update(row['id'], status='ready')
    monkeypatch.setattr('vast_jobs.services_available', lambda: True)
    with pytest.raises(VastError):
        p.store.start(row['id'], dict(id=1, price_hour_usd=.1, cuda_max_good=13, disk_gb=40, cpu_cores=8), 1, 1)
    with pytest.raises(VastError):
        p.store.create_calibration(row['id'], configurable_calibration_plan(4096, 512, 8192, .03))
    with pytest.raises(VastError):
        CloudQueue(p.store).start({'id': 1, 'machine_id': 7}, 1, 1, 300)
    p.store.update(row['id'], status='failed')
    assert p.store.clone_prepared(row['id'], 'synthetic', 'source-hash', folder / 'results', 512, 4, False) is None
    assert p.store.read(row['id'])['rental_state'] == 'none'


@pytest.mark.parametrize('warmup', [0, 31, 1441])
def test_original_warmup_dates_and_required_tolerance(monkeypatch, warmup):
    """Use unchanged original prepare_hlcvs date arithmetic, not a local PB8 checkout."""
    config = direction_config()
    config['backtest'].update(start_date='2026-01-01', end_date='2026-01-03', exchanges=['bybit'], gap_tolerance_ohlcvs_minutes=31)
    monkeypatch.setattr('pb8_config._call_helper', lambda *a, **kw: {'minutes': [warmup] * len(kw['configs'])})
    request = integrity_requirements(config, scenario_plan(config)[0])[0]
    env = namespace()
    captured = []
    class Captured(Exception):
        """Stop at the infrastructure constructor after the real date decisions."""
    def manager(exchange, start, end, **kwargs):
        """Capture native effective dates without constructing any exchange client."""
        captured.append((start, end, kwargs))
        raise Captured
    env.update(HLCVManager=manager, _has_explicit_ohlcv_source_dir=lambda c: True,
        effective_backtest_data_coins=lambda c: ['BTC'], to_ccxt_exchange_id=lambda ex: ex,
        compute_backtest_warmup_minutes=lambda c: warmup, require_config_value=lambda c, p: c['backtest'][p.split('.')[1]])
    with pytest.raises(Captured):
        asyncio.run(env['prepare_hlcvs'](config, 'bybit'))
    assert int(env['date_to_ts'](captured[0][0])) == request['start_ts']
    assert int(env['date_to_ts'](captured[0][1])) == request['end_ts']
    assert captured[0][2]['gap_tolerance_ohlcvs_minutes'] == request['tolerance'] == 31
    config['backtest'].pop('gap_tolerance_ohlcvs_minutes')
    with pytest.raises(ValueError, match='gap_tolerance_ohlcvs_minutes'):
        integrity_requirements(config, scenario_plan(config)[0])


def test_original_scenario_dataset_windows(monkeypatch):
    """Scenario windows inherit tolerance and do not validate unused export margins."""
    config = direction_config()
    config['backtest'].update(start_date='2020-01-01', end_date='2026-01-10', exchanges=['bybit', 'binance'],
        gap_tolerance_ohlcvs_minutes=120, suite_enabled=True, scenarios=[
        dict(label='combined', start_date='2026-01-01', end_date='2026-01-03'),
        dict(label='single', start_date='2026-01-05', end_date='2026-01-06', exchanges=['bybit'])])
    contexts, _, errors = scenario_plan(config)
    assert not errors
    monkeypatch.setattr('pb8_config._call_helper', lambda *a, **kw: {'minutes': [31] * len(kw['configs'])})
    env = namespace()
    env['require_config_value'] = lambda c, path: c['backtest'][path.split('.')[1]]
    env['DatasetWindow'] = lambda start, end, labels: SimpleNamespace(start_date=start, end_date=end, scenario_labels=labels)
    scenarios = [SimpleNamespace(start_date=c['config']['backtest']['start_date'], end_date=c['config']['backtest']['end_date'],
        exchanges=c['exchanges'], label=c['label']) for c in contexts]
    windows = env['_derive_dataset_windows'](config, ['binance', 'bybit'], scenarios, needed_individual_exchanges={'bybit'})
    requests = integrity_requirements(config, contexts)
    assert {r['label'] for r in requests} == set(windows)
    for request in requests:
        window = windows[request['label']]
        assert request['start_ts'] == int(env['date_to_ts'](window.start_date)) - 31 * MINUTE
        assert request['end_ts'] == int(env['date_to_ts'](window.end_date))
        assert request['tolerance'] == 120


@pytest.mark.parametrize('offset,tolerance,expected', [(30000, 0, False), (30000, 1, True)])
def test_original_subminute_intervals(tmp_path, offset, tolerance, expected):
    """Preserve the original minimum greatest interval for irregular timestamps."""
    root = tmp_path / 'source'
    path = candles(root / 'bybit/1m/BTC_USDT:USDT/2026-01-01.npy', [START, START + offset, START + MINUTE])
    original = tmp_path / 'original'
    shutil.copytree(root, original)
    assert accepts(original, START, START + MINUTE, tolerance, START) is expected
    if expected:
        export(root, [path], end=START + MINUTE, tolerance=tolerance)
    else:
        with pytest.raises(VastError):
            export(root, [path], end=START + MINUTE, tolerance=tolerance)


def test_nonnumeric_npy_values_fail_before_rental(tmp_path):
    """An NPY the original cannot consume is rejected without rewriting its bytes."""
    root = tmp_path / 'source'
    path = candles(root / 'bybit/1m/BTC_USDT:USDT/2026-01-01.npy', [START, START + MINUTE])
    arr = np.load(path).astype(str)
    arr[0, 4] = 'bad'
    np.save(path, arr)
    before = path.read_bytes()
    original = tmp_path / 'original'
    shutil.copytree(root, original)
    with pytest.raises((ValueError, TypeError)):
        accepts(original, START, START + MINUTE, 120, START)
    with pytest.raises(VastError):
        export(root, [path], end=START + MINUTE)
    assert path.read_bytes() == before


def test_expired_frozen_metadata_is_rejected_by_direct_start(preparation, monkeypatch):
    """The proof gate enforces the same 24-hour staging requirement before payment."""
    import time
    p = preparation
    row = p.run()
    monkeypatch.setattr(integrity.time, 'time', lambda: 2**40)
    with pytest.raises(VastError, match='expired'):
        integrity.require_manifest_proof(p.store, row['id'])
    monkeypatch.setattr('vast_jobs.services_available', lambda: True)
    with pytest.raises(VastError, match='expired'):
        p.store.start(row['id'], dict(id=1, price_hour_usd=.1, cuda_max_good=13, disk_gb=40, cpu_cores=8), 1, 1)
    assert p.store.read(row['id'])['rental_state'] == 'none'


def test_relative_dates_are_frozen_across_midnight(preparation, monkeypatch):
    """Archive configs and scenario dates retain the exact validated window next day."""
    import vast_history
    p = preparation
    class Clock(datetime):
        """An isolated UTC clock independent of the host day."""
        current = datetime(2026, 1, 4, 23, 59, tzinfo=timezone.utc)
        @classmethod
        def now(cls, tz=None):
            """Return the chosen synthetic clock instant."""
            return cls.current
    monkeypatch.setattr(vast_history, 'datetime', Clock)
    p.config['backtest'].update(end_date='now', suite_enabled=True, scenarios=[{'label': 'relative', 'end_date': 'now'}])
    row = p.run()
    frozen = p.store.read(row['id'], 'input/optimize.json')
    manifest = p.store.read(row['id'], 'input/manifest.json')
    assert frozen['backtest']['end_date'] == frozen['backtest']['scenarios'][0]['end_date'] == '2026-01-02'
    Clock.current = datetime(2026, 1, 5, 0, 1, tzinfo=timezone.utc)
    assert integrity_requirements(frozen, scenario_plan(frozen)[0]) == manifest['ohlcv_requirements']


@pytest.mark.parametrize('reason', ['timeout', 'cancel'])
def test_resolver_timeout_and_abort_reap_child_and_temporary_cache(preparation, monkeypatch, tmp_path, reason):
    """Exercise real subprocess cleanup with isolated PB8 infrastructure only."""
    import os
    import time
    import vast_inception
    p = preparation
    temporary = tmp_path / 'resolver-temporary'
    temporary.mkdir()
    monkeypatch.setenv('TMPDIR', str(temporary))
    import tempfile
    monkeypatch.setattr(tempfile, 'tempdir', str(temporary))
    marker = tmp_path / 'resolver-started.json'
    source = p.cache.parent / 'src'
    source.mkdir()
    (source / 'procedures.py').write_text('import asyncio,json,os\nfrom pathlib import Path\n'
        'async def get_first_timestamps_unified(*args, **kwargs):\n'
        f'    Path({str(marker)!r}).write_text(json.dumps(dict(pid=os.getpid(), cwd=str(Path.cwd()))))\n'
        '    await asyncio.sleep(60)\n')
    (p.cache / CACHE_FILES[1]).write_text('{}')
    run = subprocess.run
    seen = []
    def bounded(argv, **kwargs):
        """Shorten only the test wait; retain the production 300-second assertion."""
        seen.append(kwargs['timeout'])
        assert kwargs['timeout'] == 300
        if reason == 'timeout':
            kwargs['timeout'] = .5
            return run(argv, **kwargs)
        communicate = subprocess.Popen.communicate
        def interrupted(process, *args, **kw):
            """Interrupt the real owner after the child created its temporary cache."""
            kw['timeout'] = .5
            try:
                communicate(process, *args, **kw)
            except subprocess.TimeoutExpired:
                pass
            raise KeyboardInterrupt
        with monkeypatch.context() as patch:
            patch.setattr(subprocess.Popen, 'communicate', interrupted)
            return run(argv, **kwargs)
    monkeypatch.setattr(vast_inception.subprocess, 'run', bounded)
    with pytest.raises(VastError if reason == 'timeout' else KeyboardInterrupt):
        vast_inception.ensure_local_inception({'bybit': {'BTC': 'BTC/USDT:USDT'}})
    assert seen == [300]
    assert marker.exists(), 'The isolated child must actually reach its cache directory'
    child = json.loads(marker.read_text())
    with pytest.raises(ProcessLookupError):
        os.kill(child['pid'], 0)
    assert not Path(child['cwd']).exists()
    assert not list(temporary.iterdir())


def test_fresh_prepared_input_stages_frozen_metadata_before_worker_launch(preparation, monkeypatch, tmp_path):
    """Exercise the real remote-start controller using only an isolated command sink."""
    import gzip
    from vast_transfer import WorkerConnection
    p = preparation
    row = p.run()
    manifest = p.store.read(row['id'], 'input/manifest.json')
    assert 'public_market_cache_mtimes' not in manifest  # Reserved for archived caches/ files.
    commands, payloads = [], []
    def command(text, **kwargs):
        """Capture metadata transfer bytes without SSH, a worker or any paid action."""
        commands.append(text)
        if kwargs.get('stdin'):
            payloads.append(json.loads(gzip.decompress(kwargs['stdin'].read())))
    monkeypatch.setattr('vast_market_cache._load_local_markets', lambda *a: pytest.fail('Frozen market snapshot ignored'))
    monkeypatch.setattr('vast_inception.load_local_inception', lambda *a: pytest.fail('Frozen inception ignored'))
    connection = SimpleNamespace(store=p.store, identifier=row['id'], remote_root=str(tmp_path / 'remote'), command=command)
    WorkerConnection.start(connection)
    assert len(commands) == 3 and 'nohup' in commands[-1]
    assert payloads == [manifest['metadata_snapshot']['markets'], manifest['metadata_snapshot']['inception']]
    assert p.store.read(row['id'])['rental_state'] == 'none'


@pytest.mark.parametrize('age_days', [0, 1, 2])
def test_original_minimum_age_window_parity(tmp_path, age_days):
    """Pre-age gaps do not reject data the unchanged combined coin planner never reads."""
    from vast_inception import CACHE_FILES
    env = namespace()
    env['CombinedCoinPlan'] = lambda **kwargs: SimpleNamespace(**kwargs)
    plan = env['_plan_combined_coin'](coin='ETH', base_start_ts=START, end_ts=START + 3 * 1440 * MINUTE,
        first_timestamps_unified={'ETH': START}, minimum_coin_age_days=age_days, min_coin_age_ms=age_days * 1440 * MINUTE,
        tradfi_for_stock_perps=True, forced_sources={}, exchanges_to_consider=['bybit'])
    assert plan.effective_start_ts == START + age_days * 1440 * MINUTE
    root = tmp_path / 'source'
    paths = []
    # Broken historical day is deliberately outside the native minimum-age window.
    for index in range(4):
        day = f'2026-01-0{index + 1}'
        timestamps = START + np.arange(index * 1440, (index + 1) * 1440) * MINUTE
        if index == 0:
            timestamps = [START, START + 1439 * MINUTE]
        paths.append(candles(root / f'bybit/1m/ETH_USDT:USDT/{day}.npz', timestamps))
    original = tmp_path / 'original'
    shutil.copytree(root, original)
    expected = age_days > 0
    assert accepts(original, plan.effective_start_ts, START + 3 * 1440 * MINUTE, 120, START, 'ETH') is expected
    inception = {'files': {CACHE_FILES[0]: {'ETH': START}, CACHE_FILES[1]: {'ETH': {'bybit': START}}}}
    requests = [dict(label='combined', exchanges=['bybit'], coins=['ETH'], start_ts=START,
        end_ts=START + 3 * 1440 * MINUTE, tolerance=120, minimum_coin_age_days=age_days)]
    args = ([(p, p.relative_to(root)) for p in paths], requests, {'bybit': {'ETH': 'ETH/USDT:USDT'}}, inception, root / 'checksums.sqlite')
    if expected:
        integrity.validate_export(*args)
    else:
        with pytest.raises(VastError):
            integrity.validate_export(*args)


def test_forced_source_outside_scenario_venues_is_checked(tmp_path):
    """A forced KuCoin source cannot be omitted from the offline gate."""
    root = tmp_path / 'source'
    path = candles(root / 'kucoin/1m/ETH_USDT:USDT/2026-01-01.npz', [START, START + 180 * MINUTE])
    requests = [dict(label='combined', exchanges=['bybit'], coin_sources={'ETH': 'kucoin'}, coins=['ETH'],
        start_ts=START, end_ts=START + 180 * MINUTE, tolerance=120)]
    inception = {'files': {CACHE_FILES[0]: {'ETH': START}, CACHE_FILES[1]: {'ETH': {'kucoin': START}}}}
    with pytest.raises(VastError, match='kucoin/ETH 2026-01-01'):
        integrity.validate_export([(path, path.relative_to(root))], requests, {'kucoin': {'ETH': 'ETH/USDT:USDT'}}, inception, root / 'checksums.sqlite')


def test_pinned_default_hydration_is_distinct_from_validator_defaults(tmp_path, monkeypatch):
    """Use the provenance-bound native template at the existing editor loader boundary."""
    from pb8_config import load_pb8_editor_config
    fixture = json.loads((Path(__file__).parent / 'fixtures/pb8_october_template.json').read_text())
    assert fixture['origin']['revision'] == REVISION
    assert fixture['origin']['callable'] == 'config.schema.get_template_config'
    assert fixture['config']['backtest']['gap_tolerance_ohlcvs_minutes'] == 120
    config = direction_config()
    config['backtest'].update(start_date='2026-01-01', end_date='2026-01-02')
    authored = tmp_path / 'authored.json'
    write_json(authored, config)
    hydrated = copy.deepcopy(config)
    hydrated['backtest']['gap_tolerance_ohlcvs_minutes'] = fixture['config']['backtest']['gap_tolerance_ohlcvs_minutes']
    loaded = load_pb8_editor_config(authored, loader=lambda path: hydrated, report_changes=True)
    assert loaded['param_status']['config_changes']['backtest.gap_tolerance_ohlcvs_minutes']['added'] is True
    monkeypatch.setattr('pb8_config._call_helper', lambda *a, **kw: {'minutes': [0] * len(kw['configs'])})
    assert integrity_requirements(loaded['config'], scenario_plan(loaded['config'])[0])[0]['tolerance'] == 120
    hydrated['backtest']['gap_tolerance_ohlcvs_minutes'] = 31
    assert integrity_requirements(hydrated, scenario_plan(hydrated)[0])[0]['tolerance'] == 31
    with pytest.raises(ValueError, match='gap_tolerance_ohlcvs_minutes'):
        integrity_requirements(config, scenario_plan(config)[0])


@pytest.mark.parametrize('age_days,listing_offset,expected', [(0, 30, START), (1, 30, START + 1470 * MINUTE),
    (1, 2000, START + 3440 * MINUTE), (1, 4320, None), (4, 30, None)])
def test_original_direct_minimum_age_and_empty_payload(preparation, age_days, listing_offset, expected):
    """Run unchanged single-exchange start, skip and empty-payload decisions."""
    env = namespace()
    config = {'live': {'minimum_coin_age_days': age_days}, 'backtest': {'ohlcv_source_dir': 'isolated'}}
    captured = []
    class StopAtFetch(Exception):
        """Stop only at the infrastructure boundary after native decisions."""
    async def first_timestamps(coins, exchange=None):
        """Supply the synthetic frozen authoritative inception snapshot."""
        # get_first_timestamps_unified(exchange=...) returns exchange-specific
        # values (pinned procedures.py:549–557), despite its historical name.
        return {'ETH': START + listing_offset * MINUTE}
    async def load_markets():
        """The synthetic market is present without exchange I/O."""
    async def get_first_timestamp(coin):
        """Supply the same exchange-specific timestamp as the production gate."""
        return START + listing_offset * MINUTE
    async def get_ohlcvs(coin):
        """Capture the actual native range; emulate only its empty-range infrastructure."""
        if manager.start > START + 4320 * MINUTE:
            return env['pd'].DataFrame(columns=['timestamp', 'high', 'low', 'close', 'volume'])
        captured.append(manager.start)
        raise StopAtFetch
    manager = SimpleNamespace(load_markets=load_markets, has_coin=lambda c: True,
        get_first_timestamp=get_first_timestamp, get_ohlcvs=get_ohlcvs)
    manager.update_date_range = lambda start: setattr(manager, 'start', start)
    env.update(asyncio=asyncio, get_first_timestamps_unified=first_timestamps,
        require_live_value=lambda c, k: c['live'][k], compute_per_coin_warmup_minutes=lambda c: {},
        _has_stock_perp_calendar_context=lambda path: True, _pct_log=lambda *a: None,
        HlcvsDataIntegrityError=ValueError, utc_ms=lambda: START + 365 * 1440 * MINUTE,
        ProgressTracker=lambda *a: SimpleNamespace(maybe_log=lambda **kw: None, pct=lambda: 0,
            update=lambda: None, log_done=lambda: None))
    if expected is None:
        with pytest.raises(ValueError, match='No valid coins'):
            asyncio.run(env['prepare_hlcvs_internal'](config, ['ETH'], 'bybit', START, START, START + 4320 * MINUTE, manager))
        assert not captured
    else:
        with pytest.raises(RuntimeError, match='get_ohlcvs failed'):
            asyncio.run(env['prepare_hlcvs_internal'](config, ['ETH'], 'bybit', START, START, START + 4320 * MINUTE, manager))
        assert captured == [expected]
    root = preparation.root / 'age-parity'
    paths = []
    first_minute = (expected - START) // MINUTE if expected is not None else 0
    for day_index in range(4):
        ts = START + np.arange(max(day_index * 1440, first_minute), min((day_index + 1) * 1440, 4321)) * MINUTE
        paths.append(candles(root / f'bybit/1m/ETH_USDT:USDT/2026-01-0{day_index + 1}.npz', ts))
    inception = {'files': {CACHE_FILES[0]: {'ETH': START},
        CACHE_FILES[1]: {'ETH': {'bybit': START + listing_offset * MINUTE}}}}
    requests = [dict(label='bybit', exchanges=['bybit'], coins=['ETH'], start_ts=START,
        end_ts=START + 4320 * MINUTE, tolerance=120, minimum_coin_age_days=age_days)]
    args = ([(p, p.relative_to(root)) for p in paths], requests, {'bybit': {'ETH': 'ETH/USDT:USDT'}}, inception, root / 'checksums.sqlite')
    if expected is None:
        with pytest.raises(VastError, match='no usable selected'):
            integrity.validate_export(*args)
    else:
        integrity.validate_export(*args)


def test_all_coins_too_young_fail_before_archive(preparation):
    """A complete BTC reference cannot certify an empty eligible optimizer payload."""
    p = preparation
    p.config['live']['minimum_coin_age_days'] = 2
    with pytest.raises(VastError, match='no usable selected OHLCV markets'):
        p.run()
    row = p.store.list()[0]
    assert row['status'] == 'failed' and row['rental_state'] == 'none'
    assert not (p.store.directory(row['id']) / 'input.tar.gz').exists()


@pytest.mark.parametrize('suffix,finite', [('.npz', False), ('.npy', False), ('.npz', True), ('.npy', True)])
def test_original_nonfinite_payload_parity(tmp_path, suffix, finite):
    """Reject only the original empty-finite-payload condition, not partial NaN days."""
    root = tmp_path / 'source'
    path = candles(root / f'bybit/1m/BTC_USDT:USDT/2026-01-01{suffix}', START + np.arange(181) * MINUTE)
    if suffix == '.npz':
        with np.load(path) as data:
            arr = data['candles'].copy()
        for name in ('h', 'l', 'c', 'bv'):
            arr[name] = np.nan
            if finite:
                arr[name][0] = 1
        np.savez_compressed(path, candles=arr)
    else:
        arr = np.load(path)
        arr[:, 2:] = np.nan
        if finite:
            arr[0, 2:] = 1
        np.save(path, arr)
    original = tmp_path / 'original'
    shutil.copytree(root, original)
    manager = source_manager(original, START, START + 180 * MINUTE, 120, START)
    async def load_markets():
        """No-op for an already supplied synthetic market."""
    async def first_timestamps(*args, **kwargs):
        """Return the identical authoritative fixture metadata."""
        return {'BTC': START}
    manager.load_markets = load_markets
    manager.update_date_range = lambda start: setattr(manager, 'start_ts', start)
    manager.get_market_specific_settings = lambda coin: {}
    env = namespace()
    env.update(asyncio=asyncio, get_first_timestamps_unified=first_timestamps,
        require_live_value=lambda c, k: 0, compute_per_coin_warmup_minutes=lambda c: {},
        _has_stock_perp_calendar_context=lambda path: True, _pct_log=lambda *a: None,
        HlcvsDataIntegrityError=ValueError,
        ProgressTracker=lambda *a: SimpleNamespace(maybe_log=lambda **kw: None, pct=lambda: 0,
            update=lambda: None, log_done=lambda: None))
    operation = env['prepare_hlcvs_internal']({'backtest': {'ohlcv_source_dir': 'isolated'}},
        ['BTC'], 'bybit', START, START, START + 180 * MINUTE, manager)
    if finite:
        assert asyncio.run(operation)[0]
        export(root, [path])
    else:
        with pytest.raises(ValueError, match='No valid coins'):
            asyncio.run(operation)
        with pytest.raises(VastError, match='no usable selected'):
            export(root, [path])


def original_payload(root, coins, start=START, end=START + 180 * MINUTE):
    """Run both unchanged original loader layers with synthetic metadata and no I/O."""
    manager = source_manager(root, start, end, 120, START)
    async def load_markets():
        """Markets are already supplied by the isolated fixture."""
    async def first_timestamps(*args, **kwargs):
        """Return identical exchange-specific timestamps for each synthetic coin."""
        return {coin: START for coin in coins}
    manager.load_markets = load_markets
    manager.update_date_range = lambda start: setattr(manager, 'start_ts', start)
    manager.get_market_specific_settings = lambda coin: {}
    env = namespace()
    env.update(asyncio=asyncio, get_first_timestamps_unified=first_timestamps,
        require_live_value=lambda c, k: 0, compute_per_coin_warmup_minutes=lambda c: {},
        _has_stock_perp_calendar_context=lambda path: True, _pct_log=lambda *a: None,
        HlcvsDataIntegrityError=ValueError,
        ProgressTracker=lambda *a: SimpleNamespace(maybe_log=lambda **kw: None, pct=lambda: 0,
            update=lambda: None, log_done=lambda: None))
    return asyncio.run(env['prepare_hlcvs_internal']({'backtest': {'ohlcv_source_dir': 'isolated'}},
        coins, 'bybit', start, start, end, manager))


def test_original_mixed_finite_and_skipped_coin(tmp_path):
    """An unusable coin is skipped when another requested coin supplies a payload."""
    root = tmp_path / 'source'
    paths = [candles(root / f'bybit/1m/{coin}_USDT:USDT/2026-01-01.npz', START + np.arange(181) * MINUTE)
             for coin in ['BTC', 'ETH']]
    with np.load(paths[1]) as data:
        arr = data['candles'].copy()
    arr['h'] = np.nan
    np.savez_compressed(paths[1], candles=arr)
    original = tmp_path / 'original'
    shutil.copytree(root, original)
    assert set(original_payload(original, ['BTC', 'ETH'])[0]) == {'BTC'}
    markets = {'bybit': {coin: coin + '/USDT:USDT' for coin in ['BTC', 'ETH']}}
    inception = {'files': {CACHE_FILES[1]: {coin: {'bybit': START} for coin in ['BTC', 'ETH']}}}
    requests = [dict(label='bybit', exchanges=['bybit'], coins=['BTC', 'ETH'], start_ts=START,
        end_ts=START + 180 * MINUTE, tolerance=120)]
    integrity.validate_export([(p, p.relative_to(root)) for p in paths], requests, markets, inception, root / 'checksums.sqlite')


def test_original_cross_file_duplicate_price_precedence(tmp_path):
    """Later finite duplicate rows cannot revive the original all-NaN first source."""
    root = tmp_path / 'source'
    paths = [candles(root / f'bybit/1m/BTC_USDT:USDT/2026-01-0{day}.npz', START + np.arange(1441) * MINUTE)
             for day in [1, 2]]
    with np.load(paths[0]) as data:
        arr = data['candles'].copy()
    arr['h'] = np.nan
    np.savez_compressed(paths[0], candles=arr)
    original = tmp_path / 'original'
    shutil.copytree(root, original)
    with pytest.raises(ValueError, match='No valid coins'):
        original_payload(original, ['BTC'], end=START + 1440 * MINUTE)
    with pytest.raises(VastError, match='no usable selected'):
        export(root, paths, end=START + 1440 * MINUTE)
