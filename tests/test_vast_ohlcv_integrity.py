"""Offline regressions for candle validation before Vast packaging and rental."""
import copy
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pytest

import market_data_integrity as integrity
import vast_ohlcv_integrity as preflight
from setup.vast_gpu_benchmark.prepare import select_shards
from vast_direction_fixtures import direction_config
from vast_jobs import JobStore, digest, write_json
from vast_provider import VastError
from vast_queue import CloudQueue

DTYPE = np.dtype([('ts', 'i8'), ('o', 'f4'), ('h', 'f4'), ('l', 'f4'), ('c', 'f4'), ('bv', 'f4')])


def write_day(path, indices=range(1440), *, legacy=False):
    """Write deterministic real candles, optionally as legacy six-column NPY."""
    start = int(datetime.fromisoformat(path.stem).replace(tzinfo=timezone.utc).timestamp() * 1000)
    rows = np.empty(len(indices), dtype=DTYPE)
    rows['ts'] = [start + minute * 60_000 for minute in indices]
    for field, value in [('o', 10), ('h', 11), ('l', 9), ('c', 10), ('bv', 1)]:
        rows[field] = value
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix == '.npy':
        np.save(path, np.column_stack([rows[field] for field in DTYPE.names]) if legacy else rows)
    else:
        np.savez_compressed(path, candles=rows)
    return path


@pytest.fixture
def dataset(tmp_path, monkeypatch):
    """Two exchanges, two coins and a warmup day; no local runtime dependency."""
    config = direction_config()
    config['live']['approved_coins']['long'] = ['BTC', 'ETH']
    config['backtest'].update(start_date='2023-01-06', end_date='2023-01-07', exchanges=['binance', 'bybit'])
    root, mappings = tmp_path / 'ohlcv', tmp_path / 'coindata'
    for exchange in ('binance', 'bybit'):
        mapping = mappings / exchange / 'mapping.json'
        mapping.parent.mkdir(parents=True)
        mapping.write_text(json.dumps([dict(coin=coin, quote='USDT', swap=True, linear=True,
                                           ccxt_symbol=coin + '/USDT:USDT') for coin in ('BTC', 'ETH')]))
        for coin in ('BTC', 'ETH'):
            folder = root / ('binanceusdm' if exchange == 'binance' else exchange) / '1m' / (coin + '_USDT:USDT')
            for day in ('2023-01-05', '2023-01-06', '2023-01-07'):
                write_day(folder / (day + '.npz'))
    monkeypatch.setattr('pb8_config._call_helper', lambda operation, **payload: {'minutes': [60] * len(payload['configs'])})
    monkeypatch.setattr('vast_jobs.native_job_config', lambda source, *_args: copy.deepcopy(source))
    monkeypatch.setattr('pb8_config.save_prepared_pb8_config', lambda config, path: write_json(path, config))
    monkeypatch.setattr('sweep_cycles.build_sweep_plan', lambda source: None)
    monkeypatch.setattr('scenario_windows.build_validation_plan', lambda source: None)
    return config, root, mappings


def check(dataset):
    """Validate precisely the shards selected by production export logic."""
    config, root, mappings = dataset
    return preflight.preflight_shards(config, select_shards(config, root, mappings), root, mappings)


def test_healthy_data_passes_and_publishes_verified_bundle(dataset, tmp_path):
    """A ready job includes a validation proof covering both venues and warmup."""
    config, root, mappings = dataset
    store = JobStore(tmp_path / 'vast')
    row = store.prepare('healthy', config, 'source', root, mappings, tmp_path / 'results', 512, 4, False)
    assert row['status'] == 'ready' and row['rental_state'] == 'none'
    assert row['exchanges'] == ['binance', 'bybit']
    manifest = store.read(row['id'], 'input/manifest.json')
    assert len(manifest['files']) == 12
    assert any('2023-01-05' in item['path'] for item in manifest['files'])
    preflight.require_integrity_preflight(manifest)


def test_incomplete_day_blocks_with_exact_message_before_packaging(dataset, tmp_path):
    """The observed 1000-minute internal hole is named, not reported as missing candles."""
    config, root, mappings = dataset
    write_day(root / 'bybit/1m/BTC_USDT:USDT/2023-01-06.npz', [*range(200), *range(1200, 1440)])
    store = JobStore(tmp_path / 'vast')
    row = store.create_preparation('bad', 512, 4, False)
    expected = ('OHLCV integrity check failed: bybit/BTC 2023-01-06 (1000 missing minutes)'
                ' — repair data before renting')
    with pytest.raises(VastError) as error:
        store.prepare('bad', config, 'source', root, mappings, tmp_path / 'results', 512, 4, False, identifier=row['id'])
    assert str(error.value) == expected and error.value.status == 422
    state = store.read(row['id'])
    assert state['status'] == 'failed' and state['error'] == expected and state['rental_state'] == 'none'
    assert not (store.directory(row['id']) / 'input').exists()
    assert not CloudQueue(store).waiting()


@pytest.mark.parametrize('exchange,coin,day', [('binanceusdm', 'ETH', '2023-01-05'),
                                               ('bybit', 'BTC', '2023-01-06'),
                                               ('bybit', 'ETH', '2023-01-07')])
def test_missing_whole_day_including_edges_and_warmup_blocks(dataset, exchange, coin, day):
    """Existing neighboring days must not hide absent leading, internal or ending data."""
    _, root, _ = dataset
    (root / exchange / '1m' / (coin + '_USDT:USDT') / (day + '.npz')).unlink()
    with pytest.raises(VastError, match=coin + ' ' + day + r' \(1440 missing minutes\)'):
        check(dataset)


def test_reports_all_affected_markets_and_days(dataset):
    """One failure lists all repairs, including non-BTC coins on both exchanges."""
    _, root, _ = dataset
    for exchange, coin, day in [('binanceusdm', 'ETH', '2023-01-05'), ('bybit', 'BTC', '2023-01-06'),
                                ('bybit', 'ETH', '2023-01-07')]:
        write_day(root / exchange / '1m' / (coin + '_USDT:USDT') / (day + '.npz'), range(144))
    with pytest.raises(VastError) as error:
        check(dataset)
    assert str(error.value) == ('OHLCV integrity check failed: '
                               'binance/ETH 2023-01-05 (1296 missing minutes), '
                               'bybit/BTC 2023-01-06 (1296 missing minutes), '
                               'bybit/ETH 2023-01-07 (1296 missing minutes) — repair data before renting')


def test_healthy_cached_status_never_opens_candle_arrays(dataset, monkeypatch):
    """Both an existing Market Data scan and repeated preparation reuse signatures."""
    _, root, _ = dataset
    for path in root.glob('*/1m/*/*.npz'):
        integrity.record_daily_file(exchange=path.parts[-4], coin=path.parent.name, day=path.stem,
                                    path=path, db_path=root / 'checksums.sqlite')
    monkeypatch.setattr(integrity, 'validate_daily_npz', lambda *args, **kwargs: pytest.fail('Unchanged candle file was scanned'))
    assert len(check(dataset)) == 12
    assert len(check(dataset)) == 12


def test_bounded_cold_scan_is_cached_and_mtime_change_invalidates_it(dataset, monkeypatch):
    """Fallback reads only selected days; repairs and regressions invalidate cached rows."""
    _, root, _ = dataset
    outside = write_day(root / 'bybit/1m/BTC_USDT:USDT/2022-01-01.npz', range(1))
    validate = integrity.validate_daily_npz
    seen = []

    def recording(path, *args, **kwargs):
        """Count real day reads instead of relying on a timing-sensitive performance test."""
        seen.append(path)
        return validate(path, *args, **kwargs)

    monkeypatch.setattr(integrity, 'validate_daily_npz', recording)
    check(dataset)
    assert len(seen) == 12 and outside not in seen
    seen.clear()
    check(dataset)
    assert seen == []
    path = root / 'bybit/1m/ETH_USDT:USDT/2023-01-06.npz'
    write_day(path, [*range(200), *range(1200, 1440)])
    with pytest.raises(VastError, match='1000 missing minutes'):
        check(dataset)
    assert seen == [path]
    seen.clear()
    write_day(path)
    assert len(check(dataset)) == 12 and seen == [path]


@pytest.mark.parametrize('legacy', [False, True])
def test_legacy_npy_uses_same_validation_and_cache(dataset, legacy):
    """Both structured NPY and the PB7 six-column format remain exportable."""
    _, root, _ = dataset
    path = root / 'bybit/1m/BTC_USDT:USDT/2023-01-06.npz'
    path.unlink()
    write_day(path.with_suffix('.npy'), legacy=legacy)
    assert len(check(dataset)) == 12
    assert len(check(dataset)) == 12


def test_changed_after_preflight_never_becomes_ready(dataset, tmp_path, monkeypatch):
    """A concurrent repair between validation and copying cannot bypass the proof."""
    config, root, mappings = dataset
    path = root / 'bybit/1m/BTC_USDT:USDT/2023-01-06.npz'

    def save_and_change(config, target):
        """Simulate a writer changing the source immediately before packaging."""
        write_json(target, config)
        write_day(path, range(440))

    monkeypatch.setattr('pb8_config.save_prepared_pb8_config', save_and_change)
    store = JobStore(tmp_path / 'vast')
    with pytest.raises(VastError, match='changed after integrity preflight'):
        store.prepare('race', config, 'source', root, mappings, tmp_path / 'results', 512, 4, False)
    assert store.list()[0]['status'] == 'failed'
    assert store.list()[0]['rental_state'] == 'none'


@pytest.mark.parametrize('pool', [False, True])
def test_unchecked_legacy_job_blocks_before_manual_or_pool_rental(tmp_path, monkeypatch, pool):
    """Jobs queued before this fix cannot rent until their data has been checked."""
    store = JobStore(tmp_path / 'vast')
    row = store.create_preparation('legacy', 512, 4, False)
    folder = store.directory(row['id']) / 'input'
    folder.mkdir()
    write_json(folder / 'optimize.json', direction_config())
    write_json(folder / 'manifest.json', {'config_sha256': digest(folder / 'optimize.json'), 'files': []})
    store.update(row['id'], status='ready')
    queue = CloudQueue(store)
    monkeypatch.setattr(store, 'start', lambda *args, **kwargs: pytest.fail('Rental attempted before integrity check'))
    monkeypatch.setattr('vast_market_cache._load_local_markets', lambda *args: pytest.fail('Metadata work ran before integrity check'))
    offer = {'id': 42, 'machine_id': 7}
    if pool:
        import vast_pool
        queue.update(pool_authorization={'id': 'a' * 32, 'settings': {'hours': 1, 'budget': 1, 'idle_seconds': 300}})
        monkeypatch.setattr(vast_pool, 'rental_needed', lambda *args: True)
        monkeypatch.setattr(vast_pool, 'select_offer', lambda *args: offer)
        start = lambda: vast_pool.pool_step(queue)
    else:
        start = lambda: queue.start(offer, 1, 1, 300)
    with pytest.raises(VastError, match='prepare the job again before renting'):
        start()
    assert not queue.workers()


@pytest.mark.parametrize('mutation', ['missing', 'file', 'config'])
def test_proof_cannot_survive_missing_or_changed_manifest(mutation):
    """The persisted proof binds both frozen config identity and every shard hash."""
    manifest = {'config_sha256': 'config', 'files': [{'path': 'day.npz', 'sha256': 'before'}]}
    preflight.certify_manifest(manifest)
    if mutation == 'missing':
        del manifest['ohlcv_integrity']
    elif mutation == 'file':
        manifest['files'][0]['sha256'] = 'after'
    else:
        manifest['config_sha256'] = 'changed'
    with pytest.raises(VastError, match='before renting'):
        preflight.require_integrity_preflight(manifest)


def test_verified_listing_metadata_excludes_pre_inception_days(dataset, monkeypatch):
    """A genuinely newly listed market is healthy without impossible pre-listing history."""
    _, root, _ = dataset
    (root / 'bybit/1m/ETH_USDT:USDT/2023-01-05.npz').unlink()
    path = root / 'bybit/1m/ETH_USDT:USDT/2023-01-06.npz'
    write_day(path, range(600, 1440))
    timestamp = int(datetime(2023, 1, 6, 10, tzinfo=timezone.utc).timestamp() * 1000)
    import vast_inception
    monkeypatch.setattr(vast_inception, 'load_local_inception', lambda markets: {
        'files': {vast_inception.CACHE_FILES[1]: {'ETH': {'bybit': timestamp}}}})
    assert len(check(dataset)) == 11
    write_day(path, range(700, 1440))
    with pytest.raises(VastError, match=r'bybit/ETH 2023-01-06 \(100 missing minutes\)'):
        check(dataset)


def test_background_preparation_keeps_actionable_error(tmp_path, monkeypatch):
    """The asynchronous API wrapper must not overwrite the detailed failure message."""
    from api import vast
    store = JobStore(tmp_path / 'vast')
    row = store.create_preparation('bad', 512, 4, False)
    message = 'OHLCV integrity check failed: bybit/BTC 2023-01-06 (1000 missing minutes) — repair data before renting'

    def fail(*args, **kwargs):
        """Stop preparation before any input or rental is created."""
        raise VastError(message, 422)

    monkeypatch.setattr(store, 'prepare', fail)
    monkeypatch.setattr('api.optimize_v8._results_root', lambda: tmp_path / 'results')
    monkeypatch.setattr(vast, '_PREPARATION_STOPPING', False)
    vast._run_preparation(store, row['id'], 'bad', {}, 'source', 512, 4, False)
    assert store.read(row['id'])['error'] == message
    assert store.read(row['id'])['status'] == 'failed'


def test_current_day_ignores_future_minutes_but_blocks_stale_tail(dataset, monkeypatch):
    """End=now requires the closed minutes, not future candles or an arbitrarily old prefix."""
    from datetime import timedelta
    config, root, _ = dataset
    stamp = datetime.now(timezone.utc).replace(hour=12, minute=0, second=0, microsecond=0)

    class Clock(datetime):
        """Freeze the validation boundary without touching global wall-clock state."""

        @classmethod
        def now(cls, tz=None):
            """Return the deterministic current-day cutoff."""
            return stamp

    monkeypatch.setattr(preflight, 'datetime', Clock)
    config['backtest'].update(start_date=stamp.date().isoformat(), end_date='now')
    for exchange in ('binanceusdm', 'bybit'):
        for coin in ('BTC', 'ETH'):
            folder = root / exchange / '1m' / (coin + '_USDT:USDT')
            write_day(folder / ((stamp.date() - timedelta(days=1)).isoformat() + '.npz'))
            write_day(folder / (stamp.date().isoformat() + '.npz'), range(720))
    assert len(check(dataset)) == 8
    write_day(root / 'bybit/1m/ETH_USDT:USDT' / (stamp.date().isoformat() + '.npz'), range(600))
    with pytest.raises(VastError, match=r'bybit/ETH .* \(120 missing minutes\)'):
        check(dataset)


def test_healthy_prepared_input_is_checked_before_rental_without_rescan(dataset, tmp_path, monkeypatch):
    """The real queue rental path checks the frozen proof before starting a lease."""
    config, root, mappings = dataset
    store = JobStore(tmp_path / 'vast')
    row = store.prepare('healthy', config, 'source', root, mappings, tmp_path / 'results', 512, 4, False)
    import vast_inception
    required_markets = vast_inception.required_markets
    monkeypatch.setattr(vast_inception, 'required_markets', lambda manifest, _: required_markets(manifest, mappings))
    events = []
    require = preflight.require_integrity_preflight

    def check_proof(manifest):
        """Record the integrity gate while still exercising its production implementation."""
        require(manifest)
        events.append('integrity')

    def rent(identifier, *args):
        """Stand in for paid provider work only after all real local checks."""
        events.append('rent')
        return store.read(identifier)

    monkeypatch.setattr(preflight, 'require_integrity_preflight', check_proof)
    monkeypatch.setattr(vast_inception, 'ensure_local_inception', lambda markets: events.append('metadata'))
    monkeypatch.setattr(vast_inception, 'load_local_inception', lambda markets: events.append('metadata'))
    monkeypatch.setattr('vast_market_cache._load_local_markets', lambda exchanges: {
        exchange: {'markets': {coin + '/USDT:USDT': {} for coin in ('BTC', 'ETH')}} for exchange in exchanges})
    monkeypatch.setattr(integrity, 'validate_daily_npz', lambda *args, **kwargs: pytest.fail('Rental rescanned immutable input'))
    monkeypatch.setattr(store, 'start', rent)
    CloudQueue(store).start({'id': 42, 'machine_id': 7}, 1, 1, 300)
    assert events == ['integrity', 'metadata', 'integrity', 'metadata', 'rent']
    assert store.read(row['id'])['status'] == 'ready'
