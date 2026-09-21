"""Isolated SQLite history, workload identity, sampling and lifecycle regression tests."""
import copy
import hashlib
import json
from pathlib import Path
import threading
import time

import numpy as np
import pytest

import vast_performance as performance
from pb8_config import save_prepared_pb8_config
from secure_files import ensure_private_directory
from vast_jobs import IMAGE, REVISION, JobStore, write_json


@pytest.fixture
def bundle(tmp_path, monkeypatch):
    """Create actual tiny candle snapshots with a mocked canonical config loader."""
    store = JobStore(tmp_path / 'vast')
    configs = {}
    monkeypatch.setattr(performance, 'load_config', lambda path: copy.deepcopy(configs[str(path)]))
    monkeypatch.setattr("pb8_config.load_pb8_config", performance.load_config)

    def create(character='a', workers=4, seed=7, candles=10):
        """Build a frozen test job and same-dataset-independent rental metadata."""
        identifier = character * 32
        folder = ensure_private_directory(store.root / 'jobs' / identifier)
        input_dir = ensure_private_directory(folder / 'input')
        path = input_dir / 'ohlcv/binance/1m/BTC_USDT:USDT/2026-01-01.npz'
        ensure_private_directory(path.parent)
        np.savez_compressed(path, candles=np.zeros((candles, 6)))
        config = {'pbgui': {'private':'do-not-persist'}, 'live': {'user':'private-user', 'approved_coins':{'long':['BTC'], 'short':[]}},
                  'bot':{'long':{'n_positions':1}},
                  'backtest':{'ohlcv_source_dir':'/work/jobs/' + identifier, 'exchanges':['binance'],
                              'start_date':'2026-01-01', 'end_date':'2026-02-01', 'scenarios':[]},
                  'optimize':{'n_cpus':workers, 'seed':seed, 'iters':512, 'backend':'gpu',
                              'population_size':1024, 'bounds':{'foo':[0,1]}, 'scoring':[{'metric':'adg'}],
                              'gpu':{'exact_workers':workers, 'private_token':'do-not-persist'}}}
        save_prepared_pb8_config(config, input_dir / 'optimize.json')
        configs[str(input_dir / 'optimize.json')] = config
        write_json(input_dir / 'manifest.json', {'pb8_revision':REVISION,
            'config_sha256':hashlib.sha256((input_dir/'optimize.json').read_bytes()).hexdigest(),
            'files':[{'path':str(path.relative_to(input_dir)), 'bytes':path.stat().st_size,
                      'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}]})
        write_json(folder / 'state.json', {'id':identifier, 'status':'running', 'generation':1,
            'config_name':'test', 'created_at':1000, 'setup_started_at':1000, 'workers':workers,
            'cpu_allocation_resolved':True})
        write_json(folder / 'intent.json', {'image':IMAGE, 'deadline':9999999999,
            'offer':{'gpu_name':'Test GPU', 'machine_id':workers, 'cpu_cores':workers, 'price_hour_usd':.5}})
        return store.read(identifier)
    return store, create


def counters(stamp, proxy, exact):
    """Return a complete native observation without estimated population counters."""
    return {'sampled_at':stamp, 'proxy_total':proxy, 'exact_total':exact}


def run(identifier='a', fingerprint='f'*64):
    """Build a public run payload with only allowlisted hardware information."""
    return {'id':identifier*32, 'captured_at':time.time(), 'config_name':'test', 'status':'completed',
            'workload':{'fingerprint':fingerprint}, 'hardware':{'price_hour_usd':.5}, 'created_at':900}


def test_identical_workloads_ignore_host_cpu_and_transient_paths(bundle):
    """Different allocations are comparison axes, while seed and data alter identity."""
    store, create = bundle
    a = performance.workload_metadata(store, create('a', workers=4), IMAGE)
    b = performance.workload_metadata(store, create('b', workers=16), IMAGE)
    assert a['fingerprint'] == b['fingerprint']
    assert a['exported_candles'] == 10 and a['consumed_candles'] is None
    assert a['scenario_count'] == a['coin_count'] == a['parameter_count'] == 1
    assert 'do-not-persist' not in json.dumps(a)
    assert performance.workload_metadata(store, create('c', seed=8), IMAGE)['fingerprint'] != a['fingerprint']
    assert performance.workload_metadata(store, create('d', candles=20), IMAGE)['fingerprint'] != a['fingerprint']
    assert performance.workload_metadata(store, store.read('a'*32), 'other-image')['fingerprint'] != a['fingerprint']


def test_invalid_manifest_cannot_read_outside_job(bundle, tmp_path):
    """Traversal and unknown snapshots cannot be marked as verified workloads."""
    store, create = bundle
    row = create()
    folder = store.directory(row['id'])
    manifest = store.read(row['id'], 'input/manifest.json')
    manifest['files'][0]['path'] = '../../outside.npz'
    write_json(folder / 'input/manifest.json', manifest)
    assert performance.workload_metadata(store, row, IMAGE)['fingerprint'] is None
    (folder / 'input/optimize.json').write_text('tampered')
    assert performance.workload_metadata(store, row, IMAGE)['reason'] == 'Frozen config checksum mismatch'


def test_history_survives_job_removal_and_database_reopen(tmp_path):
    """The history owns copies and does not require surviving queue records."""
    history = performance.PerformanceHistory(tmp_path / 'vast')
    history.record(run(), [counters(1000, 1000, 100), counters(1060, 7000, 160)])
    reopened = performance.PerformanceHistory(tmp_path / 'vast')
    row = reopened.get('a'*32)
    assert row['summary']['proxy_per_minute'] == 6000
    assert row['summary']['exact_per_minute'] == 60
    assert row['summary']['exact_per_usd'] == 7200
    assert reopened.list_runs()['total'] == 1
    assert reopened.series('a'*32)['counter'][-1]['exact_total'] == 160
    assert (history.path.stat().st_mode & 0o777) == 0o600


def test_duplicate_and_stale_observations_do_not_add_work(tmp_path):
    """Per-minute UPSERT keeps newer counters and repeated imports remain idempotent."""
    history = performance.PerformanceHistory(tmp_path)
    samples = [counters(1000, 1000, 100), counters(1060, 7000, 160)]
    history.record(run(), samples)
    history.record(run(), samples)
    history.record(run(), [counters(1050, 10, 1)])
    assert len(history.series('a'*32)['counter']) == 2
    assert history.get('a'*32)['summary']['proxy_per_minute'] == 6000


def test_weighted_average_resets_and_sparse_intervals():
    """Average counts over seconds, not unweighted rate means or across restarts."""
    result = performance.summarize([counters(0, 0, 0), counters(60, 600, 60),
        counters(180, 6600, 180), counters(240, 0, 0), counters(300, 600, 60)])
    assert result['proxy_per_minute'] == 1800  # 7200 across 240 measured seconds.
    assert result['exact_per_minute'] == 60
    assert result['proxy_rate_min'] == 600 and result['proxy_rate_max'] == 3000


def test_concurrent_records_and_workload_pagination(tmp_path):
    """Concurrent collectors retain distinct runs and filter exact workload groups."""
    history = performance.PerformanceHistory(tmp_path)
    errors = []
    def write(character):
        """Record independently and expose any thread failure to pytest."""
        try:
            history.record(run(character), [counters(1000, 1, 1)])
        except Exception as exc:
            errors.append(exc)
    threads = [threading.Thread(target=write, args=(character,)) for character in 'abcdef']
    for thread in threads: thread.start()
    for thread in threads: thread.join()
    assert not errors
    assert history.list_runs(limit=2)['total'] == 6
    assert len(history.list_runs(limit=2, offset=2)['runs']) == 2
    assert history.list_runs(fingerprint='0'*64)['total'] == 0


def test_collector_records_without_browser_and_retains_deleted_runs(bundle, tmp_path, monkeypatch):
    """Scan locally, retain workload metadata, and join promptly on shutdown."""
    store, create = bundle
    row = create()
    logs = tmp_path / 'logs'; logs.mkdir()
    (logs / ('vast_' + row['id'] + '.log')).write_text(
        '2026-09-16T10:00:00Z INFO GPU optimize | gen=1 proxy=100 (1.0/s) exact=10 inflight=0\n'
        '2026-09-16T10:01:00Z INFO GPU optimize | gen=2 proxy=700 (1.0/s) exact=40 inflight=0\n')
    collector = performance.PerformanceCollector(store, logs, interval=60)
    collector.scan()
    retained = collector.history.get(row['id'])
    assert retained['summary']['proxy_per_minute'] == 600
    assert retained['workload']['fingerprint']
    assert collector.history.series(row['id'])['telemetry'][0]['phase'] == 'running'
    store.update(row['id'], status='completed', deleted_at=time.time())
    collector.scan()
    assert collector.history.get(row['id'])['status'] == 'completed'
    called = threading.Event()
    monkeypatch.setattr(collector, 'scan', called.set)
    collector.start(); assert called.wait(2)
    collector.stop(); collector.join(); collector.join()
    assert collector.thread is None


def test_database_rejects_symlinks(tmp_path):
    """A database path cannot redirect history writes outside its owned root."""
    target = tmp_path / 'outside'; target.write_text('untouched')
    root = tmp_path / 'vast'; root.mkdir()
    (root / 'performance.sqlite3').symlink_to(target)
    with pytest.raises(RuntimeError):
        performance.PerformanceHistory(root).list_runs()
    assert target.read_text() == 'untouched'


def test_delayed_collector_cannot_overwrite_newer_terminal_state(tmp_path):
    """Database transactions preserve the newest source generation across collectors."""
    history = performance.PerformanceHistory(tmp_path)
    newest = dict(run(), status='completed', source_generation=20)
    history.record(newest, [counters(1000, 100, 10), counters(1060, 700, 40)])
    stale = dict(run(), status='running', source_generation=10)
    history.record(stale, [counters(1000, 100, 10)])
    assert history.get('a'*32)['status'] == 'completed'
    assert history.get('a'*32)['source_generation'] == 20


@pytest.mark.parametrize('field,value', [('seed',9), ('population_size',2048), ('scoring',[{'metric':'sharpe'}]), ('bounds',{'foo':[0,2]})])
def test_optimizer_complexity_changes_split_comparison_groups(bundle, monkeypatch, field, value):
    """Algorithm workload settings remain part of the fingerprint."""
    store, create = bundle
    row = create()
    before = performance.workload_metadata(store, row, IMAGE)['fingerprint']
    original = performance.load_config
    def modified(path):
        """Simulate a canonical configuration with a different optimizer task."""
        config = original(path); config['optimize'][field] = value
        return config
    monkeypatch.setattr(performance, 'load_config', modified)
    assert performance.workload_metadata(store, row, IMAGE)['fingerprint'] != before


def test_collector_phase_snapshots_do_not_manufacture_utilization(bundle, tmp_path):
    """Provisioning and old metrics remain visible as phases with unknown utilization."""
    store, create = bundle
    row = create()
    store.update(row['id'], status='provisioning', runtime_metrics={'available':True,'sampled_at':1,'gpu_percent':99})
    history = performance.PerformanceHistory(store.root)
    performance.collect_run(store, history, store.read(row['id']), tmp_path / 'missing-logs')
    samples = history.series(row['id'])['telemetry']
    assert samples[0]['phase'] == 'provisioning' and samples[0]['gpu_percent'] is None
    performance.collect_run(store, history, store.read(row['id']), tmp_path / 'missing-logs')
    assert history.series(row['id'])['telemetry'] == samples


def test_collector_retains_applied_gpu_tuning_profile(bundle, tmp_path):
    """History binds measured throughput to the immutable execution profile."""
    store, create = bundle
    row = create()
    tuning = {'schema_version': 1, 'automatic': {'population_size': 8192},
              'fingerprint': 'f' * 64}
    store.update(row['id'], gpu_tuning=tuning)
    history = performance.PerformanceHistory(store.root)
    result = performance.collect_run(
        store, history, store.read(row['id']), tmp_path / 'missing-logs'
    )
    assert result['gpu_tuning'] == tuning
    assert history.get(row['id'])['gpu_tuning'] == tuning


def test_bad_legacy_config_keeps_measurements_but_never_verified_identity(bundle, tmp_path, monkeypatch):
    """A broken config loader must not discard useful historical counters."""
    store, create = bundle
    row = create()
    store.update(row['id'], throughput={'samples':[counters(1000, 0, 0),counters(1060, 600, 60)]})
    def broken(path):
        """Represent an unavailable canonical config loader without running PB8."""
        raise ValueError('invalid legacy config')
    monkeypatch.setattr(performance, 'load_config', broken)
    history = performance.PerformanceHistory(store.root)
    result = performance.collect_run(store, history, store.read(row['id']), tmp_path / 'logs')
    assert result['fingerprint'] is None
    assert result['summary']['proxy_per_minute'] == 600


def test_corrupt_job_does_not_block_other_history_records(bundle, tmp_path):
    """One malformed state file cannot interrupt the collector's whole batch."""
    store, create = bundle
    good = create()
    bad = ensure_private_directory(store.root / 'jobs' / ('f'*32))
    (bad / 'state.json').write_text('{broken')
    collector = performance.PerformanceCollector(store, tmp_path / 'logs')
    collector.scan()
    assert collector.history.get(good['id'])['workload']['fingerprint']


def test_missing_rental_keeps_previously_recorded_workload_and_hardware(bundle, tmp_path):
    """A retained completed job does not depend on keeping its rental intent forever."""
    store, create = bundle
    row = create()
    history = performance.PerformanceHistory(store.root)
    first = performance.collect_run(store, history, row, tmp_path / 'logs')
    (store.directory(row['id']) / 'intent.json').unlink()
    store.update(row['id'], status='completed')
    second = performance.collect_run(store, history, store.read(row['id']), tmp_path / 'logs')
    assert second['fingerprint'] == first['fingerprint']
    assert second['hardware'] == first['hardware']


def test_legacy_history_estimate_uses_retained_snapshot(bundle):
    """Old history gains a display estimate without changing fingerprints or disk records."""
    store, create = bundle
    row = create()
    history = performance.PerformanceHistory(store.root)
    history.record(dict(row, captured_at=1000, workload={'fingerprint':'f'*64}), [])
    loaded = history.get(row['id'])
    assert loaded['workload']['estimated_coin_candles'] == 32 * 1440
    assert loaded['fingerprint'] == 'f'*64
    assert history.list_runs()['runs'][0]['workload']['estimated_coin_candles'] == 32 * 1440
    (store.directory(row['id']) / 'input/optimize.json').unlink()
    assert history.get(row['id'])['workload']['estimated_coin_candles'] is None
