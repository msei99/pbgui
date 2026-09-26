"""Offline automatic CPU execution copies and portable local backtest configs."""

import json

import pytest

from vast_jobs import JobStore, REVISION, write_json, digest
from vast_transfer import execution_input


def test_cpu_execution_copy_preserves_original_and_manifest(tmp_path, monkeypatch):
    """A measured worker count changes only the verified execution configuration."""
    import pb8_config
    monkeypatch.setattr(pb8_config, 'load_pb8_config', lambda p: json.loads(p.read_text()))
    store = JobStore(tmp_path)
    identifier = 'a'*32
    directory = store.root / 'jobs' / identifier
    source = directory / 'input'
    source.mkdir(parents=True)
    old_identifier = 'b'*32
    config = {'optimize': {'n_cpus': 4, 'gpu': {'exact_workers': 4}, 'iters': 200000},
              'backtest': {'ohlcv_source_dir': '/work/pbgui/jobs/' + old_identifier + '/input/ohlcv'},
              'bot': {'unchanged': True}}
    write_json(source / 'optimize.json', config)
    files = [{'path': 'ohlcv/binance/1m/BTC_USDT/2026-01-01.npz', 'bytes': 12, 'sha256': '1' * 64}]
    write_json(source / 'manifest.json', {'config_sha256': digest(source / 'optimize.json'), 'files': files, 'source_config_sha256': 'original'})
    write_json(directory / 'state.json', {'id':identifier, 'auto_cpu_workers': True, 'cpu_allocation_resolved': True,
                                          'workers': 9, 'hardware': {'gpu': 'Test GPU', 'vram_bytes': 12 * 1024**3}})
    before = (source / 'optimize.json').read_bytes()
    target = execution_input(store, identifier)
    actual = json.loads((target / 'optimize.json').read_text())
    assert actual['optimize']['n_cpus'] == actual['optimize']['gpu']['exact_workers'] == 9
    assert actual['optimize']['gpu']['population_size'] == 8192
    assert actual['optimize']['gpu']['batch_size'] == 8192
    assert actual['backtest']['ohlcv_source_dir'] == '/work/pbgui/jobs/' + identifier + '/input/ohlcv'
    assert actual['optimize']['iters'] == 200000
    assert actual['bot'] == config['bot']
    assert (source / 'optimize.json').read_bytes() == before
    manifest = json.loads((target / 'manifest.json').read_text())
    assert manifest['config_sha256'] == digest(target / 'optimize.json')
    assert manifest['source_config_sha256'] == 'original'
    assert manifest['gpu_tuning']['automatic']['max_dispatch_candidate_bars'] == 1_000_000_000
    assert store.read(identifier)['gpu_tuning']['gpu_name'] == 'Test GPU'
    assert execution_input(store, identifier) == target
    stale = json.loads((target / 'optimize.json').read_text())
    stale['backtest']['ohlcv_source_dir'] = '/work/pbgui/jobs/' + old_identifier + '/input/ohlcv'
    write_json(target / 'optimize.json', stale)
    stale_manifest = json.loads((target / 'manifest.json').read_text())
    stale_manifest['config_sha256'] = digest(target / 'optimize.json')
    write_json(target / 'manifest.json', stale_manifest)
    assert execution_input(store, identifier) == target
    repaired = json.loads((target / 'optimize.json').read_text())
    assert repaired['backtest']['ohlcv_source_dir'] == '/work/pbgui/jobs/' + identifier + '/input/ohlcv'
    first_profile = manifest['execution_gpu_profile']
    store.update(identifier, hardware={'gpu': 'Replacement GPU', 'vram_bytes': 24 * 1024**3})
    assert execution_input(store, identifier) == target
    replacement = json.loads((target / 'manifest.json').read_text())
    replacement_config = json.loads((target / 'optimize.json').read_text())
    assert replacement['execution_gpu_profile'] != first_profile
    assert replacement_config['optimize']['gpu']['max_dispatch_candidate_bars'] == 2_000_000_000
    assert (source / 'optimize.json').read_bytes() == before
    rental = 'c' * 32
    rental_dir = store.root / 'jobs' / rental
    rental_dir.mkdir()
    selected = {'population_size': 6656, 'batch_size': 6656,
                'max_dispatch_candidate_bars': 130_170_880_000}
    write_json(rental_dir / 'intent.json', {'offer': {'gpu_name': 'Replacement GPU', 'vram_gb': 24},
                                            'rental_job_gpu_profiles': {identifier: selected}})
    store.update(identifier, lease_id=rental)
    assert execution_input(store, identifier) == target
    chosen = json.loads((target / 'optimize.json').read_text())['optimize']['gpu']
    assert {key: chosen[key] for key in selected} == selected
    assert (source / 'optimize.json').read_bytes() == before


@pytest.mark.parametrize('path,cleared', [('/work/pbgui/jobs/'+'a'*32+'/input/dataset', True),
    ('/home/user/dataset', False), ('/work/custom/input/dataset', False)])
def test_local_backtest_removes_only_container_dataset(path, cleared):
    """Cloud result reuse must not erase ordinary custom data overrides."""
    from api.backtest_v8 import _clear_cloud_dataset_paths
    config = {'backtest': {'hlcvs_data_dir': path, 'ohlcv_source_dir': '/local/ohlcv', 'start_date':'2020-01-01'}}
    assert _clear_cloud_dataset_paths(config) is cleared
    assert ('hlcvs_data_dir' not in config['backtest']) is cleared
    assert config['backtest']['ohlcv_source_dir'] == '/local/ohlcv'
    assert config['backtest']['start_date'] == '2020-01-01'


@pytest.mark.parametrize('quota,expected', [(9.6, 9), (10, 10), (0.5, 1), (64, 64)])
def test_automatic_worker_count_floors_effective_quota(quota, expected):
    """The measured allocation, rather than advertised cores, sets parallelism."""
    from vast_transfer import allocated_cpu_workers
    assert allocated_cpu_workers(quota, quota) == expected


@pytest.mark.parametrize('quota', [None, 0, -1, float('nan'), float('inf'), True])
def test_invalid_allocations_do_not_launch(quota):
    """Unknown quotas fail closed before the input is uploaded."""
    from vast_transfer import allocated_cpu_workers
    from vast_provider import VastError
    with pytest.raises(VastError):
        allocated_cpu_workers(quota, 21.3)


@pytest.mark.parametrize('measured,rented,expected', [(256, 21.3333, 21), (8, 21.3333, 8), (256, 10, 10), (1, .5, 1)])
def test_worker_count_respects_both_allocations(measured, rented, expected):
    """Host-wide CPU discovery never overrides the smaller paid allocation."""
    from vast_transfer import allocated_cpu_workers
    assert allocated_cpu_workers(measured, rented) == expected


@pytest.mark.parametrize('rented', [None, 0, -1, float('nan'), float('inf'), True])
def test_unknown_rented_allocation_fails_closed(rented):
    """Missing or invalid offer capacity cannot enable all host CPUs."""
    from vast_transfer import allocated_cpu_workers
    from vast_provider import VastError
    with pytest.raises(VastError, match='rented CPU'):
        allocated_cpu_workers(256, rented)


def test_runner_caps_workers_before_upload(tmp_path, monkeypatch):
    """The actual runner uses immutable rental capacity before making execution input."""
    import time
    import vast_job_runner as runner
    import vast_jobs
    import vast_provisioning_log
    from vast_provider import VastError
    import vast_runtime_metrics
    monkeypatch.setattr(runner, '_log', lambda *a, **kw: None)
    monkeypatch.setattr(vast_jobs, '_log', lambda *a, **kw: None)
    monkeypatch.setattr(vast_provisioning_log, 'collect_provisioning_log', lambda *a, **kw: None)
    monkeypatch.setattr(vast_runtime_metrics, 'sample_metrics', lambda *a: None)
    store = JobStore(tmp_path)
    identifier = 'a' * 32
    directory = tmp_path / 'jobs' / identifier
    directory.mkdir(parents=True)
    bundle = directory / 'input.tar.gz'; bundle.write_bytes(b'isolated-test')
    deadline = time.time() + 3600
    write_json(directory / 'state.json', {'id': identifier, 'status': 'provisioning',
        'rental_state': 'active', 'auto_cpu_workers': True, 'workers': 4})
    write_json(directory / 'control.json', {'stop': False, 'cleanup': False})
    write_json(directory / 'intent.json', {'deadline': deadline, 'pb8_revision': REVISION,
        'offer': {'cpu_cores': 21.3333},
        'bundle_sha256': digest(bundle)})
    monkeypatch.setattr(runner, 'validate_intent', lambda value, name: value)
    monkeypatch.setattr(runner, 'owned_instance', lambda *a: {'id': 123, 'actual_status': 'running'})
    monkeypatch.setattr(runner, 'VastClient', lambda *a: object())
    monkeypatch.setattr(runner, 'VastCredentialStore', lambda *a: type('Credentials', (), {'secrets': lambda self: {'api_key': 'fake'}})())
    checked = []
    class Connection:
        """Stop at the upload boundary without opening any connection."""
        def __init__(self, *a, **kw):
            """Accept runner connection arguments without opening SSH."""
            pass
        def bootstrap(self, client):
            """Skip provider operations for this isolated worker."""
            pass
        def operation(self, action, **kwargs):
            """Model a container exposing all host CPUs."""
            return {'cpu_cores': 256, 'revision': REVISION,
                'guard': {'instance_id': 123, 'job_id': identifier, 'deadline': deadline}}
        def upload(self, **kwargs):
            """Record the resolved count and stop before sending data."""
            checked.append(store.read(identifier)['workers'])
            raise VastError('End isolated test', 422)
    monkeypatch.setattr(runner, 'WorkerConnection', Connection)
    runner.run_loop(store, identifier)
    assert checked == [21]
    assert store.read(identifier)['cpu_allocation_resolved'] is True
