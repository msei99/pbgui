"""Offline automatic CPU execution copies and portable local backtest configs."""

import json

import pytest

from vast_jobs import JobStore, write_json, digest
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
    config = {'optimize': {'n_cpus': 4, 'gpu': {'exact_workers': 4}, 'iters': 200000}, 'bot': {'unchanged': True}}
    write_json(source / 'optimize.json', config)
    write_json(source / 'manifest.json', {'config_sha256': digest(source / 'optimize.json'), 'files': [], 'source_config_sha256': 'original'})
    write_json(directory / 'state.json', {'id':identifier, 'auto_cpu_workers': True, 'cpu_allocation_resolved': True, 'workers': 9})
    before = (source / 'optimize.json').read_bytes()
    target = execution_input(store, identifier)
    actual = json.loads((target / 'optimize.json').read_text())
    assert actual['optimize']['n_cpus'] == actual['optimize']['gpu']['exact_workers'] == 9
    assert actual['optimize']['iters'] == 200000
    assert actual['bot'] == config['bot']
    assert (source / 'optimize.json').read_bytes() == before
    manifest = json.loads((target / 'manifest.json').read_text())
    assert manifest['config_sha256'] == digest(target / 'optimize.json')
    assert manifest['source_config_sha256'] == 'original'
    assert execution_input(store, identifier) == target


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
    from vast_provider import VastError
    monkeypatch.setattr(runner, '_log', lambda *a, **kw: None)
    monkeypatch.setattr(vast_jobs, '_log', lambda *a, **kw: None)
    store = JobStore(tmp_path)
    identifier = 'a' * 32
    directory = tmp_path / 'jobs' / identifier
    directory.mkdir(parents=True)
    bundle = directory / 'input.tar.gz'; bundle.write_bytes(b'isolated-test')
    deadline = time.time() + 3600
    write_json(directory / 'state.json', {'id': identifier, 'status': 'provisioning',
        'rental_state': 'active', 'auto_cpu_workers': True, 'workers': 4})
    write_json(directory / 'control.json', {'stop': False, 'cleanup': False})
    write_json(directory / 'intent.json', {'deadline': deadline, 'offer': {'cpu_cores': 21.3333},
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
            return {'cpu_cores': 256, 'revision': runner.REVISION,
                'guard': {'instance_id': 123, 'job_id': identifier, 'deadline': deadline}}
        def upload(self, **kwargs):
            """Record the resolved count and stop before sending data."""
            checked.append(store.read(identifier)['workers'])
            raise VastError('End isolated test', 422)
    monkeypatch.setattr(runner, 'WorkerConnection', Connection)
    runner.run_loop(store, identifier)
    assert checked == [21]
    assert store.read(identifier)['cpu_allocation_resolved'] is True
