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
    assert allocated_cpu_workers(quota) == expected


@pytest.mark.parametrize('quota', [None, 0, -1, float('nan'), float('inf'), True])
def test_invalid_allocations_do_not_launch(quota):
    """Unknown quotas fail closed before the input is uploaded."""
    from vast_transfer import allocated_cpu_workers
    from vast_provider import VastError
    with pytest.raises(VastError):
        allocated_cpu_workers(quota)
