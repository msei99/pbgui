"""Offline native launch coverage; no real configs, downloads or GPU rentals."""
import hashlib
import json
from types import SimpleNamespace

import pytest

from pb8_optimize_runner import _optimizer_argv, _install_evaluation_progress_publisher
from pb9_optimize_options import OPTIONS_FILENAME, cli_arguments, stage_manifest, validate_options
from secure_files import atomic_write_private_text


def manifest():
    """Supply a completed report with the native outer format."""
    return {'name': 'prepared.json', 'text': json.dumps({'schema_version': 1,
        'kind': 'prepared-inputs', 'consumer': 'optimize', 'suite': {},
        'datasets': [{'manifest_present': True, 'manifest_sha256': 'a' * 64}], 'cases': [{}]})}


def test_native_flags_and_manifest_are_frozen_and_passed_without_shell(tmp_path):
    """All four native flags use argv, with exact imported bytes in a private file."""
    options = {'mode': 'pareto_seed', 'source': '/managed/pareto', 'scenarios': ['train', 'holdout'],
               'filter_starting_configs': True, 'compress_starting_configs': 12, 'preparation_manifest': manifest()}
    options.update(validate_options(options, {'backtest': {'scenarios': [{'label': 'train'}, {'label': 'holdout'}]}}))
    path = stage_manifest(options, tmp_path/'preparation.json', atomic_write_private_text)
    assert (tmp_path/'preparation.json').read_text() == options['preparation_manifest']['text']
    assert (tmp_path/'preparation.json').stat().st_mode & 0o777 == 0o600
    assert _optimizer_argv('optimize.py', '/config.json', options, runtime='v9', manifest_path=path) == [
        'optimize.py', '/config.json', '--start', '/managed/pareto', '--scenarios', 'train,holdout',
        '--filter-starting-configs', '--compress-starting-configs', '12', '--preparation-manifest', path]
    (tmp_path/'preparation.json').write_text('{}')
    with pytest.raises(ValueError, match='changed'):
        cli_arguments(options, path)


@pytest.mark.parametrize('options', [
    {'scenarios': ['a,b']}, {'scenarios': ['\n']}, {'scenarios': 'train'},
    {'filter_starting_configs': 1}, {'filter_starting_configs': True},
    {'mode': 'checkpoint_resume', 'compress_starting_configs': 2},
    {'mode': 'pareto_seed', 'compress_starting_configs': True},
    {'mode': 'pareto_seed', 'compress_starting_configs': 0},
    {'mode': 'pareto_seed', 'compress_starting_configs': 1.5},
    {'preparation_manifest': '/arbitrary/host/path'},
    {'preparation_manifest': {'text': '{"kind":"prepared-inputs","kind":"plan"}'}},
    {'preparation_manifest': {'text': '{"kind":"prepared-inputs","consumer":"optimize","bad":1e999}'}},
    {'preparation_manifest': {'text': 'x' * (4 * 1024 * 1024 + 1)}},
    {'preparation_manifest': {**manifest(), 'name': '../file'}},
    {'preparation_manifest': {**manifest(), 'sha256': 'b' * 64}},
])
def test_invalid_native_inputs_fail_closed(options):
    """Malformed selections and imported artifacts cannot reach either runner."""
    with pytest.raises(ValueError):
        validate_options(options)


def test_scenario_must_exist_in_frozen_config():
    """A renamed/deleted scenario cannot silently broaden a job to all cases."""
    with pytest.raises(ValueError, match='missing'):
        validate_options({'scenarios': ['train']}, {'backtest': {'scenarios': []}})


def test_legacy_argv_and_absent_native_options_keep_previous_behaviour():
    """PB8 never receives native options; old PB9 records remain fresh launches."""
    assert _optimizer_argv('cli', '/cfg', {'scenarios': ['train'], 'filter_starting_configs': True}) == ['cli', '/cfg']
    assert _optimizer_argv('cli', '/cfg', {}, runtime='v9') == ['cli', '/cfg']


def test_result_recorder_preserves_native_launch_provenance(tmp_path):
    """Checkpoint Resume can recover a subset and report after its queue is removed."""
    class Recorder:
        """Minimal native recorder with the actual ParetoStore directory contract."""
        def __init__(self):
            """Expose a result directory without opening any real stream."""
            self.store = SimpleNamespace(directory=tmp_path, n_iters=0)
        def record(self, value):
            """Stand in for one native evaluation."""
            return value
    finish = _install_evaluation_progress_publisher(SimpleNamespace(ResultRecorder=Recorder),
        {'scenarios': ['train'], 'preparation_manifest': manifest()})
    try:
        Recorder()
        stored = json.loads((tmp_path/OPTIONS_FILENAME).read_text())
        assert stored['scenarios'] == ['train']
        assert stored['preparation_manifest']['sha256'] == hashlib.sha256(manifest()['text'].encode()).hexdigest()
    finally:
        finish()


def test_cloud_worker_uses_same_native_argv_and_only_packaged_paths(tmp_path, monkeypatch):
    """The independent worker shares all CLI flags without ever seeing a host seed path."""
    from setup.vast_gpu_benchmark import cloud_worker
    monkeypatch.setattr(cloud_worker, 'ROOT', tmp_path)
    monkeypatch.setenv('PBGUI_WORKER_RUNTIME', 'pb9')
    folder = tmp_path/'input'; folder.mkdir(); (tmp_path/'output').mkdir()
    (folder/'starting_configs').mkdir()
    options = {'mode':'pareto_seed', 'source':'starting_configs', 'scenarios':['train'],
        'fine_tune_params':['global.risk'], 'polish_percentage':0.1, 'polish_bounds_mode':'clamp',
        'filter_starting_configs':True, 'compress_starting_configs':5, 'preparation_manifest':manifest()}
    (folder/'launch_options.json').write_text(json.dumps(options))
    command = cloud_worker.optimizer_command()
    local_options = {**options, 'source':str(folder/'starting_configs')}
    assert command[1:] == _optimizer_argv('/opt/passivbot/src/optimize.py', str(folder/'optimize.json'),
        local_options, runtime='v9', manifest_path=str(tmp_path/'output/preparation.json'))
    calibration = tmp_path/'calibration-cases/4096/optimize.json'
    calibrated = cloud_worker.optimizer_command(calibration)
    assert calibrated[2] == str(calibration)
    assert calibrated[3:] == command[3:]
    options['source'] = '/home/user/private/seeds'
    (folder/'launch_options.json').write_text(json.dumps(options))
    with pytest.raises(ValueError, match='packaged'):
        cloud_worker.optimizer_command()
    monkeypatch.setenv('PBGUI_WORKER_RUNTIME', 'pb8')
    assert cloud_worker.optimizer_command()[1:] == ['/opt/passivbot/src/optimize.py', str(folder/'optimize.json')]


def test_cloud_seed_bundle_is_independent_of_original_files(tmp_path, monkeypatch):
    """Managed seeds freeze their bytes; cloud argv cannot contain the original path."""
    from api import optimize_v9
    from vast_pb9 import launch_bundle
    seeds = tmp_path/'pareto'; seeds.mkdir()
    original = seeds/'candidate.json'; original.write_text('{"config_version":"v9.0.0"}')
    options = {'mode':'pareto_seed', 'source':str(seeds), 'compress_starting_configs':2}
    monkeypatch.setattr(optimize_v9.engine, '_runtime_options_from_config', lambda source: dict(options))
    monkeypatch.setattr(optimize_v9.engine, '_validate_options_for_config', lambda *args:None)
    bundle = launch_bundle({})
    original.write_text('{}')
    assert bundle['starting_configs/0000.json'] == b'{"config_version":"v9.0.0"}'
    stored = json.loads(bundle['launch_options.json'])
    assert stored['source'] == 'starting_configs'
    assert str(tmp_path).encode() not in bundle['launch_options.json']


def test_cloud_validation_preserves_actionable_native_error(monkeypatch):
    """Detached cloud preparation retains a native 422 instead of a generic error."""
    from fastapi import HTTPException
    from api import optimize_v9
    from vast_pb9 import launch_bundle
    from vast_provider import VastError
    def invalid(source):
        """Simulate the native API validator's error contract."""
        raise HTTPException(422, 'Selected scenario is missing')
    monkeypatch.setattr(optimize_v9.engine, '_runtime_options_from_config', invalid)
    with pytest.raises(VastError, match='Selected scenario is missing') as error:
        launch_bundle({})
    assert error.value.status == 422


def test_pb8_polish_and_fine_tune_argv_is_unchanged():
    """The extracted builder preserves the pre-change PB8 ordering and flag names."""
    assert _optimizer_argv('cli', '/config', {'mode':'pareto_seed', 'source':'/seed',
        'fine_tune_params':['long.risk', 'short.strategy'], 'polish_percentage':0.25}) == [
        'cli', '/config', '--start', '/seed', '--fine-tune-params', 'long.risk,short.strategy',
        '--polish-pct', '0.25', '--polish-bounds-mode', 'clamp']


def test_large_manifest_is_a_small_config_reference_and_frozen_launch(tmp_path):
    """Reports larger than the config/draft budget stay in bounded private attachments."""
    from pb9_optimize_manifests import store_manifest, resolve_manifest
    report = manifest()
    data = json.loads(report['text']); data['comment'] = 'ä' * 700_000
    report['text'] = json.dumps(data, ensure_ascii=False)
    assert len(report['text'].encode()) > 1024 * 1024
    root = tmp_path/'attachments'
    reference = store_manifest(root, report)
    assert len(json.dumps(reference)) < 300
    assert store_manifest(root, report) == reference
    assert len(list(root.glob('*.json'))) == 1
    frozen = resolve_manifest(root, reference)
    assert frozen['text'] == report['text']
    assert (root/(reference['artifact'] + '.json')).stat().st_mode & 0o777 == 0o600
    (root/(reference['artifact'] + '.json')).write_text('{}')
    with pytest.raises(ValueError, match='completed|changed'):
        resolve_manifest(root, reference)
    assert frozen['text'] == report['text']


def test_managed_manifest_storage_is_bounded_and_rejects_symlinks(tmp_path, monkeypatch):
    """No caller path, corrupt attachment or unbounded store reaches launch."""
    import pb9_optimize_manifests as storage
    monkeypatch.setattr(storage, 'MAX_ARTIFACTS', 1)
    root = tmp_path/'attachments'
    reference = storage.store_manifest(root, manifest())
    other = manifest(); data = json.loads(other['text']); data['comment'] = 'second'; other['text'] = json.dumps(data)
    with pytest.raises(ValueError, match='full'):
        storage.store_manifest(root, other)
    path = root/(reference['artifact'] + '.json')
    path.unlink(); path.symlink_to(tmp_path/'outside.json')
    with pytest.raises(ValueError, match='missing or invalid'):
        storage.resolve_manifest(root, reference)
    with pytest.raises(ValueError):
        storage.resolve_manifest(root, {**reference, 'artifact':'../outside'})
