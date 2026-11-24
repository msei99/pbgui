"""PB9 cloud admission and installer source must use one reviewed worker release."""
from __future__ import annotations

import copy
from pathlib import Path
from types import SimpleNamespace

import pytest
from jinja2 import Environment, StrictUndefined
import yaml

import passivbot_runtime
import vast_pb9
from vast_provider import VastError
from vast_worker_profiles import WorkerProfile

COMMIT = 'a' * 40
IMAGE = 'ghcr.io/msei99/pbgui-pb9-worker@sha256:' + 'b' * 64


@pytest.fixture
def admission(monkeypatch):
    """Use synthetic source/config identities without native execution or provider access."""
    identity = {'ready': True, 'commit': COMMIT, 'config_schema': 'v9.0.0', 'source_clean': True}
    profile = WorkerProfile('pb9', COMMIT, IMAGE, 'v9.0.0')
    source = {'config_version': 'v9.0.0', 'backtest': {}, 'optimize': {'scoring': [], 'limits': []}}
    calls = []

    def runtime(*, strict=False):
        """Require the real admission path to inspect a clean source."""
        assert strict is True
        calls.append('runtime')
        return dict(identity)

    def prepare(config):
        """Record native validation without loading an actual bot installation."""
        calls.append('native_prepare')
        return copy.deepcopy(config)

    native = SimpleNamespace(runtime=runtime, prepare=prepare,
                             metadata=lambda: {'scoring': {'metrics': ['cagr_fraction_strategy_equity']}})
    monkeypatch.setattr(vast_pb9, '_adapter', lambda: native)
    monkeypatch.setattr(vast_pb9, 'worker_profile', lambda _runtime: profile)
    return identity, source, native, calls


@pytest.mark.parametrize('field,value', [('commit', 'c' * 40), ('config_schema', 'v9.1.0'), ('ready', False)])
def test_mismatch_is_rejected_before_native_plan_or_job_publication(admission, monkeypatch, field, value):
    """Preview and export cannot disagree or reach data preparation for an incompatible worker."""
    identity, config, _native, calls = admission
    identity[field] = value
    import pb9_config
    monkeypatch.setattr(pb9_config, 'get_pb9_optimize_cloud_plan', lambda *_a, **_kw: pytest.fail('cloud plan called'))
    errors = vast_pb9.validate(config)
    assert errors[0]['path'] == 'worker'
    assert errors[0]['status'] == 409
    assert 'Update PB9' in errors[0]['message']
    assert 'rebuild' not in errors[0]['message'].lower()
    with pytest.raises(VastError) as rejected:
        vast_pb9.prepare(config, 256, 4, False)
    assert rejected.value.status == 409
    assert calls == ['runtime', 'runtime']


def test_matching_runtime_retains_normal_native_validation(admission):
    """Cloud availability does not require the master's machine to have a CUDA GPU."""
    _identity, config, _native, calls = admission
    assert vast_pb9.validate(config, 256, 4) == []
    assert calls == ['runtime', 'native_prepare']
    config['optimize']['scoring'] = [{'metric': 'retired_metric'}]
    assert vast_pb9.validate(config)[0]['path'] == 'optimize.scoring.0.metric'


def test_dirty_source_is_actionable_without_exposing_internal_errors(admission):
    """Source verification failures cannot fall through to expensive native preparation."""
    _identity, config, native, calls = admission
    def dirty(**_kwargs):
        """Simulate a checkout changed during a normal update."""
        raise RuntimeError('private diagnostic must not leak')
    native.runtime = dirty
    errors = vast_pb9.validate(config)
    assert errors[0]['code'] == 'source_unverified'
    assert 'private diagnostic' not in str(errors)
    assert calls == []


def test_preview_endpoint_does_not_fetch_metadata_after_worker_rejection(admission, monkeypatch):
    """An unavailable runtime returns valid=false instead of a second metadata HTTP 503."""
    from api.optimize_v9 import engine
    from api import vast_v9
    identity, config, native, _calls = admission
    identity['commit'] = 'c' * 40
    monkeypatch.setattr(vast_v9, '_profile', lambda: {'published': True})
    monkeypatch.setattr(engine, 'native', native)
    native.metadata = lambda: pytest.fail('Metadata accessed after a worker mismatch')
    result = engine.cloud_validate({'config': config})
    assert result['valid'] is False and result['config_valid'] is False
    assert result['errors'][0]['code'] == 'worker_source_mismatch'
    assert result['metrics'] == []


def test_final_native_plan_rechecks_source_against_a_racing_update(admission, monkeypatch):
    """The final native source proof stays mandatory even after a successful preview."""
    _identity, config, native, _calls = admission
    import pb9_config
    native.options = lambda: ()
    native.stop_flag = lambda: None
    monkeypatch.setattr(vast_pb9, '_launch_options', lambda _source: {'mode': 'fresh'})
    monkeypatch.setattr(pb9_config, 'get_pb9_optimize_cloud_plan', lambda *_a, **_kw:
        {'source': {'commit': 'c' * 40, 'config_schema': 'v9.0.0'}})
    with pytest.raises(VastError, match='matching version') as rejected:
        vast_pb9.prepare(config, 256, 4, False)
    assert rejected.value.status == 409


@pytest.mark.parametrize('value', ['', 'main', 'A' * 40, 'a' * 40 + '\nother', 'a' * 129])
def test_release_pin_rejects_invalid_values(tmp_path, monkeypatch, value):
    """The public pin cannot become a branch name, shell fragment or unbounded input."""
    monkeypatch.setattr(passivbot_runtime, '__file__', str(tmp_path / 'passivbot_runtime.py'))
    (tmp_path / 'pb9_ref.txt').write_text(value)
    with pytest.raises(ValueError):
        passivbot_runtime.pb9_release_commit()


def test_release_pin_rejects_symlinks_and_accepts_a_full_hash(tmp_path, monkeypatch):
    """Only a bounded regular source-controlled pin is accepted."""
    monkeypatch.setattr(passivbot_runtime, '__file__', str(tmp_path / 'passivbot_runtime.py'))
    other = tmp_path / 'other.txt'
    other.write_text('a' * 40)
    pin = tmp_path / 'pb9_ref.txt'
    pin.symlink_to(other)
    with pytest.raises(ValueError, match='Invalid'):
        passivbot_runtime.pb9_release_commit()
    pin.unlink()
    pin.write_text('a' * 40 + '\n')
    assert passivbot_runtime.pb9_release_commit() == 'a' * 40


def test_matching_runtime_passes_preview_and_export_rechecks_final_source(admission, monkeypatch):
    """A source replacement between preview and export must still fail before publication."""
    _identity, config, native, calls = admission
    assert vast_pb9.validate(config) == []
    assert 'native_prepare' in calls
    native.options = lambda: ()
    native.stop_flag = lambda: None
    import pb9_config
    monkeypatch.setattr(pb9_config, 'get_pb9_optimize_cloud_plan', lambda *_a, **_kw: {
        'source': {'commit': 'd' * 40, 'config_schema': 'v9.0.0'}})
    with pytest.raises(VastError, match='Update PB9') as error:
        vast_pb9.prepare(config, 256, 1, False)
    assert error.value.status == 409


def test_unverified_source_blocks_preview_without_loading_bot_code(admission, monkeypatch):
    """A dirty checkout cannot reach native config import merely to preview cloud availability."""
    _identity, config, native, calls = admission
    def dirty(**_kwargs):
        """Model source_identity's tracked-tree rejection."""
        raise RuntimeError('dirty source')
    native.runtime = dirty
    errors = vast_pb9.validate(config)
    assert errors[0]['code'] == 'source_unverified'
    assert 'native_prepare' not in calls


@pytest.mark.parametrize('product,branch,override,expected', [
    ('pb9', False, None, 'pin'), ('pb9', True, None, ''),
    ('pb9', False, '', 'pin'), ('pb9', True, '', ''), ('pb8', False, '', ''),
    ('pb9', True, 'c' * 40, 'c' * 40), ('pb8', False, None, ''),
])
@pytest.mark.parametrize('filename', ['master-update-pb8.yml', 'vps-update-pb8.yml'])
def test_default_update_selects_release_pin_but_explicit_branch_preserves_choice(product, branch, override, expected, filename):
    """Advancing official v9 HEAD cannot silently change a default release-matched installation."""
    variables = yaml.safe_load(Path(filename).read_text())[0]['vars']
    env = Environment(undefined=StrictUndefined)
    pin = passivbot_runtime.pb9_release_commit()
    env.globals['lookup'] = lambda _kind, _path: pin
    context = {'passivbot_product': product, 'playbook_dir': str(Path.cwd()), 'pb8_branch_switch': branch}
    if override is not None:
        context['pb8_commit'] = override
    context['passivbot_release_commit'] = env.from_string(variables['passivbot_release_commit']).render(**context)
    actual = env.from_string(variables['pb8_target_commit']).render(**context)
    assert actual == (pin if expected == 'pin' else expected)


def test_installer_clones_exact_release_source_without_tracking_moving_head(tmp_path, monkeypatch):
    """The separate master bootstrap source uses the same pin as normal Ansible updates."""
    from setup.installer import core
    commands = []
    monkeypatch.setattr(core, '_run_command', lambda argv, *_a, **_kw: commands.append([str(x) for x in argv]))
    monkeypatch.setattr(core, 'verify_passivbot_major', lambda *_a, **_kw: None)
    core._clone_installer_wheel_source('pb9', tmp_path, lambda _line: None)
    pin = passivbot_runtime.pb9_release_commit()
    assert any(command[:4] == ['git', '-C', str(tmp_path / 'pb9-source'), 'fetch'] and command[-1] == pin
               for command in commands)
    assert not any('--branch' in command for command in commands)


def test_queue_rejects_source_before_creating_job_or_reading_credentials(admission, tmp_path, monkeypatch):
    """A direct Queue request gets the same 409 as preview before any durable work."""
    from contextlib import nullcontext
    from fastapi import HTTPException
    from api import vast
    from api.optimize_v9 import engine
    identity, config, native, _calls = admission
    identity['commit'] = 'c' * 40
    path = tmp_path / 'optimize.json'
    path.write_text('{}')
    monkeypatch.setattr(engine, 'native', native)
    monkeypatch.setattr(engine, '_config_file', lambda _name: path)
    monkeypatch.setattr(engine, '_config_lock', nullcontext)
    native.load = lambda _path: config
    monkeypatch.setattr(vast, 'JobStore', lambda: pytest.fail('Job published before compatibility check'))
    monkeypatch.setattr(vast, 'VastCredentialStore', lambda *_a: pytest.fail('Credentials accessed'))
    monkeypatch.setattr(vast, 'VastClient', lambda *_a: pytest.fail('Provider accessed'))
    with pytest.raises(HTTPException) as error:
        vast._queue_preparation_job(vast.PrepareJobRequest(config_name='fixture'), runtime='pb9')
    assert error.value.status_code == 409
    assert 'Update PB9' in error.value.detail


def test_published_worker_and_installers_share_the_same_reviewed_source():
    """The recorded public manifest binds the pin, native metadata and image layers."""
    import json
    from vast_worker_profiles import worker_profile
    from vast_jobs import SUPPORTED_RENTAL_IMAGE_REVISIONS
    from vast_image_layers import IMAGE_LAYERS
    from vast_calibration import calibration_worker_available
    root = Path(__file__).resolve().parents[1]
    receipt = json.loads((root/'setup/vast_gpu_benchmark/pb9_worker_release_7e3f6d4e.json').read_text())
    contract = json.loads((root/'setup/vast_gpu_benchmark/pb9_worker_contract.json').read_text())
    profile = worker_profile('pb9')
    assert profile.revision == receipt['source'] == contract['commit'] == passivbot_runtime.pb9_release_commit()
    assert profile.schema == contract['schema']
    assert profile.image == 'ghcr.io/msei99/pbgui-pb9-worker@' + receipt['digest']
    assert IMAGE_LAYERS[profile.image] == {key[7:19]: size for key, size in receipt['layers'].items()}
    assert calibration_worker_available(profile.image)
    previous = 'ghcr.io/msei99/pbgui-pb9-worker@sha256:7ee6a7bf0f6bb0c235e87ab2988efd1d2f0529f4348f1d9c20a13d8384395bcb'
    assert SUPPORTED_RENTAL_IMAGE_REVISIONS[previous] == 'bf7329b039595e65acdf5c14475078912aeede96'
