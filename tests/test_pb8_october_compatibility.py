"""Offline regressions for the October PB8 config and worker contracts."""

import copy
import json
from pathlib import Path

import pytest

import pb8_config_helper
from vast_config_validation import validate_cloud_config
from vast_worker_compatibility import config_for_worker, worker_compatibility_errors, HSL_BUDGET_SCALING_REVISIONS
from vast_direction_fixtures import direction_config
from tests.test_vast_direction_preflight import queue, prepared, worker
from vast_queue import worker_step
from vast_jobs import IMAGE, REVISION, SUPPORTED_RENTAL_IMAGE_REVISIONS, native_job_config

OLD_IMAGE = "ghcr.io/msei99/pbgui-pb8-worker@sha256:47032b1e42076555a29909a9b888cd24efb69dec6ccd80d06ac26b0bc653ac99"
OLD_REVISION = "00ce7d0ddf123071ae67db149753d6b3619f52ac"


def test_worker_adaptation_preserves_complete_native_pb8_template():
    """The unmodified upstream template loses only two disabled fields for old workers."""
    fixture = json.loads((Path(__file__).parent / 'fixtures/pb8_october_template.json').read_text())
    revision = fixture['origin']['revision']
    assert revision in HSL_BUDGET_SCALING_REVISIONS
    assert fixture['origin']['callable'] == 'config.schema.get_template_config'
    source = fixture['config']
    original = copy.deepcopy(source)
    expected = copy.deepcopy(source)
    for side in ('long', 'short'):
        assert expected['bot'][side]['hsl'].pop('scale_budget_with_excess_allowance') is False

    exported = config_for_worker(source, OLD_REVISION)
    assert exported == expected
    assert source == original
    assert len(worker_compatibility_errors(source, OLD_REVISION)) == 2
    assert not worker_compatibility_errors(exported, OLD_REVISION)
    assert config_for_worker(source, revision) == original
    assert not worker_compatibility_errors(source, revision)


@pytest.mark.parametrize('operation', ['prepare', 'load'])
@pytest.mark.parametrize('authored', ['null', 'missing', 'fixed'])
def test_bridge_preserves_authored_gpu_cpu_values_with_mocked_native_cleaner(tmp_path, monkeypatch, operation, authored):
    """Unit-test bridge preservation in both paths using a modeled native cleaner."""
    source = {'optimize': {'backend': 'gpu'}}
    if authored != 'missing':
        source['optimize']['n_cpus'] = None if authored == 'null' else 7

    def prepare(config, **kwargs):
        """Model native preparation retaining None and hydrating omitted values."""
        result = copy.deepcopy(config)
        result['optimize'].setdefault('n_cpus', 4)
        return result

    def sanitize(config):
        """Model PB8's dump cleaner filling null from its template."""
        result = copy.deepcopy(config)
        if result['optimize']['n_cpus'] is None:
            result['optimize']['n_cpus'] = 4
        return result

    path = tmp_path / 'config.json'
    path.write_text(json.dumps(source))
    modules = {'prepare_config': prepare, 'sanitize': sanitize,
               'load_prepared_config': lambda path, **kwargs: prepare(json.loads(Path(path).read_text()))}
    monkeypatch.setattr(pb8_config_helper, '_load_pb8_modules', lambda _: modules)
    request = {'pb8_dir': str(tmp_path), 'operation': operation, 'config': source, 'config_path': str(path)}
    result = pb8_config_helper.handle(request)['config']
    assert result['optimize']['n_cpus'] == {'null': None, 'missing': 4, 'fixed': 7}[authored]
    assert source['optimize'].get('n_cpus') == (7 if authored == 'fixed' else None)


@pytest.mark.parametrize('location', ['base', 'fixed', 'scenario', 'subtree'])
def test_old_cloud_profile_rejects_new_hsl_policy(location):
    """Every supported representation reaches the compatibility boundary."""
    config = direction_config()
    key = 'bot.long.hsl.scale_budget_with_excess_allowance'
    if location == 'base':
        config['bot']['long'].setdefault('hsl', {})['scale_budget_with_excess_allowance'] = True
    elif location == 'fixed':
        config['optimize']['fixed_runtime_overrides'] = {key: True}
    else:
        overrides = {key: True} if location == 'scenario' else {'bot.long.hsl': {'scale_budget_with_excess_allowance': True}}
        config['backtest'].update(suite_enabled=True, scenarios=[{'label': 'scaled', 'overrides': overrides}])
    assert worker_compatibility_errors(config, OLD_REVISION)
    assert any('HSL budget scaling' in item['message'] for item in validate_cloud_config(config, image=OLD_IMAGE, revision=OLD_REVISION))
    assert not worker_compatibility_errors(config, next(iter(HSL_BUDGET_SCALING_REVISIONS)))


@pytest.mark.parametrize('location', ['base', 'fixed', 'scenario', 'subtree', 'nested', 'portfolio'])
def test_cloud_export_removes_disabled_unsupported_hsl_without_editing_source(location):
    """False is safe only after removal from an old-worker export, across layouts."""
    config = direction_config()
    # Supply the base path for scenario validation's existing-path contract.
    config['bot']['long']['hsl'] = {'enabled': False, 'scale_budget_with_excess_allowance': False}
    key = 'bot.long.hsl.scale_budget_with_excess_allowance'
    if location == 'fixed':
        config['optimize']['fixed_runtime_overrides'] = {key: False}
    elif location == 'portfolio':
        config['bot']['hsl'] = {'scale_budget_with_excess_allowance': False}
    elif location in ('scenario', 'subtree', 'nested'):
        overrides = ({key: False} if location == 'scenario' else
                     {'bot.long.hsl': {'scale_budget_with_excess_allowance': False}} if location == 'subtree' else
                     {'bot': {'long': {'hsl': {'scale_budget_with_excess_allowance': False}}}})
        config['backtest'].update(suite_enabled=True, scenarios=[{'label': 'disabled', 'overrides': overrides}])
    original = copy.deepcopy(config)

    assert not validate_cloud_config(config)
    exported = config_for_worker(config, OLD_REVISION)
    assert config == original
    assert 'scale_budget_with_excess_allowance' not in json.dumps(exported)
    assert exported['bot']['long']['hsl']['enabled'] is False
    assert not worker_compatibility_errors(exported, OLD_REVISION)
    assert worker_compatibility_errors(config, OLD_REVISION)
    assert config_for_worker(config, next(iter(HSL_BUDGET_SCALING_REVISIONS))) == original
    if location in ('scenario', 'subtree', 'nested'):
        assert exported['backtest']['scenarios'][0]['overrides'] == {}


@pytest.mark.parametrize('value', [0, None, 'false'])
def test_cloud_export_does_not_treat_invalid_hsl_values_as_disabled(value):
    """Only the boolean false is eligible for lossless old-schema adaptation."""
    config = direction_config()
    config['bot']['long']['hsl'] = {'scale_budget_with_excess_allowance': value}
    assert worker_compatibility_errors(config, OLD_REVISION, for_preparation=True)
    assert config_for_worker(config, OLD_REVISION) == config


@pytest.mark.parametrize('old_image', [False, True])
@pytest.mark.parametrize('value', [False, True])
def test_dispatch_skips_incompatible_candidate_and_keeps_it_waiting(queue, old_image, value):
    """Actual rental capabilities protect both current and supported old leases."""
    config = direction_config()
    config['bot']['long'].setdefault('hsl', {})['scale_budget_with_excess_allowance'] = value
    blocked = prepared(queue.store, config)
    ready = prepared(queue.store, direction_config())
    image, revision = next((i, r) for i, r in SUPPORTED_RENTAL_IMAGE_REVISIONS.items() if r not in HSL_BUDGET_SCALING_REVISIONS and i != OLD_IMAGE) if old_image else (OLD_IMAGE, OLD_REVISION)
    identifier = worker(queue, image, revision)
    assert worker_step(queue, identifier, now=100) == ready['id']
    state = queue.store.read(blocked['id'])
    assert state['status'] == 'ready'
    assert 'compatible worker' in state['error']
    assert 'lease_id' not in state
    assert queue.store.read(blocked['id'], 'input/optimize.json') == config
    if value is False:
        assert 'Prepare the job again' in state['error']


@pytest.mark.parametrize('old_image', [False, True])
@pytest.mark.parametrize('damage', ['missing', 'malformed'])
def test_dispatch_keeps_unreadable_snapshot_waiting_and_runs_next_job(queue, old_image, damage):
    """Unreadable temporary inputs fail closed on current and supported old rentals."""
    blocked = prepared(queue.store, direction_config())
    ready = prepared(queue.store, direction_config())
    snapshot = queue.store.directory(blocked['id']) / 'input/optimize.json'
    if damage == 'missing':
        snapshot.unlink()
    else:
        snapshot.write_text('{')
    image, revision = next((i, r) for i, r in SUPPORTED_RENTAL_IMAGE_REVISIONS.items() if i != IMAGE) if old_image else (IMAGE, REVISION)
    identifier = worker(queue, image, revision)

    assert worker_step(queue, identifier, now=100) == ready['id']
    state = queue.store.read(blocked['id'])
    assert state['status'] == 'ready'
    assert 'prepare the job again' in state['error'].lower()
    assert 'lease_id' not in state
    if old_image:
        # Current rentals reject earlier at direction preflight; older rentals
        # reach the new per-rental compatibility inspection itself.
        assert state['error'] == ('Waiting for a compatible worker: Cannot verify '
                                  'the prepared job for this worker. Prepare the job again.')


@pytest.mark.parametrize('value', [False, True])
def test_current_worker_exports_and_dispatches_native_hsl_scaling(queue, value):
    """The activated worker retains native HSL fields and runs their snapshots."""
    config = direction_config()
    config['bot']['long'].setdefault('hsl', {})['scale_budget_with_excess_allowance'] = value
    exported = native_job_config(config, 512, 4, False)
    assert exported['bot']['long']['hsl']['scale_budget_with_excess_allowance'] is value
    assert not validate_cloud_config(config)
    ready = prepared(queue.store, exported)
    identifier = worker(queue)
    assert worker_step(queue, identifier, now=100) == ready['id']
