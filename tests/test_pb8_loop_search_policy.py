"""Isolated contracts for fresh Loop populations and immutable HSL activation."""
import copy

import pytest

from api.loop_optimizer_v8 import LoopStart
from pb8_loop_backend import LoopBackend
from pb8_loop_controller import LoopController
from pb8_loop_store import (LoopStore, apply_hsl_policy, fresh_optimizer_config,
                            validate_change, validate_hsl_policy)
from tests.test_pb8_loop_optimizer import config, settings, valid_gpu_config


@pytest.mark.parametrize('execution', ['cpu', 'gpu', 'vast'])
def test_fresh_jobs_drop_all_seed_sources_and_persist_unique_rng(tmp_path, execution):
    """No inherited self/Pareto/resume source or winner enters the launch population."""
    source = valid_gpu_config() if execution != 'cpu' else config()
    source['optimize'].update(starting_configs='old', starting_config='older', checkpoint='resume', seed=7)
    source['pbgui'].update(optimize_runtime={'mode':'checkpoint_resume', 'source':'old'},
                          optimize_seed_mode='self', optimize_seed_path='old', starting_config=True)
    before = copy.deepcopy(source)
    initial = fresh_optimizer_config(source)
    initial['optimize']['backend'] = 'pymoo' if execution == 'cpu' else 'gpu'
    store = LoopStore(tmp_path)
    row = store.create('a'*32, settings(execution=execution), initial, {}, {}, [], initial['backtest'])
    store.update(row['owner'], row['id'], lambda current: current.update(ai_model_profile={'context': 4_000_000, 'page_tokens': 1500, 'output_tokens': 1000}))
    controller = LoopController(tmp_path, store=store)
    jobs = []
    for _ in range(3):
        proposed = copy.deepcopy(initial)
        proposed['pbgui'].update(optimize_seed_mode='path', optimize_seed_path='untrusted')
        job = controller.job(row, 'optimizer', proposed, {}, 'Independent experiment')
        row['jobs'].append(job)
        jobs.append(job)
    assert len({job['config']['optimize']['seed'] for job in jobs}) == 3
    for job in jobs:
        assert job['config']['optimize']['seed'] != 7
        assert fresh_optimizer_config(job['config']) == job['config']
        assert job['config']['pbgui']['optimize_runtime'] == {'mode':'fresh'}
        assert job['optimizer_start']['reference_only'] is True
        assert job['optimizer_start']['population'] == 'fresh_random'
    store.update(row['owner'], row['id'], lambda current: current.update(jobs=jobs))
    assert store.read(row['owner'], row['id'])['jobs'] == jobs
    assert source == before


@pytest.mark.parametrize('mode', ['on', 'off', 'auto'])
@pytest.mark.parametrize('direction', ['long', 'short', 'both', 'config'])
def test_hsl_user_choice_applies_to_private_initial_bundle(mode, direction):
    """The effective base, runtime, scenario and coin switches honor one saved policy."""
    source = config()
    source['bot']['long']['hsl'] = {'enabled': False, 'some_parameter': 4}
    source['bot']['short']['hsl'] = {'enabled': False}
    source['backtest']['scenarios'] = [{'overrides': {'bot.long.hsl': {'enabled': False}}}]
    source['optimize']['fixed_runtime_overrides'] = {'bot.long.hsl': {'enabled': False}}
    patches = {'BTC': {'bot': {'long': {'hsl': {'enabled': False}}}},
               'ETH': {'bot.short.hsl.enabled': False}}
    before = copy.deepcopy((source, patches))
    chosen = settings(hsl_mode=mode)
    chosen['goals']['direction'] = direction
    effective, overrides = apply_hsl_policy(chosen, source, patches)
    record = {'settings':chosen, 'initial_config':effective}
    validate_hsl_policy(record, effective, overrides)
    if mode == 'auto':
        assert (effective, overrides) == before
    else:
        for side in ('long', 'short'):
            expected = mode == 'on' and direction in {'both', 'config', side}
            assert effective['bot'][side]['hsl']['enabled'] is expected
            assert effective['optimize']['fixed_runtime_overrides'][f'bot.{side}.hsl.enabled'] is expected
    assert (source, patches) == before


@pytest.mark.parametrize('mode', ['on', 'off'])
@pytest.mark.parametrize('layer', ['bot', 'bounds', 'runtime', 'runtime_parent', 'coin', 'coin_parent', 'scenario'])
def test_ai_cannot_bypass_fixed_hsl_mode(mode, layer):
    """Reject contrary activation at every supported effective configuration layer."""
    chosen = settings(hsl_mode=mode)
    initial, patches = apply_hsl_policy(chosen, config(), {})
    row = {'settings':chosen, 'initial_config':initial, 'initial_overrides':{}, 'holdouts':[]}
    proposal = copy.deepcopy(initial)
    enabled = mode != 'on'
    if layer == 'bot':
        proposal['bot']['long']['hsl']['enabled'] = enabled
    elif layer == 'bounds':
        proposal['optimize']['bounds'] = {'long': {'hsl': {'enabled': [False, True]}}}
    elif layer == 'runtime':
        proposal['optimize']['fixed_runtime_overrides']['bot.long.hsl.enabled'] = enabled
    elif layer == 'runtime_parent':
        proposal['optimize']['fixed_runtime_overrides']['bot.long.hsl'] = {'enabled':enabled}
    elif layer == 'coin':
        patches = {'BTC': {'bot.long.hsl.enabled': enabled}}
    elif layer == 'coin_parent':
        patches = {'BTC': {'bot.long': {'hsl': {'enabled': enabled}}}}
    else:
        proposal['backtest']['scenarios'] = [{'overrides': {'bot.long.hsl.enabled':enabled}}]
    with pytest.raises(ValueError, match='HSL'):
        validate_change(row, proposal, patches)


def test_auto_allows_activation_and_on_allows_parameter_changes():
    """Pinning the switch must not freeze unrelated HSL parameters."""
    source = config()
    for mode in ('on', 'auto'):
        chosen = settings(hsl_mode=mode)
        initial, patches = apply_hsl_policy(chosen, source, {})
        proposal = copy.deepcopy(initial)
        proposal['bot']['long'].setdefault('hsl', {})['some_parameter'] = 99
        if mode == 'auto':
            proposal['bot']['long']['hsl']['enabled'] = True
        validate_hsl_policy({'settings':chosen, 'initial_config':initial}, proposal, patches)


@pytest.mark.parametrize('mode', ['on', 'off', 'auto'])
def test_public_loop_settings_roundtrip_hsl_mode(mode):
    """Persist the explicit switch through the same strict model used by Save/Queue."""
    chosen = settings(hsl_mode=mode)
    assert LoopStart.model_validate(chosen).model_dump()['hsl_mode'] == mode
    with pytest.raises(ValueError):
        LoopStart.model_validate(dict(chosen, hsl_mode='sometimes'))


@pytest.mark.parametrize('execution', ['cpu', 'gpu', 'vast'])
def test_backend_initial_clears_population_sources_and_applies_hsl(tmp_path, monkeypatch, execution):
    """The captured baseline is normalized before native validation and fingerprints."""
    import pb8_loop_backend as backend_module
    from api import optimize_v8 as opt
    import scenario_windows
    source = valid_gpu_config() if execution != 'cpu' else config()
    source['pbgui'].update(optimize_runtime={'mode':'pareto_seed', 'source':'__self__'}, optimize_seed_mode='self')
    source['optimize']['starting_configs'] = 'old'
    original = copy.deepcopy(source)
    monkeypatch.setattr(backend_module, 'migrate_loop_bundle', lambda bundle: copy.deepcopy(bundle))
    monkeypatch.setattr(opt, '_config_file', lambda name: tmp_path/'source.json')
    monkeypatch.setattr(opt, 'pb8_runtime_status', lambda: {'ready':True})
    monkeypatch.setattr(opt, '_validate_optimize_backend', lambda *args, **kwargs: None)
    monkeypatch.setattr(scenario_windows, 'build_validation_plan', lambda config: None)
    monkeypatch.setattr(LoopBackend, 'fingerprint', lambda self, config: {})
    if execution == 'gpu':
        import pb8_config
        monkeypatch.setattr(pb8_config, 'save_prepared_pb8_config', lambda *args, **kwargs: None)
    effective, overrides, *_ = LoopBackend(tmp_path, LoopStore(tmp_path)).initial(
        'source', settings(hsl_mode='off', execution=execution), {'config':source, 'override_configs':{}})
    assert effective == fresh_optimizer_config(effective)
    assert effective['bot']['long']['hsl']['enabled'] is False
    validate_hsl_policy({'settings':settings(hsl_mode='off'), 'initial_config':effective}, effective, overrides)
    assert source == original
