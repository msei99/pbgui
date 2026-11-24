"""Offline generation isolation and native PB9 AI/Loop execution contracts."""
import asyncio
import copy
import json
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from loop_runtime import LoopRuntime
from pb9_loop_policy import direction, initial_goals, hsl_policy, upper, validate_exposure


@pytest.fixture
def native_config():
    """Use sparse native Global/Portfolio policy, never PB8-shaped risk aliases."""
    value = json.loads((Path(__file__).parent / 'fixtures/pb9_optimize_metadata.json').read_text())['template']
    value['bot']['global']['risk'].update(max_positions=2, total_wallet_exposure_limit_ratio=1.5)
    value['bot']['global']['universe']['approved_coins'] = ['HYPE']
    value['bot']['long'] = {'enabled': True}
    value['bot']['short'] = {'enabled': False}
    value['backtest'].update(start_date='2025-01-01', end_date='2025-02-01', exchanges=['bybit'])
    value['optimize'].update(backend='pymoo', iters=256, n_cpus=1,
        bounds={'global': {'risk': {'max_positions': [1, 4, 1]}}},
        scoring=[{'metric': 'cagr_fraction_strategy_equity', 'goal': 'max'}])
    value['pbgui'] = {}
    return value


def test_runtime_rejects_unknown_and_separates_storage(tmp_path):
    """The old and native slot may never share owner state or authorization."""
    assert LoopRuntime('v8').directory(tmp_path) == tmp_path / 'data/loop_optimizer'
    assert LoopRuntime('v9').directory(tmp_path) == tmp_path / 'data/loop_optimizer_v9'
    with pytest.raises(ValueError):
        LoopRuntime('v7')


def test_shared_router_instances_have_independent_lifecycle(tmp_path):
    """Both generations expose identical operations backed by separate controllers/caches."""
    from api.loop_optimizer_engine import LoopOptimizerEngine
    old, new = (LoopOptimizerEngine(tmp_path, runtime=version) for version in ('v8', 'v9'))
    assert {(r.path, tuple(sorted(r.methods))) for r in old.router.routes} == {(r.path, tuple(sorted(r.methods))) for r in new.router.routes}
    assert new._controller.store.root != old._controller.store.root
    assert old._list_cache is not new._list_cache
    assert new._controller.store.runtime == 'v9'
    asyncio.run(new.shutdown())
    asyncio.run(old.shutdown())


def test_native_risk_bounds_inheritance_and_step(native_config):
    """A range step is not an upper bound, and explicit side policy shadows shared bounds."""
    native_config['optimize']['bounds']['global']['risk']['max_positions'] = [1, 4, 100]
    assert upper(native_config, 'long', 'max_positions') == 4
    native_config['bot']['long']['risk'] = {'max_positions': 3}
    assert upper(native_config, 'long', 'max_positions') == 3
    native_config['optimize']['fixed_runtime_overrides'] = {'bot.global.risk.max_positions': 5}
    assert upper(native_config, 'long', 'max_positions') == 3
    del native_config['bot']['long']['risk']['max_positions']
    assert upper(native_config, 'long', 'max_positions') == 5
    assert direction(native_config) == 'long'


@pytest.mark.parametrize('mode', ['on', 'off', 'auto'])
def test_native_hsl_does_not_invent_portfolio_enabled(native_config, mode):
    """Only side controllers own activation; portfolio scope remains untouched."""
    before = copy.deepcopy(native_config['bot']['portfolio'])
    result = hsl_policy({'hsl_mode': mode}, native_config, apply=True)
    assert result['bot']['portfolio']['risk']['hsl']['scope'] == before['risk']['hsl']['scope']
    assert 'hsl' not in result['bot']['portfolio']
    if mode != 'auto':
        assert result['bot']['long']['risk']['hsl']['enabled'] is (mode == 'on')
    hsl_policy({'hsl_mode': mode}, result)


def test_native_goals_write_no_legacy_risk_or_live_fields(native_config):
    """User count/direction/coin goals pin real native paths and retain sparse defaults."""
    settings = {'goals': {'direction': 'both', 'positions': 5, 'coins': ['BTC']}}
    initial_goals(native_config, settings)
    assert native_config['bot']['long']['risk']['max_positions'] == 3
    assert native_config['bot']['short']['risk']['max_positions'] == 2
    assert native_config['bot']['global']['risk']['max_positions'] == 2
    assert 'approved_coins' not in native_config['live']
    encoded = json.dumps(native_config)
    assert 'n_positions' not in encoded
    assert '"total_wallet_exposure_limit"' not in encoded
    assert direction(native_config) == 'both'


@pytest.mark.parametrize('where', ['range', 'fixed', 'coin', 'scenario'])
def test_native_twe_ceiling_checks_every_effective_overlay(native_config, where):
    """Bounds and nested overlays cannot bypass an approved exposure ceiling."""
    if where == 'range':
        native_config['optimize']['bounds']['global']['risk']['total_wallet_exposure_limit_ratio'] = [1, 5]
    elif where == 'fixed':
        native_config['optimize']['fixed_runtime_overrides'] = {'bot.global.risk.total_wallet_exposure_limit_ratio': 5}
    elif where == 'coin':
        native_config['coin_overrides'] = {'HYPE': {'bot': {'long': {'risk': {'total_wallet_exposure_limit_ratio': 5}}}}}
    else:
        native_config['backtest']['scenarios'] = [{'overrides': {'bot': {'global': {'risk': {'total_wallet_exposure_limit_ratio': 5}}}}}]
    with pytest.raises(ValueError, match='TWE'):
        validate_exposure(native_config, 2)


def test_native_changed_config_preserves_worker_and_holdout(native_config, monkeypatch):
    """The shared state machine cannot accept native hardware, source or holdout changes."""
    from pb9_loop_policy import validate_change
    from api.loop_optimizer_engine import LoopStart
    monkeypatch.setattr(LoopRuntime, 'prepare', lambda self, config: copy.deepcopy(config))
    settings = LoopStart(config_name='source', provider='chatgpt', model='test', execution='cpu', max_rounds=1).model_dump()
    record = {'settings': settings, 'initial_config': native_config, 'holdouts': [], 'training_exclusions': []}
    validate_change(record, copy.deepcopy(native_config), {})
    for mutate in (lambda c: c['optimize'].update(n_cpus=12),
                   lambda c: c['pbgui'].update(execution='vast'),
                   lambda c: c['backtest'].update(ohlcv_source_dir='/other'),
                   lambda c: c['bot']['global']['universe'].update(approved_coins='coins.json')):
        changed = copy.deepcopy(native_config)
        mutate(changed)
        with pytest.raises(ValueError):
            validate_change(record, changed, {})
    record['holdouts'] = [{'start_date': '2025-01-10', 'end_date': '2025-01-20', 'exchanges': ['bybit']}]
    with pytest.raises(ValueError, match='holdout'):
        validate_change(record, native_config, {})


def test_native_job_cannot_use_pb8_authorization(tmp_path, monkeypatch):
    """Even matching owner/loop identifiers cannot authorize a cross-generation launch."""
    from pb8_loop_store import LoopStore, authorize_native_job
    from loop_runtime import store_for_job
    with pytest.raises(ValueError, match='runtime'):
        store_for_job(tmp_path, {'runtime': 'pb9'})
    seen = []
    def read(self, owner, loop_id, **kwargs):
        seen.append(self.root)
        return {'status': 'running', 'deadline': 10**12}
    monkeypatch.setattr(LoopStore, 'read', read)
    job = {'loop_id': 'a'*32, 'loop_owner': 'b'*32, 'loop_runtime': 'v9', 'runtime': 'pb9'}
    authorize_native_job(tmp_path, job, 'a'*32)
    assert seen == [tmp_path / 'data/loop_optimizer_v9']
    with pytest.raises(HTTPException):
        authorize_native_job(tmp_path, job, None)


def test_native_backtest_bridge_freezes_identity_without_saving_config(native_config, monkeypatch, tmp_path):
    """Ownership must exist at the first queue write; user configs are not overwritten."""
    from pb9_loop_backtest import NativeBacktestLoopAPI
    from api import backtest_v9
    seen = []
    def enqueue(body, cancel, **kwargs):
        seen.append(kwargs['loop_identity'])
        return {'filename': 'test'}
    monkeypatch.setattr(backtest_v9, '_enqueue', enqueue)
    monkeypatch.setattr(backtest_v9, '_save_source', lambda *a, **k: pytest.fail('must not save a user config'))
    bridge = NativeBacktestLoopAPI()
    assert bridge.add_to_queue({'name': 'private', 'config': native_config, 'loop_identity': {'loop_id': 'a'*32, 'loop_owner': 'b'*32}})['filename'] == 'test'
    assert seen[0]['loop_id'] == 'a'*32
    monkeypatch.setattr(backtest_v9, 'PBGDIR', tmp_path)
    assert bridge._log_dir() == tmp_path / 'data/logs/backtests_v9'


def test_ai_metadata_uses_native_catalog(monkeypatch, tmp_path):
    """PB9 selection must never silently query PB8 metrics or templates."""
    from ai_capabilities import AICapabilityService
    caps = AICapabilityService(tmp_path / 'ai')
    api = SimpleNamespace(native=SimpleNamespace(metadata=lambda: {'template': {'config_version': 'v9.0.0'}, 'scoring': {'metrics': ['cagr_fraction_strategy_equity']}}))
    monkeypatch.setattr(caps, '_optimizer_api', lambda version: api if version == 'v9' else pytest.fail('wrong runtime'))
    response = caps._get_optimizer_metadata({'version': 'v9'})
    assert 'cagr_fraction_strategy_equity' in json.dumps(response)
    assert 'v9.0.0' in json.dumps(response)


def test_native_search_ranges_include_global_portfolio(native_config):
    """Shared search exploration must see PB9's inherited and portfolio bounds."""
    from pb8_loop_exploration import _tunable_ranges
    native_config['optimize']['bounds']['global']['entry_cooldown'] = {'base_duration_minutes': [1, 60]}
    native_config['optimize']['bounds']['portfolio'] = {'unstuck': {'memory_ema_span_minutes': [100, 200]}}
    ranges = _tunable_ranges(native_config)
    assert 'global.entry_cooldown.base_duration_minutes' in ranges
    assert 'portfolio.unstuck.memory_ema_span_minutes' in ranges
    assert 'global.risk.max_positions' not in ranges


def test_native_controller_completes_round_and_survives_restart(native_config, monkeypatch, tmp_path):
    """Exercise shared interpretation, optimize, CPU validation and hard limits with native JSON."""
    from tests.test_pb8_loop_optimizer import FakeBackend
    monkeypatch.setattr(LoopRuntime, 'prepare', lambda self, config: copy.deepcopy(config))
    metadata = json.loads((Path(__file__).parent / 'fixtures/pb9_optimize_metadata.json').read_text())
    monkeypatch.setattr('pb9_loop_policy.metric_catalog', lambda: (set(metadata['scoring']['metrics']), {'mean', 'min', 'max'}))
    from pb8_loop_controller import LoopController
    from pb8_loop_store import evaluate
    from api.loop_optimizer_engine import LoopStart
    import pb9_loop_policy
    metric = 'cagr_fraction_strategy_equity'
    rubric = [{'goal': 'gain', 'metrics': [metric], 'direction': 'max', 'target': None, 'scale': 1, 'weight': 1}]
    monkeypatch.setattr(pb9_loop_policy, 'metric_catalog', lambda: ({metric}, {'mean', 'max'}))
    monkeypatch.setattr(LoopRuntime, 'prepare', lambda self, config: copy.deepcopy(config))

    class Backend(FakeBackend):
        """Simulate only detached execution; retain native controller policy checks."""
        def initial(self, name, settings, bundle=None):
            """Return an immutable reviewed native source."""
            from pb8_loop_store import fresh_optimizer_config
            source = fresh_optimizer_config(native_config)
            source['pbgui']['execution'] = 'local'
            return source, {}, {'pb8': 'a'*40, 'runtime': 'v9'}, [], copy.deepcopy(source['backtest'])

        def candidates(self, row, job):
            """Expose a native result without PB8 metric aliases."""
            return [{'id': 'candidate_' + job['operation'], 'optimizer_job': job['operation'],
                     'config': job['config'], 'overrides': {}, 'metrics': {metric: .2}}]

        def observations(self, row, job):
            """Independent CPU checks remain the qualification source."""
            return {'metrics': {metric: .2}, 'exact': True,
                    'assessment': {**evaluate({metric: .2}, rubric), 'simulation_complete': True}}

    class AI:
        """Deterministic, unpaid decisions through every shared controller stage."""
        preflight = AsyncMock()

        async def decide(self, row, stage, evidence):
            """Use native names while requesting another round beyond its hard ceiling."""
            if stage == 'interpret':
                return {'rubric': rubric}
            if stage == 'select':
                return {'candidate_ids': [evidence['candidates'][0]['id']], 'reason': 'Validate native CAGR'}
            changed = copy.deepcopy(row['initial_config'])
            changed['optimize']['bounds'].setdefault('global', {}).setdefault('entry_cooldown', {})['base_duration_minutes'] = [1, 60]
            options = evidence.get('winner_options') or {}
            return {'winner_operation': next(iter(options), None), 'finish': False,
                    'reason': 'Compare native CPU growth', 'knowledge': 'Native observation',
                    'variants': [{'config': changed, 'overrides': {}}]}

    async def run():
        backend = Backend()
        controller = LoopController(tmp_path, backend=backend, ai=AI(), runtime='v9')
        settings = LoopStart(config_name='fixture', provider='chatgpt', model='fixture',
            execution='cpu', max_rounds=1, max_runs=2, goals={'presets': ['gain']}).model_dump()
        row = await controller.create('a'*32, settings)
        controller.store.update('a'*32, row['id'], lambda current: current.update(ai_model_profile={'context': 4_000_000, 'page_tokens': 1500, 'output_tokens': 65536, 'call_timeout': 60, 'streaming': False}, observer_enabled=False))
        for _ in range(40):
            await controller.tick('a'*32, row['id'])
            current = controller.store.read('a'*32, row['id'])
            if current['status'] in {'completed', 'unconfirmed', 'failed'}:
                break
        assert current['status'] == 'completed', (current.get('reason'), current.get('failure'))
        assert len([job for job in current['jobs'] if job['kind'] == 'optimizer']) == 1
        restored = LoopController(tmp_path, backend=backend, ai=AI(), runtime='v9')
        assert restored.store.read('a'*32, row['id'])['status'] == 'completed'
        assert not LoopRuntime('v8').directory(tmp_path).exists()
        await controller.close()
        await restored.close()
    asyncio.run(run())


@pytest.mark.parametrize('key', ['organic_csv', 'hlcvs_data_dir', 'coin_sources'])
def test_ai_input_files_are_rejected_before_native_helper(native_config, monkeypatch, key):
    """Unreviewed model paths never reach the helper that could read them."""
    from pb9_loop_policy import validate_change
    initial = copy.deepcopy(native_config)
    native_config['backtest'][key] = '/unreviewed'
    monkeypatch.setattr(LoopRuntime, 'prepare', lambda *_: pytest.fail('unreviewed path reached helper'))
    with pytest.raises(ValueError, match='Unreviewed'):
        validate_change({'initial_config': initial, 'settings': {}}, native_config, {})


def test_global_fixed_selector_and_defaults_use_the_actual_range(native_config):
    """The GUI count range and direction guard agree on inherited/fixed values."""
    from pb9_loop_policy import defaults
    assert defaults(native_config)['positions']['long'] == [1, 4]
    native_config['optimize']['fixed_params'] = ['global.risk.max_positions']
    assert upper(native_config, 'long', 'max_positions') == 2


@pytest.mark.parametrize('patch', [
    {'bot.long.risk.hsl.enabled': False},
    {'bot.long.risk.hsl': {'enabled': False}},
    {'bot.long.risk': {'hsl': {'enabled': False}}},
])
def test_hsl_scenario_overrides_cannot_bypass_explicit_switch(native_config, patch):
    """Dotted leaf and parent replacements retain the reviewed HSL policy."""
    hsl_policy({'hsl_mode': 'on'}, native_config, apply=True)
    native_config['backtest']['scenarios'] = [{'label': 'train', 'overrides': patch}]
    with pytest.raises(ValueError, match='HSL switch'):
        hsl_policy({'hsl_mode': 'on'}, native_config)


@pytest.mark.parametrize('runtime,loop_runtime', [('pb8', 'v9'), ('pb9', 'v8')])
def test_job_runtime_cannot_cross_loop_stores(tmp_path, runtime, loop_runtime):
    """An inconsistent marker cannot borrow authorization from the other generation."""
    from loop_runtime import store_for_job
    with pytest.raises(ValueError, match='mismatch'):
        store_for_job(tmp_path, {'runtime': runtime, 'loop_runtime': loop_runtime})


def test_queue_identity_binds_retry_to_exact_owner():
    """A replay cannot adopt an ordinary job or a different generation's Loop job."""
    from loop_runtime import queue_identity
    identity = {'loop_id': 'a'*32, 'loop_owner': 'b'*32}
    record = queue_identity(identity, 'v9')
    assert queue_identity(identity, 'v9', record) == record
    for old in ({}, {**record, 'loop_owner': 'c'*32}, {**record, 'loop_runtime': 'v8'}):
        with pytest.raises(ValueError, match='different AI Loop'):
            queue_identity(identity, 'v9', old)


def test_cloud_preparation_has_first_write_ownership_and_exact_replay(tmp_path, monkeypatch):
    """Cloud snapshots retain Loop ownership even if preparation crashes immediately."""
    from vast_jobs import JobStore
    from vast_provider import VastError
    store = JobStore(tmp_path / 'vast')
    identity = {'loop_id': 'a'*32, 'loop_owner': 'b'*32}
    kwargs = {'runtime': 'pb9', 'operation_id': 'c'*32, 'loop_identity': identity}
    first = store.create_preparation('fixture', 256, 1, False, **kwargs)
    assert store.read(first['id'])['loop_runtime'] == 'v9'
    assert store.create_preparation('fixture', 256, 1, False, **kwargs)['id'] == first['id']
    with pytest.raises(VastError, match='another operation'):
        store.create_preparation('fixture', 256, 1, False, **{**kwargs, 'loop_identity': {**identity, 'loop_owner': 'd'*32}})


def test_nested_unrelated_hsl_patch_preserves_reviewed_switch(native_config):
    """Native nested scenario patches merge leaves and do not replace the whole bot."""
    hsl_policy({'hsl_mode': 'on'}, native_config, apply=True)
    native_config['backtest']['scenarios'] = [{'overrides': {'bot': {'long': {'risk': {'max_positions': 2}}}}}]
    hsl_policy({'hsl_mode': 'on'}, native_config)


def test_hsl_nested_unrelated_changes_keep_explicit_switch(native_config):
    """Native nested override documents flatten leaves rather than replacing all bot scopes."""
    hsl_policy({'hsl_mode': 'on'}, native_config, apply=True)
    native_config['backtest']['scenarios'] = [{'overrides': {'bot': {'long': {'risk': {'max_positions': 3}}}}}]
    hsl_policy({'hsl_mode': 'on'}, native_config)
