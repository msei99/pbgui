"""Offline autonomous PB8 loop policy, budgeting, ownership and recovery contracts."""
from __future__ import annotations

import asyncio
import copy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from api.loop_optimizer_v8 import LoopStart, LoopGoals
from pb8_loop_ai import LoopAI, LoopAIResponseError, parse_object, validate_rubric
from pb8_loop_backend import LoopBackend
from pb8_loop_controller import LoopController
from pb8_loop_store import LoopStore, authorize_native_job, evaluate, validate_change
from tests.loop_ai_transport import LoopAI
from pb8_loop_evidence import LoopAIPause

OWNER = 'a' * 32
RULE = [{'goal': 'gain', 'metrics': ['gain'], 'direction': 'max', 'target': 2, 'scale': 1, 'weight': 1}]


@pytest.mark.parametrize('execution', ['cpu', 'gpu', 'vast'])
def test_completion_boundary_tolerance_is_private_to_gpu_optimizer(execution):
    """Avoid full-completion drift mismatches without changing saved or other limits."""
    from pb8_loop_store import apply_run_limit
    original = config()
    limits = [
        {'metric': 'backtest_completion_ratio', 'penalize_if': 'less_than', 'value': 1.0},
        {'metric': 'backtest_completion_ratio', 'penalize_if': 'less_than', 'value': 0.98},
        {'metric': 'drawdown_worst_strategy_eq', 'penalize_if': 'greater_than', 'value': 0.5},
        {'metric': 'backtest_completion_ratio', 'penalize_if': 'less_than', 'value': 1, 'enabled': False},
    ]
    original['optimize']['limits'] = copy.deepcopy(limits)
    effective = apply_run_limit({'execution': execution}, original)
    assert original['optimize']['limits'] == limits
    assert effective['optimize']['limits'][1:] == limits[1:]
    threshold = effective['optimize']['limits'][0]['value']
    assert threshold == (1.0 if execution == 'cpu' else 0.99)
    if execution != 'cpu':
        assert 0.9999946621713555 >= threshold
        assert 0.98 < threshold


def config():
    """Return a complete minimal input without files, credentials or real runtime."""
    return {'backtest': {'start_date': '2020-01-01', 'end_date': '2020-02-01', 'exchanges': ['binance']},
            'bot': {side: {'risk': {'n_positions': 2, 'total_wallet_exposure_limit': 1}} for side in ('long', 'short')},
            'live': {'approved_coins': {'long': ['BTC'], 'short': ['BTC']}},
            'optimize': {'backend': 'pymoo', 'bounds': {}, 'n_cpus': 1}, 'pbgui': {}}


def valid_gpu_config():
    """Complete synthetic GPU gates without changing CPU policy fixtures."""
    from tests.vast_direction_fixtures import complete_direction
    value = config()
    value['live'].update(strategy_kind='ema_anchor', approved_coins={'long': ['BTC', 'ETH'], 'short': ['BTC', 'ETH']})
    value['optimize'].update(backend='gpu', iters=512, scoring=[{'metric':'adg_strategy_eq','goal':'max'}], limits=[])
    return complete_direction(value)


def settings(**changes):
    """Validate controller settings through the public request model."""
    value = LoopStart(max_rounds=10, config_name='source', provider='chatgpt', model='pinned', goals=LoopGoals(presets=['gain']), authorization=True).model_dump()
    value.update(changes)
    value['max_rounds'] = changes.get('max_rounds', min(10, value['max_runs']))
    return value


@pytest.fixture
def record(tmp_path, monkeypatch):
    """Keep all persistent evidence under a Pytest temporary directory."""
    monkeypatch.setattr('ai_loop_manual.installed_manual', lambda: {
        'source': 'offline_test_fixture', 'digest': 'test-manual', 'index': [],
        'documents': [], 'omitted': []})
    store = LoopStore(tmp_path / 'data/loop_optimizer')
    value = store.create(OWNER, settings(), config(), {}, {'pb8': 'commit', 'data': 'shards'}, [], config()['backtest'])
    store.update(OWNER, value['id'], lambda row: row.update(baseline_required=False, observer_enabled=False, ai_model_profile={'context': 4_000_000, 'page_tokens': 1500, 'output_tokens': 65536, 'call_timeout': 60, 'streaming': False}))
    value['baseline_required'] = False
    value['observer_enabled'] = False
    value['ai_model_profile'] = {'context': 4_000_000, 'page_tokens': 1500, 'output_tokens': 65536, 'call_timeout': 60, 'streaming': False}
    return store, value


@pytest.mark.parametrize('enabled', [True, False])
def test_scenario_toggle_protects_inherited_values(record, enabled):
    """Disabled editor protects implicit dates as well as explicit scenario rows."""
    store, row = record
    row['settings']['scenario_enabled'] = enabled
    candidate = config()
    candidate['backtest']['end_date'] = '2020-03-01'
    if enabled:
        assert validate_change(row, candidate, {}) == candidate
    else:
        with pytest.raises(ValueError, match='Scenario Editor'):
            validate_change(row, candidate, {})


def test_entire_optimizer_config_is_editable(record):
    """An arbitrary valid optimizer setting is not subject to an allowlist."""
    _, row = record
    candidate = config()
    candidate['optimize'].update(new_algorithm={'arbitrary': 12}, scoring=['different'], iters=10000)
    assert validate_change(row, candidate, {}) == candidate


def _warm_start_observation(row, score, exposure=2):
    """Build isolated exact evidence with identifiable bot and optimizer settings."""
    value = copy.deepcopy(row['initial_config'])
    value['bot']['long']['risk']['total_wallet_exposure_limit'] = exposure
    value['optimize']['mutation_eta'] = 99
    return {'exact': True, 'assessment': {'score': score, 'hard_targets_met': True,
            'comparable': True, 'simulation_complete': True, 'achieved': False},
            'candidate': {'id': f'candidate_{exposure}', 'optimizer_job': f'optimizer_{exposure}',
                          'config': value, 'overrides': {}}, 'metrics': {'gain': score}}


@pytest.mark.parametrize('score,exact,comparable,complete,hard,expected', [
    (2, True, True, True, True, 'candidate_2'),
    (1, True, True, True, True, 'candidate_2'),
    (.5, True, True, True, True, 'candidate_2'),
    (2, False, True, True, True, 'baseline'),
    (2, True, False, True, True, 'baseline'),
    (2, True, True, False, True, 'baseline'),
    (20, True, True, True, False, 'baseline'),
])
def test_optimizer_start_uses_qualified_selection_without_score_ranking(
        record, score, exact, comparable, complete, hard, expected):
    """Only qualified exact winners provide comparison references; legacy score has no authority."""
    from pb8_loop_store import optimizer_start_candidate
    _, row = record
    row['baseline'] = _warm_start_observation(row, 1, 1)
    row['best'] = _warm_start_observation(row, score)
    row['best']['exact'] = exact
    row['best']['assessment'].update(comparable=comparable, simulation_complete=complete, hard_targets_met=hard)
    row['observer_jobs'] = [{'assessment': {'score': 10000}, 'candidate': _warm_start_observation(row, 10000, 90)['candidate']}]
    before = copy.deepcopy(row)
    assert optimizer_start_candidate(row)['candidate_id'] == expected
    assert row == before


def test_optimizer_job_carries_bot_but_preserves_ai_recipe_and_budget(record, tmp_path):
    """Complete AI experiments survive even when they differ from the confirmed reference."""
    store, row = record
    row['settings'].update(run_limit_mode='proxy', run_proxy=100000)
    row['best'] = _warm_start_observation(row, 2)
    row['best']['candidate']['overrides'] = {'BTC': {'bot': {'long': {'risk': {'total_wallet_exposure_limit': 2.5}}}}}
    proposed = config()
    proposed['optimize'].update(mutation_eta=8, bounds={'long': {'risk': {'total_wallet_exposure_limit': [0, 4]}}},
                               scoring=['gain'], limits=[{'metric': 'drawdown', 'value': .2}], iters=7)
    before = copy.deepcopy((row, proposed))
    controller = LoopController(tmp_path, store=store, backend=FakeBackend(), ai=FakeAI())
    job = controller.job(row, 'optimizer', proposed, {}, 'Experiment')
    assert job['config']['bot'] == proposed['bot']
    assert job['overrides'] == {}
    assert job['optimizer_start']['bot_modified'] is True
    assert job['optimizer_start']['overrides_modified'] is True
    assert job['config']['optimize'] == {**proposed['optimize'], 'iters': 10000000, 'seed': job['config']['optimize']['seed'], 'pareto_max_size': job['optimizer_archive']['effective']}
    assert job['optimizer_start']['candidate_id'] == 'candidate_2'
    assert (row, proposed) == before
    for kind in ('baseline', 'validation', 'holdout', 'observer_holdout', 'observer_full_range'):
        unchanged = controller.job(row, kind, proposed, {}, 'Existing check')
        assert unchanged['config'] == proposed and unchanged['overrides'] == {}
        assert unchanged['optimizer_start'] is None


@pytest.mark.parametrize('enabled', [True, False])
def test_optimizer_preserves_explicit_bot_and_override_experiments(record, tmp_path, enabled):
    """HSL/Unstuck toggles, sizing and sparse overrides survive in either direction."""
    store, row = record
    row['initial_config']['bot']['long'].update(hsl={'enabled': False}, unstuck={'enabled': False})
    row['best'] = _warm_start_observation(row, 2)
    best_bot = row['best']['candidate']['config']['bot']['long']
    best_bot.update(hsl={'enabled': not enabled}, unstuck={'enabled': not enabled})
    proposed = copy.deepcopy(row['best']['candidate']['config'])
    proposed['bot']['long']['hsl']['enabled'] = enabled
    proposed['bot']['long']['unstuck']['enabled'] = enabled
    proposed['bot']['long']['risk']['we_excess_allowance_pct'] = .4
    proposed['bot']['long']['strategy'] = {'ema_anchor': {'base_qty_pct': .096}}
    proposed['optimize']['bounds'] = {'long': {'risk': {'we_excess_allowance_pct': [0, .5]},
        'strategy': {'ema_anchor': {'base_qty_pct': [.001, .1, .0001]}}}}
    overrides = {'BTC': {'bot': {'long': {'hsl': {'enabled': enabled}}}}}
    before = copy.deepcopy((row, proposed, overrides))
    job = LoopController(tmp_path, store=store, backend=FakeBackend(), ai=FakeAI()).job(
        row, 'optimizer', proposed, overrides, 'Explicit bot experiment')
    from pb8_loop_store import fresh_optimizer_config
    expected_config = fresh_optimizer_config(proposed, job['config']['optimize']['seed'])
    expected_config['optimize']['pareto_max_size'] = job['optimizer_archive']['effective']
    assert job['config'] == expected_config
    assert job['overrides'] == overrides
    assert job['optimizer_start']['candidate_id'] == row['best']['candidate']['id']
    assert job['optimizer_start']['bot_modified'] is True
    assert (row, proposed, overrides) == before


@pytest.mark.parametrize('change', ['direction', 'positions', 'strategy'])
def test_explicit_bot_experiments_cannot_bypass_user_constraints(record, tmp_path, change):
    """Keeping AI bot edits must not silently repair forbidden direction/capacity changes."""
    store, row = record
    proposed = copy.deepcopy(row['initial_config'])
    if change == 'direction':
        row['settings']['goals']['direction'] = 'long'
        proposed['bot']['short']['risk']['n_positions'] = 1
    elif change == 'positions':
        row['settings']['goals']['positions'] = 1
        proposed['bot']['long']['risk']['n_positions'] = 2
    else:
        proposed['live']['strategy_kind'] = 'ema_anchor'
    with pytest.raises(ValueError):
        LoopController(tmp_path, store=store, backend=FakeBackend(), ai=FakeAI()).job(
            row, 'optimizer', proposed, {}, 'Forbidden change')


def test_optimizer_start_preserves_authorized_strategy_experiment(record, tmp_path):
    """A permitted strategy switch keeps its compatible template and remains validated."""
    store, row = record
    row['settings']['strategy_enabled'] = True
    row['best'] = _warm_start_observation(row, 2)
    proposed = config()
    proposed['live']['strategy_kind'] = 'ema_anchor'
    controller = LoopController(tmp_path, store=store, backend=FakeBackend(), ai=FakeAI())
    job = controller.job(row, 'optimizer', proposed, {}, 'Switch strategy')
    assert job['config']['bot'] == proposed['bot']
    assert job['optimizer_start']['candidate_id'] is None
    row['settings']['strategy_enabled'] = False
    with pytest.raises(ValueError, match='Strategy changes'):
        controller.job(row, 'optimizer', proposed, {}, 'Disallowed switch')


def test_warm_start_retains_winner_through_worse_rounds_and_restart(record, tmp_path, monkeypatch):
    """Evaluation retains the confirmed reference without forcing it over complete AI experiments."""
    store, row = record
    async def isolated_operation(function, *args):
        """Keep the controller test synchronous with isolated data and no native jobs."""
        return function(*args)
    monkeypatch.setattr('pb8_loop_controller.owned_thread', isolated_operation)
    def initialize(current):
        """Prepare a fixed baseline and enough experiment allowance."""
        current.update(rubric=RULE, baseline=_warm_start_observation(current, 1, 1))
        current['settings'].update(patience=10, max_runs=20)
    store.update(OWNER, row['id'], initialize)
    for number, (score, expected) in enumerate([(1.5, 2), (1.2, 2), (1.5, 2), (1.8, 5)]):
        def ready(current):
            """Supply one completed exact comparison for the current synthetic round."""
            observation = _warm_start_observation(current, score, number + 2)
            observation['operation'] = f'validation_{number}'
            current.update(round=number, phase='evaluate', observations=[observation])
            current['jobs'].append({'operation': observation['operation'], 'kind': 'validation',
                                    'round': number, 'status': 'completed'})
        store.update(OWNER, row['id'], ready)
        controller = LoopController(tmp_path, store=store, backend=FakeBackend(), ai=FakeAI())
        asyncio.run(controller._tick(store.read(OWNER, row['id'])))
        current = store.read(OWNER, row['id'])
        assert current['phase'] == 'optimize'
        job = current['jobs'][-1]
        assert job['optimizer_start']['candidate_id'] == f'candidate_{expected}'
        assert job['config']['bot'] == config()['bot']
        assert job['optimizer_start']['bot_modified'] is True
        assert job['config']['optimize']['iters'] == 777


def test_ai_receives_new_exact_start_before_best_is_committed(record, monkeypatch):
    """The evaluation prompt specifies fresh populations without a winning starting bot."""
    store, row = record
    row['observer_jobs'] = [{'metrics': {'gain': 'HUMAN_ONLY_OBSERVER_RESULT'}}]
    winner = _warm_start_observation(row, 2)
    class AI(LoopAI):
        """Capture the real provider payload without using a provider."""
        async def preflight(self, owner, settings):
            """Return bounded local model metadata."""
            return {'output_limit': 16000}
        async def _request(self, current, metadata, content, output_tokens):
            """Verify the fresh seed while retaining the original input separately."""
            assert 'optimizer_start' not in content
            assert content['search_policy']['population'] == 'fresh_random'
            assert content['search_policy']['winner_use'] == 'comparison_only'
            assert content['hsl_mode'] == 'auto'
            assert content['initial_config'] == row['initial_config']
            assert content['loop_id'] == row['id']
            assert content['max_rounds'] == row['settings']['max_rounds']
            assert 'HUMAN_ONLY_OBSERVER_RESULT' not in json.dumps(content)
            return '{"winner_operation":null,"finish":true}', {'tokens': None, 'usd': None}
    asyncio.run(AI(store).decide(row, 'evaluate', {'best': winner}))


@pytest.mark.parametrize('change', ['bot', 'bounds', 'fixed', 'symbol'])
def test_positions_cannot_be_changed_indirectly(record, change):
    """User goal capacity applies to defaults, search bounds and all override layers."""
    _, row = record
    row['settings']['goals']['positions'] = 4
    candidate = config()
    overrides = {}
    if change == 'bot':
        candidate['bot']['long']['risk']['n_positions'] = 1
    elif change == 'bounds':
        candidate['optimize']['bounds'] = {'long': {'risk': {'n_positions': [1, 9]}}}
    elif change == 'fixed':
        candidate['optimize']['fixed_runtime_overrides'] = {'bot.long.risk.n_positions': 9}
    else:
        overrides = {'BTC': {'bot': {'long': {'risk': {'n_positions': 9}}}}}
    with pytest.raises(ValueError, match='position capacity'):
        validate_change(row, candidate, overrides)


def test_holdout_cannot_be_training(record):
    """Enabled editor still cannot use the independent final evaluation period."""
    _, row = record
    row['settings']['scenario_enabled'] = True
    row['holdouts'] = [{'start_date': '2020-02-01', 'end_date': '2020-03-01', 'exchanges': ['binance']}]
    with pytest.raises(ValueError, match='holdout'):
        validate_change(row, config(), {})


def test_knowledge_is_private_idempotent_and_recovers_index(record):
    """Full evidence survives index interruption and is isolated by owner."""
    store, row = record
    entry = {'coins': [], 'knowledge': 'Measured improvement', 'observations': []}
    store.remember(row, 'evidence', entry)
    store.remember(row, 'evidence', entry)
    assert len(store.knowledge(OWNER)['entries']) == 1
    assert store.knowledge('b' * 32)['entries'] == []
    assert store.path(OWNER, row['id']).stat().st_mode & 0o777 == 0o600
    (store.owner_dir(OWNER) / 'knowledge.json').unlink()
    store.remember(row, 'evidence', entry)
    assert len(store.context(row)) == 1


def test_unknown_goal_cannot_be_confirmed():
    """A qualitative rule with no target retains an unconfirmed result."""
    assert evaluate({'gain': 3}, RULE)['achieved']
    assert not evaluate({'gain': 3}, [dict(RULE[0], target=None)])['achieved']
    assert not evaluate({'gain': 3}, [dict(RULE[0], goal='custom', target=None)])['achieved']
    with pytest.raises(ValueError, match='omitted'):
        validate_rubric(RULE, {'presets': ['gain', 'drawdown']})
    with pytest.raises(ValueError):
        parse_object('{"gain":NaN}')


def test_native_queue_requires_live_controller(record, tmp_path):
    """A manual queue start cannot bypass loop controls or owner boundaries."""
    store, row = record
    data = {'loop_id': row['id'], 'loop_owner': OWNER}
    with pytest.raises(HTTPException):
        authorize_native_job(tmp_path, data, None)
    authorize_native_job(tmp_path, data, row['id'])
    store.update(OWNER, row['id'], lambda value: value.update(status='paused'))
    with pytest.raises(HTTPException):
        authorize_native_job(tmp_path, data, row['id'])


class FakeBackend:
    """Model detached jobs without launching real subprocesses or cloud leases."""
    def __init__(self):
        self.prepared = {}; self.starts = []; self.cleanups = []; self.stops = []

    def capacity(self, row):
        return 3

    def existing_candidates(self, record):
        """Ordinary mock configs have no saved optimizer evidence."""
        return []

    def prepare(self, row, job):
        return self.prepared.setdefault(job['operation'], {'id': job['operation'], 'execution': 'cpu'})

    def poll(self, row, job):
        return {'status': 'completed' if job['operation'] in self.starts else 'queued', 'started': job['operation'] in self.starts}

    def start(self, row, job):
        self.starts.append(job['operation'])
        return self.poll(row, job)

    def candidates(self, row, job):
        return [{'id': 'candidate_' + job['operation'], 'optimizer_job': job['operation'], 'config': job['config'], 'overrides': {}, 'metrics': {'gain': 1}}]

    def validation_config(self, row, candidate, holdout=False):
        return candidate['config']

    def observations(self, row, job):
        return {'metrics': {'gain': 1}, 'exact': True, 'assessment': {**evaluate({'gain': 1}, RULE), 'simulation_complete': True}}

    def stop(self, row, job):
        self.stops.append(job['operation'])

    def cleanup(self, row):
        self.cleanups.append(row['id'])


class FakeAI:
    """Deterministic decisions exercise autonomous subsequent cycles."""
    async def decide(self, row, stage, evidence):
        if stage == 'interpret':
            return {'rubric': RULE}
        if stage == 'select':
            return {'candidate_ids': [evidence['candidates'][0]['id']], 'reason': 'Validate the distinct candidate while preserving budget for future rounds'}
        changed = copy.deepcopy(row['initial_config']); changed['optimize']['iters'] = 777
        options = evidence.get('winner_options', {})
        winner = max(options, key=lambda key: options[key]['metrics']['gain']) if options else None
        return {'winner_operation': winner, 'finish': False, 'reason': 'Compare exact gain and try more iterations', 'knowledge': 'Gain below target', 'variants': [{'config': changed, 'overrides': {}}]}


@pytest.mark.parametrize('limit', [1, 2])
def test_hard_completed_round_ceiling_overrides_ai_continuation(record, tmp_path, limit):
    """A persistent AI request to continue cannot launch round N+1, even after restart."""
    store, row = record
    store.update(OWNER, row['id'], lambda current: current['settings'].update(max_rounds=limit, max_runs=200))
    backend = FakeBackend()
    seen = []
    class AI(FakeAI):
        """Capture exact progress and ensure user-only observer data stays private."""
        async def decide(self, current, stage, evidence):
            assert 'HUMAN_ONLY_HOLDOUT' not in json.dumps(evidence)
            if stage == 'evaluate':
                seen.append(evidence)
            return await super().decide(current, stage, evidence)
    store.update(OWNER, row['id'], lambda current: current.update(observer_jobs=[{'metrics': 'HUMAN_ONLY_HOLDOUT'}]))
    async def run():
        for _ in range(limit * 6 + 5):
            controller = LoopController(tmp_path, store=store, backend=backend, ai=AI())
            await controller.tick(OWNER, row['id'])
    asyncio.run(run())
    current = store.read(OWNER, row['id'])
    assert len(current['history']) == limit
    assert len([job for job in current['jobs'] if job['kind'] == 'optimizer']) == limit
    assert current['stop_reason'] == 'Maximum completed rounds reached'
    assert seen[-1]['remaining_rounds'] == 0
    assert seen[-1]['completed_rounds_after_evaluation'] == limit
    assert seen[-1]['loop_id'] == row['id']


@pytest.mark.parametrize('limit', [None, 0, -1, True, 1.5, '25'])
def test_round_ceiling_is_explicit_and_integral(limit):
    """Missing or malformed round authorization must fail at the API boundary."""
    values = dict(config_name='source', provider='chatgpt', model='test', max_runs=200)
    if limit is not None:
        values['max_rounds'] = limit
    with pytest.raises(ValueError):
        LoopStart(**values)


def test_one_hundred_rounds_are_allowed():
    """The ceiling is chosen by the user and is not hardcoded to 25."""
    assert LoopStart(config_name='source', provider='chatgpt', model='test', max_runs=200, max_rounds=100).max_rounds == 100


def test_legacy_queue_without_round_ceiling_cannot_start(record, tmp_path):
    """An old saved run must be configured explicitly instead of receiving an inferred limit."""
    store, row = record
    def legacy(current):
        current.update(status='queued')
        current['settings'].pop('max_rounds')
    store.update(OWNER, row['id'], legacy)
    controller = LoopController(tmp_path, store=store, backend=FakeBackend(), ai=FakeAI())
    with pytest.raises(ValueError, match='Maximum completed rounds'):
        controller.action(OWNER, row['id'], 'start')
    assert store.read(OWNER, row['id'])['status'] == 'queued'


@pytest.mark.parametrize('change,success', [({}, True), ({'proxy_evaluations': 99999}, False),
    ({'limit_reached': None}, False), ({'error': 'worker failed'}, False),
    ({'results_consumed': False}, False), ({'result_count': 0}, False), ({'status': 'failed'}, False)])
def test_ai_receives_clean_proxy_completion_only_with_collected_work(change, success):
    """Quota cancellation counts as success; user cancellation and missing results do not."""
    from pb8_loop_store import optimizer_execution_evidence
    job = dict(status='cancelled', limit_reached='proxy', proxy_evaluations=106496,
               results_consumed=True, result_count=1, error=None)
    job.update(change)
    evidence = optimizer_execution_evidence(job, dict(run_limit_mode='proxy', run_proxy=100000))
    assert (evidence['status'] == 'completed') is success
    assert evidence['backend_status'] == job['status']
    assert evidence['completed_evolution_proxy'] == job['proxy_evaluations']
    if success:
        assert evidence['completion_reason'] == 'proxy_limit_reached'
        assert evidence['proxy_overshoot'] == 6496


@pytest.mark.parametrize('selection', ['profitable', 'retain', 'unknown', 'violation', 'incomplete', 'missing', 'null'])
def test_ai_exact_winner_is_authoritative_but_cannot_bypass_validation(record, tmp_path, monkeypatch, selection):
    """Low legacy scores cannot overrule AI selection; invalid selections never advance."""
    store, row = record
    rules = [{'goal': 'gain', 'metrics': ['gain'], 'direction': 'max', 'target': None},
             {'goal': 'drawdown', 'metrics': ['drawdown'], 'direction': 'min', 'target': .5}]
    baseline = _warm_start_observation(row, 1, 1)
    baseline['operation'] = 'baseline_check'
    observations = []
    for name, gain, dd, complete in [('profitable', 2.6449, .4796, True),
                                      ('violation', 5, .6, True), ('incomplete', 10, .1, False)]:
        item = _warm_start_observation(row, gain, gain)
        item.update(operation=name, metrics={'gain': gain, 'drawdown': dd})
        item['assessment'] = {**evaluate(item['metrics'], rules), 'simulation_complete': complete,
                              'score': -1000 if name == 'profitable' else 1000}
        observations.append(item)
    def initialize(current):
        """Use only synthetic completed jobs and exact observations in temporary storage."""
        current.update(phase='evaluate', rubric=rules, baseline=baseline, observations=observations,
                       jobs=[{'operation': item['operation'], 'kind': 'validation', 'round': 0,
                              'status': 'completed'} for item in observations])
    row = store.update(OWNER, row['id'], initialize)
    class AI(FakeAI):
        """Choose explicitly without consulting the backend score."""
        async def decide(self, current, stage, evidence):
            assert set(evidence['winner_options']) == {'baseline_check', 'profitable'}
            decision = await super().decide(current, stage, evidence)
            if selection == 'missing':
                decision.pop('winner_operation')
            else:
                decision['winner_operation'] = {'retain': 'baseline_check', 'null': None}.get(selection, selection)
            return decision
    async def isolated(function, *args):
        """Run the fake backend and isolated store without external resources."""
        return function(*args)
    monkeypatch.setattr('pb8_loop_controller.owned_thread', isolated)
    controller = LoopController(tmp_path, store=store, backend=FakeBackend(), ai=AI())
    if selection in {'profitable', 'retain'}:
        asyncio.run(controller._tick(row))
        current = store.read(OWNER, row['id'])
        expected = 'profitable' if selection == 'profitable' else 'baseline_check'
        assert current['best']['operation'] == expected
        assert current['history'][0]['winner_operation'] == expected
        assert current['jobs'][-1]['optimizer_start']['candidate_id'] == current['best']['candidate']['id']
    else:
        with pytest.raises(ValueError, match='AI must|AI selected'):
            asyncio.run(controller._tick(row))
        assert not store.read(OWNER, row['id'])['history']


def test_goal_evidence_has_no_weighted_score():
    """A ceiling is checked without rewarding unused risk or computing a total score."""
    rules = [{'goal': 'drawdown', 'metrics': ['dd'], 'direction': 'min', 'target': .5}]
    low, high = [evaluate({'dd': dd}, rules) for dd in (.01, .49)]
    assert low['hard_targets_met'] and high['hard_targets_met']
    assert low['score'] is None and high['score'] is None


def test_candidate_evidence_is_not_shortlisted_by_goal_score(record, tmp_path, monkeypatch):
    """Every bounded native Pareto artifact reaches AI selection, including entries after twelve."""
    from api import optimize_v8 as opt
    store, row = record
    files = [tmp_path / f'{index}.json' for index in range(600)]
    monkeypatch.setattr(opt, '_resolve_result_path', lambda value: tmp_path)
    monkeypatch.setattr(opt, '_pareto_files_for_listing', lambda directory: (directory, files))
    monkeypatch.setattr(opt, '_read_json', lambda path: {**config(), 'metrics': {'gain': int(path.stem)}})
    monkeypatch.setattr(opt, '_pareto_summary', lambda data, reducer: data['metrics'])
    candidates = LoopBackend(tmp_path, store)._result_candidates(row,
        {'path': 'isolated', 'result': 'isolated', 'modified': 1},
        {'operation': 'optimizer', 'config': config(), 'overrides': {}})
    assert len(candidates) == 600
    assert candidates[-1]['metrics']['gain'] == 599
    assert all('guidance_score' not in candidate for candidate in candidates)


def test_invalid_gpu_variant_is_repaired_before_job_allocation(record, tmp_path, monkeypatch):
    """Native GPU errors return to AI repair without launching a doomed job."""
    store, row = record
    monkeypatch.setattr('pb8_loop_controller._log', lambda *args, **kwargs: None)
    async def isolated_operation(function, *args):
        """Run only the fake backend and temporary store in this policy test."""
        return function(*args)
    monkeypatch.setattr('pb8_loop_controller.owned_thread', isolated_operation)
    def bootstrap(value):
        """Capture an isolated GPU input before initial AI variant selection."""
        value['settings']['execution'] = 'vast'
        value['initial_config'] = valid_gpu_config()
        value.update(phase='bootstrap', rubric=RULE, bootstrap_candidates=[],
                     bootstrap={'status': 'evaluating', 'candidate_count': 0, 'sources': []})
    store.update(OWNER, row['id'], bootstrap)
    class AI(FakeAI):
        """Use the actionable native constraint in the next model response."""
        async def decide(self, current, stage, evidence):
            changed = copy.deepcopy(current['initial_config'])
            changed['optimize']['gpu'] = {'validate_per_generation': 32,
                                          'drift_window': 256 if evidence.get('repair_error') else 128}
            return {'variants': [{'config': changed, 'overrides': {}}]}
    backend = FakeBackend()
    controller = LoopController(tmp_path, store=store, backend=backend, ai=AI())
    asyncio.run(controller.tick(OWNER, row['id']))
    current = store.read(OWNER, row['id'])
    assert current['status'] == 'running' and current['phase'] == 'bootstrap'
    assert 'at least 256' in current['repair_error']
    assert current['jobs'] == [] and not backend.starts and not backend.prepared
    asyncio.run(controller.tick(OWNER, row['id']))
    current = store.read(OWNER, row['id'])
    assert current['phase'] == 'optimize' and current['errors'] == 0
    assert len(current['jobs']) == 1 and current['repair_error'] is None


@pytest.mark.parametrize('gpu, valid', [
    ({}, True),
    ({'validate_per_generation': 32, 'drift_window': 128}, False),
    ({'validate_per_generation': 32, 'drift_window': 256}, True),
    ({'validate_per_generation': 32, 'drift_probes': 1, 'drift_window': 256}, False),
    ({'validate_per_generation': 32, 'drift_probes': 1, 'drift_window': 384}, True),
    ({'drift_probes': 8}, False),
    ({'drift_probes': 0}, True),
    ({'drift_halt': 0}, False),
    ({'drift_min_samples': 256}, False),
])
def test_gpu_variant_drift_evidence_contract(record, gpu, valid):
    """Validate coordinated evidence windows before any native job is allocated."""
    from pb8_loop_store import validate_gpu_drift
    _store, row = record
    row['settings']['execution'] = 'vast'
    row['initial_config']['optimize']['backend'] = 'gpu'
    proposed = copy.deepcopy(row['initial_config'])
    proposed['optimize']['gpu'] = gpu
    if valid:
        validate_gpu_drift(proposed)
    else:
        with pytest.raises(ValueError, match='optimize.gpu'):
            validate_gpu_drift(proposed)


def test_autonomous_second_cycle_survives_controller_replacement(record, tmp_path):
    """Initial run, exact validation and adjusted next run proceed without approvals."""
    store, row = record
    backend = FakeBackend()
    async def run():
        controller = LoopController(tmp_path, store=store, backend=backend, ai=FakeAI())
        for _ in range(5):
            await controller.tick(OWNER, row['id'])
        current = store.read(OWNER, row['id'])
        assert current['round'] == 1
        assert current['jobs'][-1]['config']['optimize']['iters'] == 777
        replacement = LoopController(tmp_path, store=store, backend=backend, ai=FakeAI())
        await replacement.tick(OWNER, row['id'])
        assert len(backend.starts) == len(set(backend.starts)) == 3
        assert len(store.knowledge(OWNER)['entries']) == 1
    asyncio.run(run())


@pytest.mark.parametrize('target', [None, .5])
def test_explicit_additional_rounds_survive_early_ai_finish_and_repair(record, tmp_path, target):
    """Requested continuation rounds override optional finish without relaxing caps."""
    store, row = record
    store.update(OWNER, row['id'], lambda current: current.update(required_rounds=2))
    seen = []
    class AI(FakeAI):
        """Finish early once, then comply with the exact remaining-round request."""
        async def decide(self, current, stage, evidence):
            if stage == 'interpret':
                return {'rubric': [dict(RULE[0], target=target)]}
            if stage == 'evaluate':
                seen.append(evidence['required_rounds_remaining'])
                if not evidence.get('repair_error'):
                    decision = await super().decide(current, stage, evidence)
                    return {**decision, 'finish': True, 'variants': [], 'reason': 'A good candidate is already retained'}
            return await super().decide(current, stage, evidence)
    backend = FakeBackend()
    async def run():
        controller = LoopController(tmp_path, store=store, backend=backend, ai=AI())
        for _ in range(5):
            await controller.tick(OWNER, row['id'])
        current = store.read(OWNER, row['id'])
        assert current['status'] == 'running' and current['phase'] == 'evaluate'
        assert len(current['history']) == 1 and 'explicitly requested 2' in current['repair_error']
        for _ in range(5):
            await controller.tick(OWNER, row['id'])
        current = store.read(OWNER, row['id'])
        assert len(current['history']) == 2 and current['status'] in {'completed', 'unconfirmed'}
        assert current['errors'] == 0
        assert sum(job['kind'] == 'optimizer' for job in current['jobs']) == 2
    asyncio.run(run())
    assert seen == [1, 1, 0]


def test_late_ai_response_after_stop_cannot_launch(record, tmp_path):
    """Stop revokes a pending interpretation even if the provider replies later."""
    store, row = record
    backend = FakeBackend()
    async def run():
        entered, release = asyncio.Event(), asyncio.Event()
        class BlockingAI(FakeAI):
            async def decide(self, current, stage, evidence):
                entered.set(); await release.wait(); return {'rubric': RULE}
        controller = LoopController(tmp_path, store=store, backend=backend, ai=BlockingAI())
        task = asyncio.create_task(controller.tick(OWNER, row['id']))
        await entered.wait()
        controller.action(OWNER, row['id'], 'stop')
        release.set(); await task
        await controller.tick(OWNER, row['id'])
        assert store.read(OWNER, row['id'])['status'] == 'stopped'
        assert not backend.starts
    asyncio.run(run())


def test_invalid_acknowledged_json_can_be_repaired(record):
    """Malformed acknowledged replies do not become ambiguous billable requests."""
    store, row = record
    class AI(LoopAI):
        async def preflight(self, owner, settings):
            return {'output_limit': 16000}
        async def _request(self, *args):
            return 'invalid JSON', {'tokens': None, 'usd': None}
    with pytest.raises(ValueError):
        asyncio.run(AI(store).decide(row, 'interpret', {}))
    current = store.read(OWNER, row['id'])
    assert current['pending_ai'] is None
    assert current['ai_calls'] == 1
    assert current['ai_tokens_reserved'] > 0


def test_model_context_limit_prevents_request(record):
    """A model context overflow must fail before dispatching an external request."""
    store, row = record
    class AI(LoopAI):
        async def preflight(self, owner, settings):
            return {'loop_profile': {'context': 1, 'page_tokens': 1500}}
        async def _request(self, *args):
            pytest.fail('Context overflow must reject before transport')
    with pytest.raises(LoopAIPause, match='context budget'):
        asyncio.run(AI(store).decide(row, 'interpret', {}))


def test_vast_workers_cannot_claim_unrelated_loop_jobs(tmp_path):
    """Ordinary and individual loop pools have disjoint pending-job sets."""
    from vast_jobs import JobStore, write_json
    from vast_queue import CloudQueue
    from secure_files import ensure_private_directory
    queue = CloudQueue(JobStore(tmp_path / 'vast'))
    for key, loop in [('a' * 32, None), ('b' * 32, '1' * 32), ('c' * 32, '2' * 32)]:
        directory = ensure_private_directory(queue.root / 'jobs' / key)
        write_json(directory / 'state.json', {'id': key, 'kind': 'job', 'status': 'ready', 'rental_state': 'none', 'created_at': 0, 'loop_id': loop})
    assert [job['id'] for job in queue.waiting()] == ['a' * 32]
    assert [job['id'] for job in queue.waiting('1' * 32)] == ['b' * 32]


def test_three_vast_slots_use_frozen_budget_and_reject_fourth(tmp_path, monkeypatch):
    """One GPU per owned rental, with all starting leases consuming shared slots."""
    import vast_queue
    from api.vast import RentalPreferences
    from vast_jobs import JobStore, digest, write_json
    from secure_files import ensure_private_directory
    from vast_provider import VastError
    from vast_direction_fixtures import direction_config
    store = LoopStore(tmp_path / 'data/loop_optimizer')
    preferences = RentalPreferences(gpu_name='RTX 3060', budget=5, max_rentals=3).model_dump()
    row = store.create(OWNER, settings(execution='vast', vast=preferences), valid_gpu_config(), {}, {'pb8': 'commit'}, [], config()['backtest'])
    jobs = JobStore(tmp_path / 'vast'); queue = vast_queue.CloudQueue(jobs)
    queue.update(gpu_preferences=preferences)
    directory = ensure_private_directory(jobs.root / 'jobs' / ('b' * 32))
    write_json(directory / 'state.json', {'id': 'b' * 32, 'kind': 'job', 'status': 'ready', 'rental_state': 'none', 'workers': 1, 'created_at': 1, 'loop_id': row['id'], 'loop_owner': OWNER})
    folder = ensure_private_directory(directory / 'input')
    write_json(folder / 'optimize.json', direction_config())
    write_json(folder / 'manifest.json', {'config_sha256': digest(folder / 'optimize.json')})
    monkeypatch.setattr(vast_queue, 'PROJECT', tmp_path)
    monkeypatch.setattr(vast_queue, 'preflight_local_metadata', lambda *args: None)
    def start(identifier, offer, hours, budget):
        assert budget == 5
        return jobs.update(identifier, rental_state='create_pending')
    monkeypatch.setattr(jobs, 'start', start)
    offer = {'id': 1, 'machine_id': 101, 'gpu_name': 'RTX 3060', 'num_gpus': 1}
    kwargs = {'loop_id': row['id'], 'loop_owner': OWNER, 'loop_limits': preferences}
    workers = [queue.start(offer, preferences['hours'], 5, -1, **kwargs) for _ in range(3)]
    assert len({worker['id'] for worker in workers}) == 3
    assert all(worker['loop_owner'] == OWNER for worker in workers)
    with pytest.raises(VastError, match='limits changed'):
        queue.start(offer, preferences['hours'], 5, -1, **kwargs)
    assert queue.read().get('worker_id') is None


def test_holdout_is_reserved_once_across_concurrent_loops(record):
    """Loops created together cannot both call the same window independent evidence."""
    store, row = record
    row['holdouts'] = [{'start_date': '2021-01-01', 'end_date': '2021-02-01'}]
    assert store.claim_holdouts(row)
    assert not store.claim_holdouts(dict(row, id='c' * 32))


def test_seed_and_checkpoint_options_are_not_silently_replaced(tmp_path, monkeypatch):
    """Native runtime options distinguish population seeding from exact resume."""
    from api import optimize_v8 as opt
    monkeypatch.setattr(opt, '_validate_pareto_seed_source', lambda source: source)
    value = config(); value['pbgui']['optimize_runtime'] = {'mode': 'pareto_seed', 'source': '__self__'}
    assert opt._runtime_options_from_config(value)['mode'] == 'pareto_seed'
    with pytest.raises(HTTPException):
        opt._runtime_options_from_config(dict(value, pbgui={'optimize_runtime': {'mode': 'checkpoint_resume', 'source': '../outside'}}))


def test_evaluation_wire_format_retains_all_reports_and_lossless_configs():
    """Remove repeated evidence without a score shortlist or missing measurements."""
    from pb8_loop_ai import compact_evaluation_content
    initial = {'shared': 'x' * 20_000, 'bot': {'long': {'ema': 10}}, 'literal.key': {'remove': 1}}
    observations = []
    options = {}
    for index in range(6):
        candidate_config = copy.deepcopy(initial)
        candidate_config['bot']['long']['ema'] += index
        candidate_config['literal.key'].pop('remove')
        report = {'gain': 1.25 + index, 'unrelated_but_retained': 123.5}
        observation = {'operation':str(index), 'candidate':{'id':str(index), 'config':candidate_config, 'metrics':{'optimizer_gain':index}},
                       'metrics':report, 'per_report':[copy.deepcopy(report) for _ in range(8)]}
        observation['per_report'][1].pop('unrelated_but_retained')
        observation['per_report'][2]['unrelated_but_retained'] = None
        observations.append(observation)
        options[str(index)] = {'candidate_id':str(index), 'metrics':report, 'per_report':observation['per_report']}
    original = {'initial_config':initial, 'rubric':[{'metrics':['gain']}], 'evaluation_contract':{},
                'knowledge':[{'observations':[{'metrics':{'gain':1.5,'unused_old_metric':7}}]}],
                'evidence':{'observations':observations,'winner_options':options,'baseline':None,'best':None}}
    before = copy.deepcopy(original)
    wire = compact_evaluation_content(original)
    assert original == before
    assert set(wire['evidence']['winner_options']) == set(options)
    assert len(wire['evidence']['observations']) == 6
    for old, new in zip(observations, wire['evidence']['observations']):
        assert len(new['per_report']) == len(old['per_report']) and new['metrics'] == old['metrics']
        table = new['per_report_extra']
        reports = [{**report, **{key: table['rows'][index][column] for column, key in enumerate(table['columns'])
                                if column not in table['missing'].get(str(index), [])}}
                   for index, report in enumerate(new['per_report'])]
        assert reports == old['per_report']
        assert new['candidate']['metrics'] == old['candidate']['metrics']
        rebuilt = copy.deepcopy(initial)
        for change in new['candidate']['config_from_initial']['set']:
            parent = rebuilt
            for segment in change['path'][:-1]:
                parent = parent[segment]
            parent[change['path'][-1]] = change['value']
        for path in new['candidate']['config_from_initial']['remove']:
            parent = rebuilt
            for segment in path[:-1]:
                parent = parent[segment]
            del parent[path[-1]]
        assert rebuilt == old['candidate']['config']
    assert wire['knowledge'][0]['observations'][0]['metrics'] == {'gain':1.5}
    assert len(json.dumps(wire)) < len(json.dumps(original)) / 2


def test_round_archive_accepts_multiple_bounded_exact_observations(record):
    """Six real-sized reports may exceed the config limit without losing evidence."""
    store, row = record
    entry = {'status': 'observed_exact', 'observations': [{'detail': 'x' * 120_000, 'operation': str(i)} for i in range(6)]}
    store.remember(row, 'large-round', entry)
    saved = store.knowledge(row['owner'])['entries'][-1]
    assert len(saved['observations']) == 6
    assert saved['observations'][5]['detail'] == entry['observations'][5]['detail']
    store.remember(row, 'large-round', entry)
    assert len(store.knowledge(row['owner'])['entries']) == 1
    with pytest.raises(ValueError, match='Credentials'):
        store.remember(row, 'secret-round', {'observations': [{'api_key': 'forbidden'}]})
    with pytest.raises(ValueError, match='512 KB'):
        store.remember(row, 'oversize-observation', {'observations': [{'detail': 'x' * (513 * 1024)}]})


@pytest.mark.parametrize('suffix,comparable', [('_usd', True), ('_btc', False)])
def test_exact_smoothness_legacy_names_use_only_native_usd_metrics(record, tmp_path, monkeypatch, suffix, comparable):
    """Unqualified legacy names retain quote-equity semantics, never BTC/proxies."""
    from api import backtest_v8 as bt
    store, row = record
    names = ('exponential_fit_error', 'equity_choppiness', 'equity_jerkiness')
    row['rubric'] = [{'goal':'uptrend', 'metrics':[name], 'direction':'min', 'target':None} for name in names]
    report = {name + suffix: i + .25 for i, name in enumerate(names)} | {'backtest_completion_ratio':1}
    path = tmp_path / 'analysis.json'
    path.write_text(json.dumps(report))
    monkeypatch.setattr(bt, '_result_analysis_paths', lambda name: [path])
    monkeypatch.setattr(bt, '_read_json', lambda path: json.loads(path.read_text()))
    result = LoopBackend(tmp_path, store).observations(row, {'name':'job'})
    assert result['assessment']['comparable'] is comparable
    if comparable:
        assert all(result['per_report'][0][name] == report[name + suffix] for name in names)


def test_fixed_metric_aggregation_requires_each_report_to_pass(record, tmp_path, monkeypatch):
    """A good first scenario cannot hide a failing second scenario."""
    from api import backtest_v8 as bt
    store, row = record
    row['rubric'] = RULE
    paths = []
    for index, gain in enumerate([3, 1]):
        path = tmp_path / f'analysis-{index}.json'; path.write_text(json.dumps({'gain': gain, 'backtest_completion_ratio': 1})); paths.append(path)
    monkeypatch.setattr(bt, '_result_analysis_paths', lambda name: paths)
    monkeypatch.setattr(bt, '_read_json', lambda path: json.loads(path.read_text()))
    result = LoopBackend(tmp_path, store).observations(row, {'name': 'job'})
    assert result['metrics']['gain'] == 2
    assert not result['assessment']['achieved']


def test_owner_api_only_lists_own_loops(record, tmp_path, monkeypatch):
    """The loop API derives owner identity from authentication, never request data."""
    from api import loop_optimizer_v8 as api
    store, row = record
    store.create('b' * 32, settings(), config(), {}, {}, [], config()['backtest'])
    monkeypatch.setattr(api, '_controller', LoopController(tmp_path, store=store, backend=FakeBackend(), ai=FakeAI()))
    monkeypatch.setattr(api, 'owner_key', lambda user: OWNER)
    response = asyncio.run(api.list_loops(SimpleNamespace(user_id='user')))
    assert [loop['id'] for loop in json.loads(response.body)['loops']] == [row['id']]


def test_paid_ai_usage_is_recorded_and_reconciled(record):
    """A paid API call reserves output cost and reconciles acknowledged token usage."""
    store, row = record
    row = store.update(OWNER, row['id'], lambda value: value['settings'].update(provider='opencode-zen', ai_budget_confirmed=True, ai_decision_token_limit=10_000_000, ai_decision_usd_limit=10))
    class AI(LoopAI):
        async def preflight(self, owner, settings):
            return {'cost': {'input':1,'output':2}, 'output_limit':16000, 'protocol':'chat'}
        async def _request(self, *args):
            return json.dumps({'rubric': [dict(RULE[0], goal=key) for key in set(self.store.read(OWNER, row['id'])['settings']['goals']['presets']) | ({'custom'} if self.store.read(OWNER, row['id'])['settings']['goals'].get('text') else set())]}), {'tokens':{'input_tokens':100,'output_tokens':200},'usd':None}
    asyncio.run(AI(store).decide(row, 'interpret', {}))
    current = store.read(OWNER, row['id'])
    assert current['ai_usd_reserved'] == pytest.approx(.0005)
    assert current['last_usage']['usd'] == pytest.approx(.0005)
    assert current['pending_ai'] is None
    assert current['last_ai_decision']['round'] == row['round']


def test_unacknowledged_ai_request_retains_budget_and_cannot_repeat(record):
    """A cancelled/interrupted task must retain its unconfirmed request reservation."""
    store, row = record
    class AI(LoopAI):
        async def preflight(self, owner, settings):
            return {}
        async def _request(self, *args):
            raise asyncio.CancelledError()
    ai = AI(store)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(ai.decide(row, 'interpret', {}))
    current = store.read(OWNER, row['id'])
    assert current['pending_ai']
    with pytest.raises(LoopAIPause, match='interrupted'):
        asyncio.run(ai.decide(current, 'interpret', {}))
    assert store.read(OWNER, row['id'])['ai_calls'] == 1


@pytest.mark.parametrize('metadata,expected', [
    ({'output_limit': 65536}, 65536),
    ({}, 32768),
    ({'output_limit': 65536, 'context': 50000}, None),
])
def test_loop_output_uses_model_allowance_and_context(record, metadata, expected):
    """Full config decisions use the catalog output allowance without overrunning context."""
    store, row = record
    dispatched = []
    class AI(LoopAI):
        """Capture output budgets without a provider call."""
        async def preflight(self, owner, settings):
            return {**metadata, 'loop_profile': {**row['ai_model_profile'], **({'context': metadata['context']} if metadata.get('context') else {})}}
        async def _request(self, current, model, content, output_tokens):
            dispatched.append(output_tokens)
            return json.dumps({'rubric': RULE}), {'tokens': None, 'usd': None}
    if expected is None:
        with pytest.raises(LoopAIPause):
            asyncio.run(AI(store).decide(row, 'interpret', {}))
        assert not dispatched
        return
    asyncio.run(AI(store).decide(row, 'interpret', {}))
    if expected is not None:
        assert dispatched == [expected]
    else:
        assert 0 < dispatched[0] < metadata['output_limit']
        assert store.read(OWNER, row['id'])['ai_tokens_reserved'] <= metadata['context']


def _loop_chat_for_payloads(payloads):
    """Feed native protocol replies through isolated HTTP contexts with no credentials."""
    from ai_chat import AIChatService
    class Response:
        """Simulate the HTTP context only."""
        status = 200
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            return False
    class Session:
        """No remote requests and no enabled tools."""
        def post(self, url, **kwargs):
            assert len(kwargs['json']['tools']) == 1
            return Response()
    class Chat:
        """Reuse production text extraction against deterministic replies."""
        credentials = SimpleNamespace(load_go_key=lambda owner: 'test-only-key')
        _response_text = staticmethod(AIChatService._response_text)
        _chat_completion_text = staticmethod(AIChatService._chat_completion_text)
        _messages_text = staticmethod(AIChatService._messages_text)
        async def _http_session(self):
            return Session()
        async def _read_json_response(self, *args, **kwargs):
            return payloads.pop(0)
    return Chat()


@pytest.mark.parametrize('protocol,payload,error', [
    ('chat', {'choices': [{'finish_reason': 'length'}]}, 'output limit'),
    ('chat', {'choices': [{'finish_reason': 'tool_calls', 'message': {'tool_calls': [{'id': 'call'}]}}]}, 'requested a tool'),
    ('chat', {'choices': [{'finish_reason': 'stop', 'message': {'function_call': {'name': 'execute'}}}]}, 'requested a tool'),
    ('chat', {'choices': [{'finish_reason': 'content_filter'}]}, 'content filter'),
    ('responses', {'status': 'incomplete', 'incomplete_details': {'reason': 'max_output_tokens'}}, 'output limit'),
    ('responses', {'status': 'completed', 'output': [{'type': 'function_call'}]}, 'requested a tool'),
    ('responses', {'status': 'failed'}, 'before completion'),
    ('messages', {'stop_reason': 'max_tokens'}, 'output limit'),
    ('messages', {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use'}]}, 'requested a tool'),
    ('messages', {'stop_reason': 'refusal'}, 'declined'),
    ('chat', {'model': 'other-model', 'choices': [{'finish_reason': 'stop', 'message': {'content': '{}'}}]}, 'pinned model'),
    ('chat', {'error': {'message': 'provider-specific detail'}}, 'provider returned an error'),
])
def test_acknowledged_native_rejections_clear_pending_and_record_usage(record, protocol, payload, error):
    """Rejected replies are accounted for and repairable, never ambiguous duplicate requests."""
    store, row = record
    row = store.update(OWNER, row['id'], lambda value: value['settings'].update(provider='opencode-zen', effort='', ai_budget_confirmed=True, ai_decision_token_limit=10_000_000, ai_decision_usd_limit=10))
    payload = copy.deepcopy(payload)
    payload['usage'] = {'prompt_tokens': 100, 'completion_tokens': 200}
    class AI(LoopAI):
        """Retain the real native dispatch and response-validation path."""
        async def preflight(self, owner, settings):
            return {'protocol': protocol, 'cost': {'input': 1, 'output': 2}, 'output_limit': 32768}
    ai = AI(store, _loop_chat_for_payloads([payload]))
    with pytest.raises(LoopAIResponseError, match=error):
        asyncio.run(ai.decide(row, 'interpret', {}))
    current = store.read(OWNER, row['id'])
    assert current['pending_ai'] is None
    assert current['ai_calls'] == 1
    assert current['ai_usd_reserved'] == pytest.approx(.0005)
    assert current['last_usage']['tokens'] == payload['usage']
    assert not current.get('last_ai_decision')
    assert current['jobs'] == []


def test_incomplete_native_reply_enters_controller_repair_before_launch(record, tmp_path):
    """A completed HTTP reply must not prematurely fail the loop or launch invalid work."""
    store, row = record
    row = store.update(OWNER, row['id'], lambda value: value['settings'].update(provider='opencode-go', effort=''))
    replies = [{'choices': [{'finish_reason': 'length'}], 'usage': {'completion_tokens': 32768}},
               {'choices': [{'finish_reason': 'stop', 'message': {'content': json.dumps({'rubric': RULE})}}]}]
    class AI(LoopAI):
        """Mock preflight, preserving production acknowledgement and controller repair."""
        async def preflight(self, owner, settings):
            return {'protocol': 'chat', 'output_limit': 32768}
    backend = FakeBackend()
    controller = LoopController(tmp_path, store=store, backend=backend, ai=AI(store, _loop_chat_for_payloads(replies)))
    asyncio.run(controller.tick(OWNER, row['id']))
    current = store.read(OWNER, row['id'])
    assert current['status'] == 'running'
    assert current['pending_ai'] is None
    assert 'output limit' in current['repair_error']
    assert current['jobs'] == [] and not backend.starts
    asyncio.run(controller.tick(OWNER, row['id']))
    current = store.read(OWNER, row['id'])
    assert current['status'] == 'running' and current['phase'] == 'optimize', current.get('last_error')
    assert current['rubric'] == RULE and current['ai_calls'] == 2
    assert current['last_error'] is None and current['repair_error'] is None and current['errors'] == 0
    assert not backend.starts


def test_repeated_incomplete_bootstrap_replies_end_with_consistent_status(record, tmp_path):
    """Output repair remains bounded and the initial evaluation cannot look active after failure."""
    store, row = record
    def bootstrap(current):
        current['settings'].update(provider='opencode-go', effort='')
        current.update(phase='bootstrap', rubric=RULE, bootstrap_candidates=[{'id':'historical', 'optimizer_job':'saved', 'config':current['initial_config'], 'metrics':{'gain':1}, 'source':{'run':'saved','path':'saved','modified':1}}],
                       bootstrap={'status': 'evaluating', 'candidate_count': 0})
    row = store.update(OWNER, row['id'], bootstrap)
    replies = [{'choices': [{'finish_reason': 'length'}]} for _ in range(3)]
    class AI(LoopAI):
        """Use native response validation on three rejected acknowledged replies."""
        async def preflight(self, owner, settings):
            return {'protocol': 'chat', 'output_limit': 32768}
    backend = FakeBackend()
    controller = LoopController(tmp_path, store=store, backend=backend, ai=AI(store, _loop_chat_for_payloads(replies)))
    for _ in range(3):
        asyncio.run(controller.tick(OWNER, row['id']))
    current = store.read(OWNER, row['id'])
    assert current['status'] == 'failed' and current['errors'] == 3, current.get('last_error')
    assert current['pending_ai'] is None
    assert current['bootstrap']['status'] == 'failed'
    assert current['bootstrap']['reason'] == current['reason']
    assert current['ai_calls'] == 3 and not backend.starts


def test_invalid_metrics_give_the_model_actionable_repair_details():
    """Identify the failed goal and required shape without weakening rubric validation."""
    rubric = [dict(RULE[0], metrics={'gain': 'explanation'})]
    with pytest.raises(ValueError, match="goal 'gain'.*JSON list"):
        validate_rubric(rubric, {'presets': ['gain']})


def test_invalid_config_preparation_cannot_start_next_cycle(record, tmp_path):
    """Invalid preparation cannot bypass the required exact-backtest gate."""
    store, row = record
    class Backend(FakeBackend):
        def prepare(self, current, job):
            if current['round'] == 0:
                raise ValueError('Invalid initial backend setting')
            return super().prepare(current, job)
    async def run():
        backend = Backend(); controller = LoopController(tmp_path, store=store, backend=backend, ai=FakeAI())
        await controller.tick(OWNER, row['id'])
        await controller.tick(OWNER, row['id'])
        assert store.read(OWNER, row['id'])['phase'] == 'evaluate'
        await controller.tick(OWNER, row['id'])
        assert store.read(OWNER, row['id'])['round'] == 0
        assert store.read(OWNER, row['id'])['status'] == 'failed'
        assert not backend.starts
    asyncio.run(run())


def test_changed_input_stops_new_launch(record, tmp_path, monkeypatch):
    """Different datasets are never mixed into a claimed comparison."""
    store, row = record
    backend = LoopBackend(tmp_path, store)
    monkeypatch.setattr(backend, 'capacity', lambda current:1)
    monkeypatch.setattr(backend, 'fingerprint', lambda current:{'pb8':'new commit','data':'shards'})
    monkeypatch.setattr(backend, 'poll', lambda *args:pytest.fail('Changed inputs must reject before native launch'))
    with pytest.raises(ValueError, match='changed'):
        backend.start(row, {'kind':'optimizer','config':config()})
    assert store.read(OWNER, row['id'])['status'] == 'unconfirmed'


@pytest.mark.parametrize('kind', ['validation', 'holdout', 'observer_holdout', 'observer_full_range'])
def test_local_backtest_start_ignores_cloud_optimizer_workers(record, tmp_path, monkeypatch, kind):
    """Cloud worker counts cannot block native local backtest slots or mutate configs."""
    from api import optimize_v8, backtest_v8
    import psutil
    store, row = record
    backend = LoopBackend(tmp_path, store)
    candidate=config()
    candidate['optimize']['n_cpus']=23
    job={'kind':kind, 'config':candidate}
    monkeypatch.setattr(optimize_v8, '_load_queue', lambda: [])
    monkeypatch.setattr(backtest_v8, '_load_queue', lambda: [])
    monkeypatch.setattr(psutil, 'cpu_count', lambda: 16)
    monkeypatch.setattr(psutil, 'virtual_memory', lambda: type('Memory', (), {'available':1024**3})())
    monkeypatch.setattr(backend, '_start_locked', lambda current, native: {'status':'running'})
    monkeypatch.setattr(backend, 'poll', lambda *args: pytest.fail('Local backtest was blocked by cloud CPU count'))
    assert backend.start(row, job)['status']=='running'
    assert candidate['optimize']['n_cpus']==23
    assert store.read(OWNER, row['id'])['initial_config']['optimize']['n_cpus']==1


def test_pause_blocks_new_native_and_cloud_starts(record, tmp_path):
    """A controller replacement honors durable pause before preparing any work."""
    store, row = record
    backend = FakeBackend()
    controller = LoopController(tmp_path, store=store, backend=backend, ai=FakeAI())
    asyncio.run(controller.tick(OWNER, row['id']))
    controller.action(OWNER, row['id'], 'pause')
    asyncio.run(controller.tick(OWNER, row['id']))
    assert not backend.prepared and not backend.starts


def test_failed_attempt_consumes_run_limit(record, tmp_path):
    """A started failed optimizer is counted and cannot create a further attempt."""
    store, row = record
    row = store.update(OWNER, row['id'], lambda value:value['settings'].update(max_runs=1, max_rounds=1))
    backend = FakeBackend()
    controller = LoopController(tmp_path, store=store, backend=backend, ai=FakeAI())
    job = controller.job(row,'optimizer',config(),{},'Failed actual attempt')
    job.update(status='failed', started=True)
    store.update(OWNER, row['id'], lambda value:value.update(phase='evaluate',jobs=[job],rubric=RULE))
    asyncio.run(controller.tick(OWNER, row['id']))
    current = store.read(OWNER, row['id'])
    assert current['status'] == 'failed'
    assert len(current['jobs']) == 1


def test_manual_backtest_restart_cannot_bypass_loop_limits(tmp_path, monkeypatch):
    """Reject before stopping/resetting an owned validation job's execution evidence."""
    from api import backtest_v8 as bt
    from file_lock import advisory_file_lock
    path = tmp_path / 'job.json'; path.write_text(json.dumps({'loop_id':'a'*32}))
    monkeypatch.setattr(bt, '_queue_file', lambda name:path)
    monkeypatch.setattr(bt, '_queue_lock', lambda:advisory_file_lock(tmp_path / '.queue'))
    monkeypatch.setattr(bt, '_read_json', lambda file:json.loads(file.read_text()))
    monkeypatch.setattr(bt, '_terminate_verified', lambda name:pytest.fail('An owned job must not be reset by manual restart'))
    with pytest.raises(HTTPException) as exc:
        bt.restart_queue_item('job', None)
    assert exc.value.status_code == 409


@pytest.mark.parametrize('direction', ['long', 'short', 'both'])
def test_use_config_preserves_initial_direction(record, direction):
    """An inherited direction keeps disabled risk values and rejects reactivation."""
    from pb8_loop_store import configured_direction
    store, row = record
    initial = config()
    for side in ('long', 'short'):
        if direction not in {'both', side}:
            initial['bot'][side]['risk']['total_wallet_exposure_limit'] = 0
    row['initial_config'] = initial
    assert row['settings']['goals']['direction'] == 'config'
    assert configured_direction(initial) == direction
    assert validate_change(row, copy.deepcopy(initial), {}) == initial
    if direction != 'both':
        changed = copy.deepcopy(initial)
        disabled = 'short' if direction == 'long' else 'long'
        changed['bot'][disabled]['risk']['total_wallet_exposure_limit'] = 1
        with pytest.raises(ValueError, match='Use config'):
            validate_change(row, changed, {})


def test_config_direction_uses_bounds_and_fixed_overrides():
    """The optimizer's effective direction includes tunable and fixed risk values."""
    from pb8_loop_store import configured_direction
    value = config()
    value['bot']['short']['risk']['total_wallet_exposure_limit'] = 0
    assert configured_direction(value) == 'long'
    value['optimize']['bounds'] = {'short': {'risk': {'total_wallet_exposure_limit':[0,1]}}}
    assert configured_direction(value) == 'both'
    value['optimize']['fixed_runtime_overrides'] = {'bot.short.risk.total_wallet_exposure_limit':0}
    assert configured_direction(value) == 'long'


@pytest.mark.parametrize('direction', ['long', 'short', 'both'])
@pytest.mark.parametrize('execution', ['cpu', 'vast'])
def test_initial_use_config_does_not_rewrite_direction_settings(tmp_path, monkeypatch, direction, execution):
    """Use config retains the user's risk defaults and search bounds verbatim."""
    from api import optimize_v8 as opt
    import pb8_config
    initial = valid_gpu_config() if execution == 'vast' else config()
    initial['config_version'] = 'v8.6.0'
    for side in ('long', 'short'):
        if direction not in {'both', side}:
            initial['bot'][side]['risk']['total_wallet_exposure_limit'] = 0
            if execution == 'vast':
                initial['bot'][side]['risk']['n_positions'] = 0
                initial['optimize']['bounds'][side+'_total_wallet_exposure_limit'] = [0, 0]
                initial['optimize']['bounds'][side+'_n_positions'] = [0, 0]
    path = tmp_path / 'source.json'; path.write_text(json.dumps(initial))
    monkeypatch.setattr(opt, '_config_file', lambda name:path)
    monkeypatch.setattr(opt, 'get_config', lambda name,session=None:{'config':copy.deepcopy(initial),'override_configs':{}})
    monkeypatch.setattr('pb8_loop_backend.migrate_loop_bundle',lambda value:copy.deepcopy(value))
    monkeypatch.setattr(opt, '_load_override_payloads', lambda *args:{})
    monkeypatch.setattr(opt, 'pb8_runtime_status', lambda:{'ready':True})
    backend = LoopBackend(tmp_path, LoopStore(tmp_path/'loops'))
    monkeypatch.setattr(backend, 'fingerprint', lambda value:{})
    prepared, *_ = backend.initial('source', settings(execution=execution))
    assert prepared['bot'] == initial['bot']
    assert prepared['optimize']['bounds'] == initial['optimize']['bounds']
    assert prepared['optimize']['backend'] == ('gpu' if execution == 'vast' else 'pymoo')


@pytest.mark.parametrize('protocol,wire_key', [('chat','reasoning_effort'), ('responses','reasoning')])
def test_shared_reasoning_reaches_native_loop_request(record, protocol, wire_key):
    """The inherited assistant effort must reach each native provider wire format."""
    from ai_chat import AIChatService
    store, row = record
    row['settings'].update(provider='opencode-go', effort='high', service_tier='')
    sent = {}
    class Response:
        """Isolated HTTP context without a real provider connection."""
        status = 200
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            return False
    class Session:
        """Capture the actual outgoing native request body."""
        def post(self, url, **kwargs):
            sent.update(kwargs['json'])
            return Response()
    class Chat:
        """Supply only mocked credentials and transport helpers."""
        credentials = SimpleNamespace(load_go_key=lambda owner: 'test-only-key')
        _response_text = staticmethod(AIChatService._response_text)
        _chat_completion_text = staticmethod(AIChatService._chat_completion_text)
        async def _http_session(self):
            return Session()
        async def _read_json_response(self, *args, **kwargs):
            return {'status':'completed', 'output':[{'type':'message','content':[{'type':'output_text','text':'{}'}]}],
                    'choices':[{'finish_reason':'stop','message':{'content':'{}'}}]}
    metadata = {'protocol':protocol, 'reasoning_variants':[{'id':'high','type':'effort','value':'high'}]}
    text, _ = asyncio.run(LoopAI(store, Chat())._request(row, metadata, {}, 16384))
    assert text == '{}'
    assert sent[wire_key] == ('high' if protocol == 'chat' else {'effort':'high','summary':'auto'})


def test_ai_usage_is_not_a_separate_loop_stop_limit(record):
    """Prior call/token/USD totals do not create the removed extra loop stop limits."""
    store, row = record
    def prior_usage(value):
        value.update(ai_calls=1000, ai_tokens_reserved=10_000_000, ai_usd_reserved=100)
    row = store.update(OWNER, row['id'], prior_usage)
    class AI(LoopAI):
        """Isolated acknowledged decision transport."""
        async def preflight(self, owner, settings):
            return {}
        async def _request(self, *args):
            return json.dumps({'rubric': RULE}), {}
    assert asyncio.run(AI(store).decide(row, 'interpret', {})) == {'rubric': RULE}
    assert store.read(OWNER, row['id'])['ai_calls'] == 1001


@pytest.mark.parametrize('requested,expected', [(None,12), (8,8), (12,12), (24,None)])
def test_vast_loop_duration_inherits_and_obeys_saved_rental_limit(tmp_path, monkeypatch, requested, expected):
    """Vast start uses current preferences and rejects oversized requests before creation."""
    import api.loop_optimizer_v8 as api_loop
    import vast_queue
    import vast_credentials
    created = []
    class Queue:
        """Read isolated rental preferences without runtime state or real rentals."""
        root = tmp_path
        def read(self):
            return {'gpu_preferences': {'hours':12, 'budget':5, 'max_rentals':3}}
    async def create(owner, settings):
        created.append(settings)
        return settings
    monkeypatch.setattr(vast_queue, 'CloudQueue', Queue)
    monkeypatch.setattr(vast_credentials, 'VastCredentialStore', lambda root: SimpleNamespace(metadata=lambda: {'configured':True}))
    monkeypatch.setattr(api_loop, 'get_ai_chat_service', lambda: SimpleNamespace(get_preferences=lambda owner: {'jev_max_cost_usd':.01}))
    monkeypatch.setattr(api_loop, '_controller', SimpleNamespace(create=create))
    monkeypatch.setattr(api_loop, 'projection', lambda row: row)
    monkeypatch.setattr(api_loop, '_log', lambda *args, **kwargs: None)
    values = {'max_rounds':10, 'config_name':'source', 'provider':'chatgpt','model':'pinned','execution':'vast','authorization':True}
    if requested is not None:
        values['hours'] = requested
    body = LoopStart(**values)
    session = SimpleNamespace(user_id='isolated-user')
    if expected is None:
        with pytest.raises(HTTPException, match='Rental & Automation') as error:
            asyncio.run(api_loop.start_loop(body, session))
        assert error.value.status_code == 422
        assert not created
    else:
        response = asyncio.run(api_loop.start_loop(body, session))
        assert response.status_code == 201
        assert created[0]['hours'] == expected
        assert created[0]['vast']['hours'] == 12


@pytest.mark.parametrize('values', [
    {'run_limit_mode':'iters'}, {'run_limit_mode':'hours'},
    {'run_limit_mode':'proxy', 'run_proxy':100},
    {'run_limit_mode':'hours','run_hours':25},
    {'execution':'vast','run_limit_mode':'iters','run_iters':255},
])
def test_invalid_per_run_limits_are_rejected(values):
    """Missing, incompatible and oversized per-run limits cannot enter controller state."""
    with pytest.raises(ValueError):
        LoopStart(max_rounds=10, config_name='source', provider='chatgpt', model='pinned', **values)


@pytest.mark.parametrize('mode,value', [('iters',500), ('hours',1), ('proxy',1000)])
def test_explicit_run_limits_apply_to_every_ai_variant(record, tmp_path, mode, value):
    """AI iteration changes cannot bypass an explicit user run limit on later cycles."""
    store, row = record
    field = {'iters':'run_iters','hours':'run_hours','proxy':'run_proxy'}[mode]
    row = store.update(OWNER, row['id'], lambda current: current['settings'].update(run_limit_mode=mode, **{field:value}))
    controller = LoopController(tmp_path, store=store, backend=FakeBackend(), ai=FakeAI())
    async def run():
        for _ in range(5):
            await controller.tick(OWNER, row['id'])
    asyncio.run(run())
    current = store.read(OWNER, row['id'])
    optimizers = [job for job in current['jobs'] if job['kind']=='optimizer']
    assert len(optimizers) >= 2
    assert {job['config']['optimize']['iters'] for job in optimizers} == {value if mode=='iters' else 10_000_000}


@pytest.mark.parametrize('mode,expired,paused', [('hours',True,False),('hours',False,False),('proxy',True,False),('proxy',False,False),('hours',True,True)])
def test_run_limit_stops_only_owned_optimizer_and_preserves_partial_results(record, tmp_path, monkeypatch, mode, expired, paused):
    """A quota stop evaluates saved candidates, survives replacement and never tears down its rental."""
    store, row = record
    class Backend(FakeBackend):
        """Model a running optimizer and its native completed stop signal."""
        def poll(self, current, job):
            return {'status':'stopped' if job['operation'] in self.stops else 'running', 'started':True,
                    'source':{'started_at':1000}, 'proxy_evaluations':None if job['operation'] in self.stops else 100 if expired else 99}
        def stop(self, current, job):
            assert self.store.read(OWNER, row['id'])['jobs'][0]['limit_reached'] == mode
            super().stop(current, job)
    backend = Backend(); backend.store = store
    controller = LoopController(tmp_path, store=store, backend=backend, ai=FakeAI())
    row['settings'].update(run_limit_mode=mode, run_hours=1, run_proxy=100)
    job = controller.job(row, 'optimizer', config(), {}, 'Quota test')
    job.update(backend={'id':job['operation'],'execution':'cpu'}, status='running', started=True)
    row = store.update(OWNER,row['id'],lambda current: current.update(settings=row['settings'],phase='optimize',rubric=RULE,jobs=[job],status='paused' if paused else 'running',deadline=100000))
    monkeypatch.setattr('pb8_loop_controller.time.time', lambda: 4600 if expired else 4599)
    asyncio.run(controller._tick(row))
    current = store.read(OWNER,row['id'])
    assert len(backend.stops) == int(expired)
    assert not backend.cleanups and not backend.starts
    assert current['jobs'][0]['run_started_at'] == 1000
    assert current['jobs'][0]['proxy_evaluations'] == (100 if expired else 99)
    if expired and not paused:
        assert current['phase'] == 'select' and current['candidates']
    replacement = LoopController(tmp_path,store=store,backend=backend,ai=FakeAI())
    asyncio.run(replacement.sync_jobs(current,launch=False))
    assert len(backend.stops) == int(expired)


def test_run_hours_excludes_provisioning_and_waiting(record, tmp_path, monkeypatch):
    """Dispatch/setup time cannot consume the per-run runtime allowance."""
    store,row=record
    class Backend(FakeBackend):
        """A rented worker is still setting up and has not launched the optimizer."""
        def poll(self,current,job):
            return {'status':'provisioning','started':True,'source':{'dispatch_at':1000}}
    backend=Backend();controller=LoopController(tmp_path,store=store,backend=backend)
    row['settings'].update(run_limit_mode='hours',run_hours=.01)
    job=controller.job(row,'optimizer',config(),{},'Setup')
    job.update(backend={'id':job['operation'],'execution':'vast'},status='provisioning',started=True)
    row=store.update(OWNER,row['id'],lambda current:current.update(settings=row['settings'],jobs=[job]))
    monkeypatch.setattr('pb8_loop_controller.time.time',lambda:5000)
    current=asyncio.run(controller.sync_jobs(row,launch=False))
    assert not backend.stops
    assert current['jobs'][0].get('run_started_at') is None


def test_pending_per_run_stop_survives_controller_restart(record,tmp_path):
    """A persisted quota stop is retried rather than relaunching a queued job."""
    store,row=record;backend=FakeBackend();controller=LoopController(tmp_path,store=store,backend=backend)
    job=controller.job(row,'optimizer',config(),{},'Pending stop')
    job.update(backend={'id':job['operation'],'execution':'cpu'},status='queued',limit_reached='hours',run_started_at=1000)
    row=store.update(OWNER,row['id'],lambda current:current.update(jobs=[job]))
    asyncio.run(controller.sync_jobs(row))
    assert backend.stops == [job['operation']]
    assert not backend.starts


@pytest.mark.parametrize('execution',['gpu','vast'])
@pytest.mark.parametrize('modern',[False, True])
def test_native_proxy_limit_reads_actual_log_counter(record,tmp_path,monkeypatch,execution,modern):
    """GPU limits use logged proxy evaluations, never estimated generation populations."""
    from api import optimize_v8 as opt
    import vast_jobs
    store,row=record
    row['settings'].update(execution=execution,run_limit_mode='proxy',run_proxy=50)
    filename='b'*32
    log=tmp_path / (('vast_' if execution=='vast' else '')+filename+'.log')
    log.write_text('GPU optimizer progress | gen=1 phase=gpu_proxy | evolution_proxy_completed_run=42 seed_proxy=999 seed_exact=9 evolution_exact=2/10000000 evolution_pending=64\n' if modern else 'GPU optimize | gen=1 proxy=42 (7.0/s) exact=2 inflight=0\n')
    monkeypatch.setattr(opt,'_log_dir',lambda:tmp_path)
    source={'loop_id':row['id'],'loop_owner':OWNER,'status':'running','dispatch_at':900,
            'started_at':1000,'gpu_candidates':9999}
    if execution=='vast':
        monkeypatch.setattr(vast_jobs,'JobStore',lambda:SimpleNamespace(read=lambda name:source,directory=lambda name:tmp_path / 'jobs'))
    else:
        monkeypatch.setattr(opt,'_queue_file',lambda name:tmp_path / 'queue.json')
        monkeypatch.setattr(opt,'_read_json',lambda path:source)
        monkeypatch.setattr(opt,'_queue_status',lambda data:('running',123))
        monkeypatch.setattr(opt,'_launch_config_file',lambda name:tmp_path / 'missing-launch.json')
        monkeypatch.setattr(opt,'_read_runner_state',lambda name:{'started_at':1000})
    job={'kind':'optimizer','backend':{'id':filename,'execution':execution}}
    state=LoopBackend(tmp_path,store).poll(row,job)
    assert state['proxy_evaluations']==42
    assert state['source']['started_at']==1000


def test_inherited_config_values_resolve_search_bounds_and_fixed_overrides():
    """UI defaults must describe effective search capacities instead of raw bot values."""
    from pb8_loop_store import config_defaults
    source=config()
    source['live'].update(approved_coins={'long':['BTC','ETH'],'short':['SOL']}, ignored_coins={'long':['DOGE'],'short':[]})
    source['optimize'].update(iters=5000,bounds={'long':{'risk':{'n_positions':[1,6]}},'short':{'risk':{'n_positions':[2,8]}}})
    values=config_defaults(source)
    assert values['coins']['long']==['BTC','ETH'] and values['ignored_coins']['long']==['DOGE']
    assert values['positions']=={'long':[1,6],'short':[2,8]}
    assert values['total_positions']==[3,14] and values['iters']==5000 and values['direction']=='both'
    source['optimize'].update(fixed_params=['bot.long.risk.n_positions'],fixed_runtime_overrides={'bot.short.risk.n_positions':3})
    values=config_defaults(source)
    assert values['positions']=={'long':[2,2],'short':[3,3]}
    assert values['total_positions']==[5,5]
    source['optimize']['fixed_runtime_overrides']['bot.short.risk.total_wallet_exposure_limit']=0
    values=config_defaults(source)
    assert values['direction']=='long' and values['total_positions']==[2,2]


def test_loop_options_project_first_config_without_starting_runtime(tmp_path,monkeypatch):
    """Inherited previews work for the default selection even if the PB8 runtime is unavailable."""
    import api.loop_optimizer_v8 as api_loop
    from api import optimize_v8 as opt
    import vast_queue, vast_credentials
    loaded=[]
    source=config(); source['optimize']['iters']=1234
    def get_config(name,session):
        loaded.append(name)
        return {'config':source}
    def unavailable(*args):
        raise ValueError('PB8 runtime is not ready')
    monkeypatch.setattr(opt,'list_configs',lambda *args,**kwargs:{'configs':[{'name':'first'}]})
    monkeypatch.setattr(opt,'get_config',get_config)
    monkeypatch.setattr(api_loop,'_controller',SimpleNamespace(backend=SimpleNamespace(initial=unavailable)))
    monkeypatch.setattr(vast_queue,'CloudQueue',lambda:SimpleNamespace(root=tmp_path,read=lambda:{'gpu_preferences':{'hours':12}}))
    monkeypatch.setattr(vast_credentials,'VastCredentialStore',lambda root:SimpleNamespace(metadata=lambda:{'configured':False}))
    response=asyncio.run(api_loop.options('',SimpleNamespace(user_id='test-user')))
    result=json.loads(response.body)
    assert loaded==['first']
    assert result['config_name']=='first' and result['config_defaults']['iters']==1234
    assert result['config_defaults']['coins']['long']==['BTC']
    assert not result['gpu_available']
    assert 'config' not in result


@pytest.mark.parametrize("goal,label", [
    ("gain", "preset 'gain': maximize compound daily gain of strategy equity"),
    ("gain", "gain (preset: maximize average daily gain on strategy equity)"),
    ("drawdown", "drawdown (preset: minimize worst strategy-equity drawdown, hard cap 50%)"),
    ("uptrend", "uptrend (preset: participation/gain during up-trending regimes, weighted ADG)"),
    ("drawdown", "preset 'drawdown': keep drawdown below 50%"),
    ("uptrend", 'preset "uptrend": stable upward equity'),
    ("custom", "free text: drawdowns last only a few hours"),
])
def test_legacy_goal_labels_have_unambiguous_ids(goal, label):
    """The actual failure format normalizes without inventing missing goals."""
    goals = {"presets": [goal] if goal != "custom" else [], "text": "brief drawdowns" if goal == "custom" else ""}
    rubric = [dict(RULE[0], goal=label)]
    normalized = validate_rubric(rubric, goals)
    assert normalized[0]["goal"] == goal
    assert normalized[0]["description"] == label
    assert rubric[0]["goal"] == label


@pytest.mark.parametrize("label", ["maximize gain", "preset 'unknown': more gain", "free text: gain"])
def test_ambiguous_or_unrequested_goals_are_rejected(label):
    """Prose cannot silently satisfy a requested preset or fabricate custom goals."""
    with pytest.raises(ValueError, match="exact IDs: gain"):
        validate_rubric([dict(RULE[0], goal=label)], {"presets": ["gain"]})


def test_missing_goal_error_and_legacy_target_protection():
    """Retry feedback names missing IDs, and normalization cannot bypass user targets."""
    with pytest.raises(ValueError, match="drawdown, uptrend"):
        validate_rubric(RULE, {"presets": ["gain", "drawdown", "uptrend"]})
    with pytest.raises(ValueError, match="preserve explicit"):
        validate_rubric([dict(RULE[0], goal="preset 'gain': more gain")], {"presets": ["gain"], "targets": {"gain": 999}})


def test_interpretation_request_contains_exact_required_goal_ids(record):
    """Every provider receives the canonical preset/custom ID contract."""
    store, row = record
    row = store.update(OWNER, row['id'], lambda value: value['settings']['goals'].update(presets=['gain','drawdown','uptrend'], text='short drawdowns'))
    class AI(LoopAI):
        """Inspect the isolated request without a real model call."""
        async def preflight(self, owner, settings):
            return {'output_limit':16000}
        async def _request(self, record, metadata, content, output_tokens):
            assert content['required_goal_ids'] == ['custom','drawdown','gain','uptrend']
            return json.dumps({'rubric': [dict(RULE[0], goal=key) for key in set(self.store.read(OWNER, row['id'])['settings']['goals']['presets']) | ({'custom'} if self.store.read(OWNER, row['id'])['settings']['goals'].get('text') else set())]}), {'status':'subscription'}
    asyncio.run(AI(store).decide(row, 'interpret', {}))


def test_legacy_interpretation_proceeds_to_initial_optimizer(record, tmp_path):
    """The formerly failing response schedules a native job without another AI retry."""
    store, row = record
    class AI(FakeAI):
        """Return a formerly rejected but unambiguous goal label."""
        async def decide(self, row, stage, evidence):
            return {'rubric':[dict(RULE[0], goal="preset 'gain': maximize growth")]}
    backend = FakeBackend()
    controller = LoopController(tmp_path, store=store, backend=backend, ai=AI())
    asyncio.run(controller.tick(OWNER, row['id']))
    current = store.read(OWNER, row['id'])
    assert current['phase'] == 'optimize' and current['status'] == 'running'
    assert current['rubric'][0]['goal'] == 'gain'
    assert len(current['jobs']) == 1
    assert current['errors'] == 0
    assert backend.starts == []


def test_overlapping_progress_watches_have_one_timer():
    """Concurrent navigation/visibility watches cannot multiply polling timers."""
    import subprocess
    script = r"""
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync('frontend/js/pb8_loop_optimizer.js','utf8');
const body = source.match(/  async function watch\(\) \{[\s\S]*?\n  \}/)[0];
const pending = [], timers = new Map();
let counter = 0;
const context = vm.createContext({timer:null,watchGeneration:0,stopped:false,
    document:{hidden:false},window:{state:{}},state:{panel:'loops-queue'},isLoopPanel(){return true;},
    clearTimeout(id){timers.delete(id);},
    setTimeout(fn,delay){assert.equal(delay,3000);timers.set(++counter,fn);return counter;},
    poll(){return new Promise(resolve=>pending.push(resolve));}});
vm.runInContext(body,context);
(async()=>{
    const first=context.watch(), second=context.watch();
    pending.shift()(); await first;
    assert.equal(timers.size,0,'superseded watch must not schedule');
    pending.shift()(); await second;
    assert.equal(timers.size,1,'one active polling owner');
    const third=context.watch();
    assert.equal(timers.size,0);
    context.stopped=true;context.watchGeneration++;
    pending.shift()();await third;
    assert.equal(timers.size,0,'page shutdown must not revive polling');
})().catch(error=>{console.error(error);process.exitCode=1;});
"""
    result = subprocess.run(['node','-e',script], cwd=Path(__file__).resolve().parents[1], capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stdout + result.stderr


class SavedResultBackend(FakeBackend):
    """Expose historical guidance without creating optimizer/backtest processes."""
    def existing_candidates(self, record):
        """Snapshot one result attributed to the source config."""
        return [{'id':'saved_candidate','optimizer_job':'saved_run','metrics':{'gain':.5},
                 'config':copy.deepcopy(record['initial_config']), 'overrides':{},
                 'source':{'run':'source_run','modified':'2026-09-30','artifact':'p.json'}, 'guidance_score':.5}]


class SavedResultAI(FakeAI):
    """Adjust the first optimizer using historical metrics and current goals."""
    def __init__(self):
        self.stages = []
    async def decide(self, row, stage, evidence):
        """Generate a targeted config without claiming an exact improvement."""
        self.stages.append(stage)
        if stage == 'bootstrap':
            assert evidence['existing_results'][0]['metrics'] == {'gain':.5}
            assert evidence['exact'] is False
            changed=copy.deepcopy(row['initial_config']);changed['optimize']['iters']=777
            return {'reason':'Increase exploration based on saved low gain','knowledge':'Historical gain was below target',
                    'variants':[{'config':changed,'overrides':{},'reason':'More iterations'}]}
        return await super().decide(row,stage,evidence)


def test_saved_results_adjust_first_optimizer_and_survive_restart(record, tmp_path):
    """Saved evidence is evaluated before any native optimization, durably once."""
    store,row=record; backend=SavedResultBackend();ai=SavedResultAI()
    controller=LoopController(tmp_path,store=store,backend=backend,ai=ai)
    asyncio.run(controller.tick(OWNER,row['id']))
    current=store.read(OWNER,row['id'])
    assert current['phase']=='bootstrap' and current['jobs']==[] and backend.starts==[]
    assert current['bootstrap']['candidate_count']==1
    assert current['bootstrap']['candidates'][0]['metrics']['gain']==.5
    assert 'config' not in current['bootstrap']['candidates'][0]
    controller=LoopController(tmp_path,store=store,backend=backend,ai=ai)
    asyncio.run(controller.tick(OWNER,row['id']))
    current=store.read(OWNER,row['id'])
    assert current['phase']=='optimize' and current['round']==0
    assert len(current['jobs'])==1 and current['jobs'][0]['config']['optimize']['iters']==777
    assert current['bootstrap']['status']=='applied'
    assert current['best'] is None and current['validation_count']==0 and not current['holdout_used']
    assert ai.stages==['interpret','bootstrap'] and backend.starts==[]
    assert 'bootstrap_candidates' not in current
    assert store.knowledge(OWNER)['entries'][0]['status']=='historical_optimizer_guidance'
    asyncio.run(controller.tick(OWNER,row['id']))
    assert len(backend.starts)==1 and ai.stages==['interpret','bootstrap']


@pytest.mark.parametrize('invalid', ['unchanged','seed_only','protected_scenario','too_many'])
def test_initial_adjustment_cannot_bypass_policy(record,tmp_path,invalid):
    """Saved evidence cannot bypass Scenario protection or per-loop parallel limits."""
    store,row=record;backend=SavedResultBackend()
    class AI(SavedResultAI):
        """Return a deliberately invalid initial variant."""
        async def decide(self,row,stage,evidence):
            if stage!='bootstrap':return await super().decide(row,stage,evidence)
            changed=copy.deepcopy(row['initial_config'])
            if invalid=='protected_scenario':changed['backtest']['start_date']='2020-01-15'
            if invalid=='seed_only':changed['optimize']['seed']=12345
            variants=[{'config':changed,'overrides':{}}]
            if invalid=='too_many':variants=variants*2
            return {'variants':variants}
    controller=LoopController(tmp_path,store=store,backend=backend,ai=AI())
    asyncio.run(controller.tick(OWNER,row['id']))
    asyncio.run(controller.tick(OWNER,row['id']))
    current=store.read(OWNER,row['id'])
    assert current['phase']=='bootstrap' and current['errors']==1 and current['jobs']==[]
    assert backend.starts==[] and not current['holdout_used']


def test_saved_results_use_optional_jev_once_before_adjustment(record,tmp_path):
    """A restarted controller retains JEV evidence and never duplicates its request."""
    store,row=record;backend=SavedResultBackend()
    class AI(SavedResultAI):
        """Ask for JEV on the first decision and use its persisted feedback."""
        jev_calls=0
        async def decide(self,row,stage,evidence):
            decision=await super().decide(row,stage,evidence)
            if stage=='bootstrap':decision['jev_needed']=True
            return decision
        async def jev(self,row,candidates):
            self.jev_calls+=1
            return {'best':'saved_candidate'}
    ai=AI();controller=LoopController(tmp_path,store=store,backend=backend,ai=ai)
    asyncio.run(controller.tick(OWNER,row['id']))
    asyncio.run(controller.tick(OWNER,row['id']))
    assert store.read(OWNER,row['id'])['phase']=='bootstrap'
    replacement=LoopController(tmp_path,store=store,backend=backend,ai=ai)
    asyncio.run(replacement.tick(OWNER,row['id']))
    assert ai.jev_calls==1 and store.read(OWNER,row['id'])['phase']=='optimize'


def test_jev_projection_preserves_goal_evidence_within_context(record, monkeypatch):
    """Large optimizer metric catalogs cannot prevent a goal-specific JEV consultation."""
    import ai_openrouter
    from ai_token_budget import estimate_jev_input_tokens, JEV_CONTEXT_TOKENS
    store, row = record
    row = store.update(OWNER, row['id'], lambda current: current.update(
        rubric=RULE, settings={**current['settings'], 'jev_budget_usd': .05, 'jev_max_cost_usd': .01}))
    diagnostics = {'optimizer_diagnostic_' + str(index) + '_' + 'x' * 70: index + .0123456789 for index in range(500)}
    candidates = [{'id': 'c_' + f'{index:024x}', 'metrics': {**diagnostics, 'gain': index / 10}}
                  for index in range(12)]
    original = {'state': {'goals': row['settings']['goals'], 'candidates': candidates},
                'questions': {'best': {'type': 'choice', 'instructions': 'Choose the best candidate',
                                      'criteria': {item['id']: json.dumps(item['metrics'])[:500] for item in candidates}}}}
    with pytest.raises(ai_openrouter.OpenRouterDecisionError, match='context budget'):
        ai_openrouter.prepare_user_jev_payload(ai_openrouter.JEV_MODEL, original)
    events = []
    async def budget(session, key, requests, maximum):
        """Check the production payload before any reservation or dispatch."""
        assert store.read(OWNER, row['id'])['jev_usd_reserved'] == 0
        request = requests[0]
        assert estimate_jev_input_tokens(request) < JEV_CONTEXT_TOKENS
        assert request['state']['rubric'] == RULE
        assert [item['id'] for item in request['state']['candidates']] == [item['id'] for item in candidates]
        assert [item['metrics'] for item in request['state']['candidates']] == [{'gain': item['metrics']['gain']} for item in candidates]
        assert set(request['questions']['best']['criteria']) == {item['id'] for item in candidates}
        assert maximum == row['settings']['jev_max_cost_usd']
        events.append('budget')
    async def send(session, key, request):
        """Return a typed transport fixture after the durable cost reservation."""
        assert store.read(OWNER, row['id'])['jev_usd_reserved'] == row['settings']['jev_max_cost_usd']
        events.append('send')
        return 'Choice: ' + candidates[0]['id']
    monkeypatch.setattr(ai_openrouter, 'check_jev_budget', budget)
    monkeypatch.setattr(ai_openrouter, '_send_structured_jev_request', send)
    chat = _loop_chat_for_payloads([])
    chat.credentials.openrouter_configured = lambda owner: True
    chat.credentials.load_openrouter_key = lambda owner: 'test-only-key'
    answer = asyncio.run(LoopAI(store, chat).jev(row, candidates))
    assert answer == 'Choice: ' + candidates[0]['id']
    assert events == ['budget', 'send']
    assert all(len(item['metrics']) == 501 for item in candidates)


def test_jev_cost_rejection_does_not_reserve_or_dispatch(record, monkeypatch):
    """Projection cannot bypass the existing shared JEV consultation cost limit."""
    import ai_openrouter
    store, row = record
    row = store.update(OWNER, row['id'], lambda current: current.update(
        rubric=RULE, settings={**current['settings'], 'jev_budget_usd': .05, 'jev_max_cost_usd': .01}))
    candidates = [{'id': 'c_first', 'metrics': {'gain': 1}}, {'id': 'c_second', 'metrics': {'gain': 2}}]
    async def budget(*args):
        """Reject the cost estimate before a paid request."""
        raise ai_openrouter.JevBudgetExceeded(.1)
    async def send(*args):
        """No provider traffic is allowed after failed preflight."""
        pytest.fail('JEV must not dispatch after cost rejection')
    monkeypatch.setattr(ai_openrouter, 'check_jev_budget', budget)
    monkeypatch.setattr(ai_openrouter, '_send_structured_jev_request', send)
    chat = _loop_chat_for_payloads([])
    chat.credentials.openrouter_configured = lambda owner: True
    chat.credentials.load_openrouter_key = lambda owner: 'test-only-key'
    with pytest.raises(ai_openrouter.JevBudgetExceeded):
        asyncio.run(LoopAI(store, chat).jev(row, candidates))
    assert store.read(OWNER, row['id'])['jev_usd_reserved'] == 0


@pytest.mark.parametrize('known_error', [True, False])
def test_initial_jev_failure_retains_safe_reason_only(record, tmp_path, known_error):
    """Known JEV diagnostics survive without exposing arbitrary transport exception details."""
    from ai_openrouter import OpenRouterDecisionError
    store, row = record
    class AI(SavedResultAI):
        """Request optional JEV and reject it with a controlled diagnostic."""
        async def decide(self, current, stage, evidence):
            decision = await super().decide(current, stage, evidence)
            if stage == 'bootstrap':
                decision['jev_needed'] = True
            return decision
        async def jev(self, current, candidates):
            if known_error:
                raise OpenRouterDecisionError('Jev input exceeds the estimated 32,000-token context budget')
            raise RuntimeError('test-only-sensitive-transport-detail')
    controller = LoopController(tmp_path, store=store, backend=SavedResultBackend(), ai=AI())
    asyncio.run(controller.tick(OWNER, row['id']))
    asyncio.run(controller.tick(OWNER, row['id']))
    current = store.read(OWNER, row['id'])
    diagnostic = current['bootstrap_jev_answers'][0]
    assert 'JEV request failed:' in diagnostic
    assert 'test-only-sensitive-transport-detail' not in diagnostic
    assert ('context budget' in diagnostic) == known_error
    assert current['status'] == 'running' and current['bootstrap_jev_done']


def test_saved_candidate_lookup_is_bounded_exact_and_readonly(record,tmp_path,monkeypatch):
    """Only matching source runs are read; corrupt files do not hide usable evidence."""
    from api import optimize_v8 as opt
    store,row=record;row={**row,'rubric':RULE}
    root=tmp_path/'results';root.mkdir()
    sources=[]
    for index in range(5):
        directory=root/f'run_{index}';(directory/'pareto').mkdir(parents=True)
        payload={**config(),'metrics':{'gain':index+1,'drawdown_worst':.1}}
        (directory/'pareto'/'valid.json').write_text(json.dumps(payload))
        (directory/'pareto'/'broken.json').write_text('{')
        sources.append({'name':'source' if index<4 else 'source_other','path':str(directory),
                        'result':directory.name,'modified':f'2026-09-{30-index}'})
    before={str(p):p.read_bytes() for p in root.rglob('*.json')}
    monkeypatch.setattr(opt,'_results_root',lambda:root)
    monkeypatch.setattr(opt,'_list_results',lambda:sources)
    monkeypatch.setattr(opt,'_all_results_first',lambda path:None)
    candidates=LoopBackend(tmp_path,store).existing_candidates(row)
    assert len(candidates)==4
    assert {item['source']['run'] for item in candidates}=={'run_0','run_1','run_2','run_3'}
    assert [item['metrics']['gain'] for item in candidates] == [1, 2, 3, 4]
    assert {str(p):p.read_bytes() for p in root.rglob('*.json')}==before


def test_saved_results_progress_excludes_internal_candidate_configs(record):
    """Progress reports provenance while keeping source candidate config copies private."""
    from api.loop_optimizer_v8 import projection
    store,row=record
    row['bootstrap']={'status':'evaluating','sources':['run'],'candidate_count':1}
    row['bootstrap_candidates']=[{'config':config()}]
    result=projection(row)
    assert result['bootstrap']==row['bootstrap']
    assert 'bootstrap_candidates' not in result


@pytest.mark.parametrize('invalid', [None, 'missing_job', 'invalid_id', 'symlink', 'wrong_name'])
def test_vast_saved_result_attribution_uses_job_provenance(record, tmp_path, monkeypatch, invalid):
    """Generic cloud labels use exact job metadata; invalid provenance never guesses."""
    from api import optimize_v8 as opt
    store, row = record
    row = {**row, 'rubric': RULE}
    root = tmp_path / 'results'; directory = root / 'cloud'
    (directory / 'pareto').mkdir(parents=True)
    (directory / 'pareto' / 'candidate.json').write_text(json.dumps({**config(), 'metrics': {'gain': .5}}))
    identifier = '1' * 32
    job_dir = tmp_path / 'data/vast/jobs' / identifier; job_dir.mkdir(parents=True)
    (job_dir / 'state.json').write_text(json.dumps({'id': identifier, 'config_name': 'other' if invalid == 'wrong_name' else 'source'}))
    marker = directory / '.pbgui_vast.json'
    provenance = {'job_id': '2' * 32 if invalid == 'missing_job' else '../invalid' if invalid == 'invalid_id' else identifier}
    if invalid == 'symlink':
        external = tmp_path / 'external.json'; external.write_text(json.dumps(provenance)); marker.symlink_to(external)
    else:
        marker.write_text(json.dumps(provenance))
    source = {'name': 'backtests', 'result': 'cloud', 'path': str(directory), 'modified': '2026-10-01'}
    monkeypatch.setattr(opt, '_results_root', lambda: root)
    monkeypatch.setattr(opt, '_list_results', lambda: [source])
    before = {str(p): p.read_bytes() for p in tmp_path.rglob('*.json')}
    candidates = LoopBackend(tmp_path, store).existing_candidates(row)
    assert bool(candidates) == (invalid is None)
    assert {str(p): p.read_bytes() for p in tmp_path.rglob('*.json')} == before


def test_vast_loop_candidates_use_exact_backend_job_id(record, tmp_path, monkeypatch):
    """Two cloud runs with the same name cannot be confused after a loop completes."""
    from api import optimize_v8 as opt
    store, row = record; row = {**row, 'rubric': RULE}
    root = tmp_path / 'results'; sources = []
    for digit in ('1', '2'):
        identifier = digit * 32; directory = root / digit
        (directory / 'pareto').mkdir(parents=True)
        (directory / 'pareto' / 'candidate.json').write_text(json.dumps({**config(), 'metrics': {'gain': int(digit)}}))
        (directory / '.pbgui_vast.json').write_text(json.dumps({'job_id': identifier}))
        job_dir = tmp_path / 'data/vast/jobs' / identifier; job_dir.mkdir(parents=True)
        (job_dir / 'state.json').write_text(json.dumps({'id': identifier, 'config_name': 'loop_job'}))
        sources.append({'name': 'backtests', 'result': digit, 'path': str(directory), 'modified': '2026-10-01'})
    monkeypatch.setattr(opt, '_results_root', lambda: root)
    monkeypatch.setattr(opt, '_list_results', lambda: sources)
    backend = LoopBackend(tmp_path, store)
    job = {'name': 'loop_job', 'operation': 'owned', 'config': config(), 'overrides': {},
           'backend': {'execution': 'vast', 'id': '2' * 32}}
    result = backend.candidates(row, job)
    assert len(result) == 1 and result[0]['metrics']['gain'] == 2
    assert result[0]['source']['run'] == '2'
    for identifier in ('3' * 32, None):
        job['backend']['id'] = identifier
        with pytest.raises(ValueError, match='uniquely attributable'):
            backend.candidates(row, job)


@pytest.mark.parametrize('execution', ['gpu', 'vast'])
def test_proxy_limit_stops_from_generation_profile_before_next_summary(record, tmp_path, execution):
    """A 100k proxy quota stops on generation 13, without waiting for generation 20."""
    from api import optimize_v8 as opt
    store, row = record
    text = 'GPU optimize | gen=10 proxy=81920 (200.0/s) exact=640 inflight=0\n'
    for generation in (11, 12, 13):
        text += '[gpu-profile] ' + json.dumps({'event':'generation', 'generation':generation,
            'population_size':8192, 'seed_proxy_reused':0, 'screening':[], 'exact_completed':generation*64}) + '\n'
    class Backend(FakeBackend):
        """Feed real parser output into owned stop/collection without cloud actions."""
        def poll(self, current, job):
            return {'status':'stopped' if job['operation'] in self.stops else 'running', 'started':True,
                    'source':{'started_at':1000}, 'proxy_evaluations':opt._parse_optimize_log_status(text)['proxy_evaluations']}
    backend = Backend()
    controller = LoopController(tmp_path, store=store, backend=backend, ai=FakeAI())
    row['settings'].update(execution=execution, run_limit_mode='proxy', run_proxy=100000)
    gpu_config = valid_gpu_config()
    row['initial_config'] = copy.deepcopy(gpu_config)
    gpu_config['optimize']['backend'] = 'gpu'
    job = controller.job(row, 'optimizer', gpu_config, {}, 'Proxy quota')
    job.update(backend={'id':job['operation'],'execution':execution}, status='running', started=True)
    row = store.update(OWNER, row['id'], lambda current:current.update(settings=row['settings'],
        phase='optimize', rubric=RULE, jobs=[job]))
    asyncio.run(controller._tick(row))
    current = store.read(OWNER, row['id'])
    assert backend.stops == [job['operation']] and not backend.cleanups and not backend.starts
    assert current['jobs'][0]['limit_reached'] == 'proxy'
    assert current['phase'] == 'select' and current['candidates']


@pytest.mark.parametrize('prior_state', ['missed', 'consumed', 'validated', 'still_running'])
def test_skipped_quota_results_recovered_after_active_round_once(record, tmp_path, prior_state):
    """Recovery waits for the active round and never duplicates validated source jobs."""
    store, row = record
    class Backend(FakeBackend):
        """Report active round completion without touching any native jobs."""
        def poll(self, current, job):
            return {'status':'running' if prior_state=='still_running' else 'completed', 'started':True}
    class AI(FakeAI):
        """Select the recovered earlier candidate when it is offered."""
        async def decide(self, current, stage, evidence):
            if stage=='select':return {'candidate_ids':[evidence['candidates'][-1]['id']], 'reason': 'Validate the recovered distinct candidate within the remaining CPU budget'}
            return await super().decide(current,stage,evidence)
    backend = Backend(); controller = LoopController(tmp_path, store=store, backend=backend, ai=AI())
    old = controller.job(row, 'optimizer', config(), {}, 'Initial optimizer')
    old.update(status='cancelled', started=True, limit_reached='proxy', backend={'id':old['operation'],'execution':'vast'})
    if prior_state=='consumed':old['results_consumed']=True
    current = controller.job({**row,'round':1}, 'optimizer', config(), {}, 'Next variant')
    current.update(status='running', started=True, backend={'id':current['operation'],'execution':'vast'})
    jobs = [old,current]
    if prior_state=='validated':
        candidate=backend.candidates(row,old)[0]
        validation=controller.job(row,'validation',config(),{},'Earlier validation',candidate=candidate)
        validation.update(status='completed',started=True)
        jobs.append(validation)
    row=store.update(OWNER,row['id'],lambda value:value.update(round=1,phase='optimize',rubric=RULE,jobs=jobs))
    asyncio.run(controller._tick(row))
    current_row=store.read(OWNER,row['id'])
    assert not backend.starts and not backend.stops and not backend.cleanups
    if prior_state=='still_running':
        assert current_row['phase']=='optimize' and current_row['validation_count']==0
        return
    assert current_row['phase']=='select'
    assert len(current_row['candidates']) == (2 if prior_state=='missed' else 1)
    if prior_state=='missed':
        assert current_row['jobs'][0]['results_consumed'] and current_row['jobs'][0]['result_count']==1
        replacement=LoopController(tmp_path,store=store,backend=backend,ai=AI())
        asyncio.run(replacement._tick(current_row))
        current_row=store.read(OWNER,row['id'])
        assert current_row['phase']=='validate' and current_row['validation_count']==1
        assert current_row['jobs'][-1]['candidate']['optimizer_job']==old['operation']


def test_quota_result_errors_remain_specific_for_ai_repair(record, tmp_path):
    """Missing native output must not become an unsupported generic optimizer failure."""
    store,row=record
    class Backend(FakeBackend):
        """A stopped run has no attributable output yet."""
        def candidates(self,current,job):raise ValueError('Native result attribution unavailable')
    backend=Backend();controller=LoopController(tmp_path,store=store,backend=backend,ai=FakeAI())
    job=controller.job(row,'optimizer',config(),{},'Quota run')
    job.update(status='cancelled',started=True,limit_reached='proxy',backend={'id':job['operation'],'execution':'vast'})
    row=store.update(OWNER,row['id'],lambda value:value.update(phase='optimize',rubric=RULE,jobs=[job]))
    asyncio.run(controller._tick(row))
    row=store.read(OWNER,row['id'])
    assert row['phase']=='evaluate' and row['repair_error']=='Native result attribution unavailable'
    assert row['jobs'][0]['result_error']==row['repair_error']


@pytest.mark.parametrize('group', ['scoring', 'limits'])
def test_cloud_loop_rejects_exact_only_metrics_before_queue(record, group):
    """Cloud metric checks retain user objectives and report an explicit alternative."""
    _, row = record
    row['settings']['execution'] = 'vast'
    proposed = valid_gpu_config()
    row['initial_config'] = copy.deepcopy(proposed)
    proposed['optimize'].update(backend='gpu', **{group: [{'metric': 'gain_strategy_eq', 'goal': 'max'}]})
    original = copy.deepcopy(proposed)
    with pytest.raises(ValueError, match=r'Vast.ai optimize.*gain_strategy_eq') as error:
        validate_change(row, proposed, {})
    assert 'adg_strategy_eq' in str(error.value)
    assert proposed == original
    proposed['optimize'][group][0]['metric'] = 'adg_strategy_eq'
    if group == 'limits':
        proposed['optimize'][group][0].update(penalize_if='less_than', value=0.0)
    assert validate_change(row, proposed, {}) == proposed


@pytest.mark.parametrize('group', ['scoring', 'limits'])
@pytest.mark.parametrize('metric', __import__('vast_config_validation')._METRIC_CONTRACT['exact_only_metrics'])
def test_every_exact_only_cloud_metric_is_blocked_without_changing_cpu_goals(metric, group):
    """The complete pinned exclusion list is enforced, without rewriting CPU metrics."""
    from pb8_loop_store import validate_cloud_loop_metrics
    proposed = {'optimize': {group: [{'metric': metric, 'goal': 'max', 'value': 1}]}}
    original = copy.deepcopy(proposed)
    with pytest.raises(ValueError, match='Unsupported cloud metrics'):
        validate_cloud_loop_metrics(proposed, 'vast')
    validate_cloud_loop_metrics(proposed, 'cpu')
    assert proposed == original


@pytest.mark.parametrize(('group', 'field'), [('limits', 'reducer'), ('limits', 'stat'),
                                            ('scoring', 'reducer'), ('scoring', 'aggregate')])
def test_cloud_loop_rejects_worst_reducer_before_job_preparation(group, field):
    """Invalid AI reducers fail early without silently changing a user's limit."""
    from pb8_loop_store import validate_cloud_loop_metrics
    from vast_config_validation import REDUCERS
    proposed = {'optimize': {group: [{'metric': 'drawdown_worst_strategy_eq', field: 'worst'}]}}
    original = copy.deepcopy(proposed)
    with pytest.raises(ValueError, match='Unsupported optimizer reducer'):
        validate_cloud_loop_metrics(proposed, 'vast')
    assert proposed == original
    validate_cloud_loop_metrics(proposed, 'cpu')
    for reducer in REDUCERS:
        proposed['optimize'][group][0][field] = reducer
        validate_cloud_loop_metrics(proposed, 'vast')


@pytest.mark.parametrize(('group', 'alias'), [('limits', 'stat'), ('scoring', 'aggregate')])
def test_cloud_loop_rejects_conflicting_reducer_aliases(group, alias):
    """Cloud aliases must agree so worker interpretation remains unambiguous."""
    from pb8_loop_store import validate_cloud_loop_metrics
    proposed = {'optimize': {group: [{'metric': 'drawdown_worst_strategy_eq', 'reducer': 'max', alias: 'mean'}]}}
    with pytest.raises(ValueError, match='Conflicting reducer aliases'):
        validate_cloud_loop_metrics(proposed, 'vast')
    proposed['optimize'][group][0][alias] = 'max'
    validate_cloud_loop_metrics(proposed, 'vast')


def test_cloud_metric_policy_reaches_ai_with_custom_instruction_snapshot(record):
    """Every provider sees immutable eligibility even when a run uses custom instructions."""
    from vast_config_validation import METRICS, REDUCERS, _METRIC_CONTRACT
    store, row = record
    row['settings']['execution'] = 'vast'
    row['ai_instructions'] = {'id': 'custom', 'name': 'Custom', 'text': 'Return JSON.'}
    class AI(LoopAI):
        """Inspect the actual provider payload without making a model request."""
        async def preflight(self, owner, settings):
            """Use isolated metadata without a provider call."""
            return {'output_limit': 16000}
        async def _request(self, current, metadata, content, output_tokens):
            """Keep exact goal metrics separate from GPU objective eligibility."""
            policy = content['optimizer_metric_policy']
            assert set(policy['allowed_metrics']) == METRICS
            assert set(policy['allowed_reducers']) == REDUCERS
            assert set(policy['exact_only_metrics']) == set(_METRIC_CONTRACT['exact_only_metrics'])
            assert 'gain_strategy_eq' not in policy['allowed_metrics']
            assert 'adg_strategy_eq' in policy['allowed_metrics']
            assert policy['exact_goal_metrics_restricted'] is False
            assert content['goals'] == current['settings']['goals']
            return json.dumps({'rubric': [dict(RULE[0], goal=key) for key in set(self.store.read(OWNER, row['id'])['settings']['goals']['presets']) | ({'custom'} if self.store.read(OWNER, row['id'])['settings']['goals'].get('text') else set())]}), {'status': 'subscription'}
    asyncio.run(AI(store).decide(row, 'interpret', {}))


def test_failed_optimizer_keeps_native_error_in_termination_details(record, tmp_path):
    """A completed polling cycle must not replace the actual failure with a generic label."""
    store, row = record
    message = 'Cloud configuration is invalid: optimize.scoring.0.metric: Unsupported cloud metrics: gain_strategy_eq.'
    class Backend(FakeBackend):
        """Simulate a terminal cloud preparation error without accessing cloud state."""
        def candidates(self, current, job):
            """Preparation failed before producing any exact result."""
            return []
        def poll(self, current, job):
            """Return the recorded native error."""
            return {'status': 'failed', 'started': False, 'source': {'error': message}}
    controller = LoopController(tmp_path, store=store, backend=Backend(), ai=FakeAI())
    job = controller.job(row, 'optimizer', config(), {}, 'Initial run')
    job.update(backend={'id': job['operation'], 'execution': 'vast'})
    row = store.update(OWNER, row['id'], lambda current: current.update(phase='optimize', jobs=[job], rubric=RULE))
    asyncio.run(controller.tick(OWNER, row['id']))
    row = store.read(OWNER, row['id'])
    assert row['phase'] == 'evaluate' and row['repair_error'] == message
    asyncio.run(controller.tick(OWNER, row['id']))
    row = store.read(OWNER, row['id'])
    assert row['status'] == 'failed' and row['failure']['detail'] == message


def test_job_projection_reports_actual_pinned_changes_and_candidate_links(record, tmp_path):
    """Reports show effective settings and detailed list leaves without full config copies."""
    from api.loop_optimizer_v8 import projection
    store,row=record;row['settings'].update(run_limit_mode='proxy',run_proxy=100000)
    controller=LoopController(tmp_path,store=store,backend=FakeBackend(),ai=FakeAI())
    changed=config();changed['optimize']['iters']=100000
    changed['optimize']['limits']=[{'metric':'strategy_eq_recovery_days_max','value':.25}]
    row['best'] = _warm_start_observation(row, 2)
    row['best']['candidate']['overrides'] = {'BTC':{'bot':{'long':{'risk':{'n_positions':3}}}}}
    optimizer=controller.job({**row,'round':1},'optimizer',changed,
        row['best']['candidate']['overrides'],'Recovery objective')
    candidate={'id':'candidate','optimizer_job':optimizer['operation']}
    validation=controller.job(row,'validation',config(),{},'Fixed comparison',candidate=candidate)
    row['jobs']=[optimizer,validation]
    report=projection(row)
    actual=report['jobs'][0]
    assert actual['native_iters']==10000000
    assert {'path':'optimize.limits.0.value','before':None,'after':.25} in actual['changes']
    assert any(field['path'].startswith('overrides.BTC.') for field in actual['changes'])
    assert not {'config','overrides','executed_config'} & actual.keys()
    assert report['jobs'][1]['candidate_optimizer']==optimizer['operation']


@pytest.mark.parametrize('evidence_kind', ['missing','proxy','incomparable','unowned','pending','valid'])
def test_next_cycle_requires_completed_owned_exact_backtest(record,tmp_path,evidence_kind):
    """Only completed comparable exact backtests can authorize an adaptive optimizer round."""
    store,row=record;backend=FakeBackend()
    class AI(FakeAI):
        """Count decisions so the gate cannot spend another AI call without validation."""
        calls=0
        async def decide(self,current,stage,evidence):
            self.calls+=1
            return await super().decide(current,stage,evidence)
    ai=AI();controller=LoopController(tmp_path,store=store,backend=backend,ai=ai)
    optimizer=controller.job(row,'optimizer',config(),{},'Optimizer')
    optimizer.update(status='completed',started=True)
    candidate=backend.candidates(row,optimizer)[0]
    check=controller.job(row,'validation',config(),{},'Fixed comparison',candidate=candidate)
    check.update(status='running' if evidence_kind=='pending' else 'completed',started=True)
    assessment={**evaluate({'gain':1},RULE), 'simulation_complete': True}
    if evidence_kind=='incomparable':assessment['comparable']=False
    observation={'operation':'not_owned' if evidence_kind=='unowned' else check['operation'],
                 'candidate':candidate,'metrics':{'gain':1},'exact':evidence_kind!='proxy','assessment':assessment}
    row=store.update(OWNER,row['id'],lambda current:current.update(phase='evaluate',rubric=RULE,jobs=[optimizer,check],
        observations=[] if evidence_kind=='missing' else [observation]))
    asyncio.run(controller._tick(row));current=store.read(OWNER,row['id'])
    if evidence_kind=='valid':
        assert current['round']==1 and ai.calls==1
        assert current['history'][0]['cycle_score']==assessment['score']
        assert current['history'][0]['observations'][0]['operation']==check['operation']
    else:
        assert current['round']==0 and current['status']=='failed' and ai.calls==0
        assert 'Next cycle blocked' in current['reason']
    assert not backend.starts


def test_cycle_reports_compare_exact_goals_and_record_duration(record,monkeypatch):
    """Worse and better goal values remain separate from cumulative-best scores."""
    from api.loop_optimizer_v8 import _cycle_reports
    store,row=record
    rules=RULE+[{'goal':'drawdown','metrics':['drawdown_worst'],'direction':'min','target':.3,'weight':1,'scale':1}]
    row['rubric']=rules;row['round']=1;row['status']='completed';row['phase']='evaluate'
    row['jobs']=[{'kind':'optimizer','round':number,'status':'completed','created_at':1000+number*1000,'ended_at':1400+number*1000} for number in (0,1)]
    row['history']=[{'round':number,'assessment':evaluate({'gain':gain,'drawdown_worst':dd},rules),'evaluated_at':1500+number*1000} for number,gain,dd in [(0,1,.4),(1,.8,.2)]]
    reports=_cycle_reports(row)
    assert reports[0]['duration_seconds']==500 and reports[1]['duration_seconds']==500
    assert reports[1]['goals'][0]['delta']==pytest.approx(-.2)
    assert reports[1]['goals'][0]['improved'] is False
    assert reports[1]['goals'][1]['delta']==pytest.approx(-.2)
    assert reports[1]['goals'][1]['improved'] is True
    row['history']=[{'round':0,'best_score':999}]
    assert all(item['score'] is None for item in _cycle_reports(row))


def test_snapshot_diff_is_authenticated_and_owner_scoped(record,tmp_path,monkeypatch):
    """Only snapshots in the authenticated owner's loop can be compared."""
    import httpx
    from fastapi import FastAPI, Request
    from api import loop_optimizer_v8 as api
    from api.auth import require_auth
    store,row=record;controller=LoopController(tmp_path,store=store,backend=FakeBackend(),ai=FakeAI())
    original=controller.job(row,'optimizer',config(),{},'Original')
    changed=copy.deepcopy(config());changed['optimize']['iters']=777
    later=controller.job(row,'optimizer',changed,{},'Later')
    store.update(OWNER,row['id'],lambda current:current.update(jobs=[original,later]))
    monkeypatch.setattr(api,'_controller',controller)
    monkeypatch.setattr(api,'_owner',lambda session:session)
    app=FastAPI();app.include_router(api.router,prefix='/loops')
    def authenticate(request:Request):
        value=request.headers.get('x-test-owner')
        if not value:raise HTTPException(401,'Authentication required')
        return value
    authenticate.__annotations__['request']=Request
    app.dependency_overrides[require_auth]=authenticate
    async def run():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://fixture') as client:
            url=f"/loops/{row['id']}/diff?operation={later['operation']}&baseline={original['operation']}"
            assert (await client.get(url)).status_code==401
            result=await client.get(url,headers={'x-test-owner':OWNER})
            assert result.status_code==200 and result.headers['cache-control']=='no-store'
            assert {'path':'optimize.iters','before':None,'after':777} in result.json()['fields']
            assert (await client.get(url,headers={'x-test-owner':'b'*32})).status_code!=200
            other=f"/loops/{row['id']}/diff?operation={'e'*32}"
            assert (await client.get(other,headers={'x-test-owner':OWNER})).status_code==404
    asyncio.run(run())


def test_cycle_report_exposes_completed_checks_while_ai_evaluates(record):
    """Successful exact metrics are visible before the next AI decision is persisted."""
    from api.loop_optimizer_v8 import _cycle_reports
    _,row=record
    row.update(rubric=RULE,phase='evaluate',round=0,status='running')
    row['jobs']=[{'kind':'optimizer','round':0,'status':'completed','operation':'a'*32,'created_at':1000,'ended_at':1100},
                 {'kind':'validation','round':0,'status':'completed','operation':'b'*32,'created_at':1100,'ended_at':1200}]
    row['observations']=[{'operation':'b'*32,'exact':True,'candidate':{'id':'result','optimizer_job':'a'*32},
                          'assessment':evaluate({'gain':1},RULE),'metrics':{'gain':1,'unrelated':7}}]
    cycle=_cycle_reports(row)[0]
    assert cycle['score'] is None and cycle['completed_backtests']==1 and cycle['active']
    assert cycle['observations'][0]['candidate_id']=='result'
    assert cycle['observations'][0]['metrics']=={'gain':1}


@pytest.mark.parametrize('enabled', [False, True])
@pytest.mark.parametrize('location', ['live', 'runtime', 'override'])
def test_strategy_permission_covers_override_and_runtime_paths(record, enabled, location):
    """Changing strategy is opt-in, including sparse overrides and runtime dotted keys."""
    _, row = record
    row['initial_config']['live']['strategy_kind'] = 'ema_anchor'
    row['settings']['strategy_enabled'] = enabled
    changed = copy.deepcopy(row['initial_config']); overrides = {}
    if location == 'live':
        changed['live']['strategy_kind'] = 'trailing_martingale'
    elif location == 'runtime':
        changed['optimize']['fixed_runtime_overrides'] = {'live.strategy_kind': 'trailing_martingale'}
    else:
        overrides = {'BTC.json': {'live': {'strategy_kind': 'trailing_martingale'}}}
    if enabled:
        validate_change(row, changed, overrides)
    else:
        with pytest.raises(ValueError, match='Strategy changes are disabled'):
            validate_change(row, changed, overrides)


def test_new_loop_runs_unchanged_baseline_before_any_optimizer(tmp_path):
    """Even a planned initial optimizer cannot launch until the baseline has completed."""
    store = LoopStore(tmp_path / 'loops')
    row = store.create(OWNER, settings(), config(), {}, {'pb8': 'commit', 'data': 'shards'}, [], config()['backtest'])
    store.update(OWNER, row['id'], lambda current: current.update(ai_model_profile={'context': 4_000_000, 'page_tokens': 1500, 'output_tokens': 1000}))
    backend = FakeBackend(); controller = LoopController(tmp_path, store=store, backend=backend, ai=FakeAI())
    async def run():
        await controller.tick(OWNER, row['id'])
        await controller.tick(OWNER, row['id'])
        current = store.read(OWNER, row['id'])
        assert current['phase'] == 'baseline' and not backend.starts
        baseline = next(job for job in current['jobs'] if job['kind'] == 'baseline')
        assert baseline['config'] == current['initial_config']
        await controller.tick(OWNER, row['id'])
        current = store.read(OWNER, row['id'])
        assert current['baseline']['exact'] and current['phase'] == 'optimize'
        assert backend.starts == [baseline['operation']]
        replacement = LoopController(tmp_path, store=store, backend=backend, ai=FakeAI())
        await replacement.tick(OWNER, row['id'])
        assert len(backend.starts) == 2 and backend.starts.count(baseline['operation']) == 1
    asyncio.run(run())


def test_holdout_balance_stays_at_backtest_root(record, tmp_path):
    """Native PB8 scenarios reject starting_balance; inheritance belongs at the root."""
    store, row = record
    row['holdouts'] = [{'label': 'holdout', 'start_date': '2020-03-01', 'end_date': '2020-04-01', 'exchanges': ['binance'], 'starting_balance': 10000}]
    candidate = {'config': config()}
    result = LoopBackend(tmp_path, store).validation_config(row, candidate, True)
    assert result['backtest']['starting_balance'] == 10000
    assert 'starting_balance' not in result['backtest']['scenarios'][0]
    assert row['holdouts'][0]['starting_balance'] == 10000


@pytest.mark.parametrize('completion,qualified', [(1, True), (.01, False), (None, False)])
def test_exact_qualification_rejects_partial_and_unknown_simulations(record, tmp_path, monkeypatch, completion, qualified):
    """High scored liquidation or missing completion proof cannot become a winner."""
    from api import backtest_v8 as bt
    store, row = record; row['rubric'] = RULE
    report = {'gain': 10}
    if completion is not None:
        report['backtest_completion_ratio'] = completion
    path = tmp_path / 'analysis.json'; path.write_text(json.dumps(report))
    monkeypatch.setattr(bt, '_result_analysis_paths', lambda name: [path])
    monkeypatch.setattr(bt, '_read_json', lambda path: json.loads(path.read_text()))
    obs = LoopBackend(tmp_path, store).observations(row, {'name': 'job', 'config': config()})
    assert obs['exact'] and obs['assessment']['simulation_complete'] is qualified
    assert obs['assessment']['comparable'] is qualified
    assert obs['assessment']['achieved'] is qualified


def test_holdout_reservation_release_and_consumption_are_distinct(record):
    """Pre-data failure can release a claim; evaluated dates remain unavailable to all runs."""
    store, row = record
    row['holdouts'] = [{'start_date': '2020-03-01', 'end_date': '2020-04-01'}]
    assert store.claim_holdouts(row) and store.claim_holdouts(row)
    assert store.knowledge(OWNER)['used_holdouts'] == []
    assert not store.claim_holdouts(dict(row, id='b' * 32))
    store.settle_holdouts(row, evaluated=False)
    assert store.claim_holdouts(dict(row, id='b' * 32))
    store.settle_holdouts(dict(row, id='b' * 32), evaluated=True)
    assert store.knowledge(OWNER)['used_holdouts'] == row['holdouts']
    assert not store.claim_holdouts(row)


def test_targetless_preferences_never_trigger_goal_completion():
    """Measured ranking preferences are not numeric stop targets or violations."""
    result = evaluate({'gain': 5}, [dict(RULE[0], target=None)])
    assert result['comparable'] and result['hard_targets_met'] and not result['achieved']
    assert not result['goals'][0]['requirement']


def test_consumed_holdout_stays_excluded_from_continued_training(record):
    """Removing a used final window from fresh confirmation cannot expose it to AI training."""
    _, row = record
    row['settings']['scenario_enabled'] = True
    row['holdouts'] = []
    row['training_exclusions'] = [{'start_date':'2020-03-01','end_date':'2020-04-01','exchanges':['binance']}]
    changed=copy.deepcopy(row['initial_config']);changed['backtest'].update(start_date='2020-03-01',end_date='2020-04-01')
    with pytest.raises(ValueError,match='holdout may not enter optimizer training'):
        validate_change(row,changed,{})


def test_loop_overview_options_do_not_load_or_validate_legacy_sources(tmp_path, monkeypatch):
    """Queue/results refreshes cannot surface selected-config migration errors."""
    import api.loop_optimizer_v8 as api_loop
    from api import optimize_v8 as opt
    import vast_queue
    import vast_credentials

    def forbidden(*args, **kwargs):
        raise AssertionError('Overview must not prepare a selected config')

    monkeypatch.setattr(opt, 'list_configs', lambda *args, **kwargs: {'configs': [{'name': 'legacy'}]})
    monkeypatch.setattr(opt, 'get_config', forbidden)
    monkeypatch.setattr(api_loop, '_source_bundle', forbidden)
    monkeypatch.setattr(api_loop, '_controller', SimpleNamespace(backend=SimpleNamespace(initial=forbidden)))
    monkeypatch.setattr(vast_queue, 'CloudQueue', lambda: SimpleNamespace(root=tmp_path, read=lambda: {}))
    monkeypatch.setattr(vast_credentials, 'VastCredentialStore', lambda root: SimpleNamespace(metadata=lambda: {}))
    response = asyncio.run(api_loop.options('legacy', SimpleNamespace(user_id='test-user'),
                                           source_kind='config', source_id='legacy', list_only=True))
    result = json.loads(response.body)
    assert result['configs'] == ['legacy']
    assert result['config_defaults'] is None
    assert result['bundle_digest'] is None


@pytest.mark.parametrize('direction', ['long', 'short'])
def test_suite_disabled_side_uses_same_coin_universe(direction):
    """Suite list normalization leaves risk, bounds and the original input intact."""
    from pb8_loop_backend import normalize_suite_coins
    initial = config()
    initial['backtest']['suite_enabled'] = True
    disabled = 'short' if direction == 'long' else 'long'
    initial['bot'][disabled]['risk']['total_wallet_exposure_limit'] = 0
    initial['live']['approved_coins'] = {direction: ['HYPE'], disabled: []}
    prepared = copy.deepcopy(initial)
    normalize_suite_coins(prepared)
    assert prepared['live']['approved_coins'] == {'long': ['HYPE'], 'short': ['HYPE']}
    assert prepared['bot'] == initial['bot']
    assert prepared['optimize'] == initial['optimize']
    assert initial['live']['approved_coins'][disabled] == []


def test_suite_active_asymmetric_sides_are_rejected():
    """Never broaden the trading universe when both sides are active."""
    from pb8_loop_backend import normalize_suite_coins
    value = config()
    value['backtest']['suite_enabled'] = True
    value['live']['approved_coins']['short'] = ['ETH']
    with pytest.raises(ValueError, match='Both directions are active'):
        normalize_suite_coins(value)
    value['backtest']['suite_enabled'] = False
    normalize_suite_coins(value)
    assert value['live']['approved_coins']['short'] == ['ETH']


@pytest.mark.parametrize('direction', ['long', 'short'])
def test_suite_initial_single_side_can_be_queued(tmp_path, monkeypatch, direction):
    """Real normalization and controller policy agree on a disabled Suite side."""
    from api import optimize_v8 as opt
    initial = config()
    initial['backtest']['suite_enabled'] = True
    monkeypatch.setattr('pb8_loop_backend.migrate_loop_bundle', lambda bundle: copy.deepcopy(bundle))
    monkeypatch.setattr(opt, 'pb8_runtime_status', lambda: {'ready': True})
    backend = LoopBackend(tmp_path, LoopStore(tmp_path / 'loops'))
    monkeypatch.setattr(backend, 'fingerprint', lambda value: {})
    goals = settings()['goals']
    goals.update(direction=direction, coins=['HYPE'])
    async def preflight(owner, selected):
        """Keep model access offline while exercising real controller creation."""
        return {}
    controller = LoopController(tmp_path, store=backend.store, backend=backend,
                                ai=SimpleNamespace(preflight=preflight))
    row = asyncio.run(controller.create(OWNER, settings(goals=goals),
                                       {'config': initial, 'override_configs': {}}, queued=True))
    result = row['initial_config']
    disabled = 'short' if direction == 'long' else 'long'
    assert row['status'] == 'queued'
    assert backend.store.read(OWNER, row['id'])['initial_config'] == result
    assert result['live']['approved_coins'] == {'long': ['HYPE'], 'short': ['HYPE']}
    assert result['bot'][disabled]['risk']['total_wallet_exposure_limit'] == 0
    assert result['bot'][disabled]['risk']['n_positions'] == 0
    assert validate_change(row, copy.deepcopy(result), {}) == result
    for change in ('coins', 'bot', 'bounds', 'fixed', 'symbol'):
        changed = copy.deepcopy(result)
        overrides = {}
        if change == 'coins':
            changed['live']['approved_coins'][disabled] = ['ETH']
        elif change == 'bot':
            changed['bot'][disabled]['risk']['n_positions'] = 1
        elif change == 'bounds':
            changed['optimize']['bounds'][disabled]['risk']['n_positions'] = [0, 1]
        elif change == 'fixed':
            changed['optimize']['fixed_runtime_overrides'] = {f'bot.{disabled}.risk.n_positions': 1}
        else:
            overrides = {'HYPE': {'bot': {disabled: {'risk': {'n_positions': 1}}}}}
        with pytest.raises(ValueError, match='requested coins|Disabled trading direction|position capacity'):
            validate_change(row, changed, overrides)


def test_failed_optimizer_partial_exact_results_still_require_comparison(record, tmp_path):
    """Safety-stopped optimizer results advance to selection, never directly to a winner."""
    store, row = record
    class Backend(FakeBackend):
        """Keep imported exact candidates while retaining the native failure."""
        def poll(self, current, job):
            """Return a stopped optimizer with collected exact results."""
            return {'status': 'failed', 'started': True, 'source': {'error': 'GPU safety check failed'}}
    controller = LoopController(tmp_path, store=store, backend=Backend(), ai=FakeAI())
    job = controller.job(row, 'optimizer', config(), {}, 'Safety stopped')
    job.update(backend={'id': job['operation'], 'execution': 'vast'})
    store.update(OWNER, row['id'], lambda current: current.update(phase='optimize', jobs=[job], rubric=RULE))
    asyncio.run(controller.tick(OWNER, row['id']))
    current = store.read(OWNER, row['id'])
    assert current['phase'] == 'select' and current['candidates']
    assert current['jobs'][0]['status'] == 'failed'
    assert current['jobs'][0]['result_count'] == 1
    assert 'GPU safety check failed' in current['jobs'][0]['error']
    assert not current.get('best')


def test_generated_holdout_survives_real_loop_queue_and_observer(tmp_path, monkeypatch):
    """A contract-1 AI source gives the queued Loop a separate Holdout backtest."""
    from api import optimize_v8 as opt
    from scenario_templates import generate_scenario_template
    preview = generate_scenario_template({'template': 'walk_forward', 'start_date': '2024-12-12',
        'end_date': '2026-10-03', 'window_days': 132, 'stride_days': 132,
        'training_windows': 4, 'holdout_windows': 1,
        'exchange_mode': 'inherit', 'exchanges': ['bybit', 'hyperliquid']})
    initial = config()
    initial['backtest'].update(suite_enabled=True, start_date='2024-12-12', end_date='2026-10-03',
                              scenarios=preview['training_scenarios'], starting_balance=10000)
    initial['pbgui'] = {'scenario_template': preview['provenance']}
    monkeypatch.setattr('pb8_loop_backend.migrate_loop_bundle', lambda bundle: copy.deepcopy(bundle))
    monkeypatch.setattr(opt, 'pb8_runtime_status', lambda: {'ready': True})
    backend = LoopBackend(tmp_path, LoopStore(tmp_path / 'loops'))
    monkeypatch.setattr(backend, 'fingerprint', lambda value: {})
    async def preflight(owner, selected):
        """No provider access while using real Loop creation and holdout extraction."""
        return {}
    controller = LoopController(tmp_path, store=backend.store, backend=backend,
                                ai=SimpleNamespace(preflight=preflight))
    row = asyncio.run(controller.create(OWNER, settings(),
                                       {'config': initial, 'override_configs': {}}, queued=True))
    assert row['holdouts'] and row['training_exclusions'] == row['holdouts']
    assert row['observer_holdouts'] == row['holdouts']
    holdout = backend.observer_config(row, {'config': row['initial_config'], 'overrides': {}}, 'observer_holdout')
    assert holdout['backtest']['start_date'] == '2026-05-25'
    assert holdout['backtest']['end_date'] == '2026-10-03'
    assert all(s['end_date'] < '2026-05-25' for s in row['comparison']['scenarios'])
    assert not holdout.get('pbgui', {}).get('scenario_template')


def test_proxy_progress_persists_missing_and_older_samples(record, tmp_path):
    """Expose completed native counters without inventing zero or losing known progress."""
    from api.loop_optimizer_v8 import projection
    store, row = record
    class Backend(FakeBackend):
        """Return isolated native progress samples without launching any process."""
        sample = None
        def poll(self, current, job):
            """Keep this fixture job running while changing only the sampled counter."""
            return {'status':'running', 'started':True, 'proxy_evaluations':self.sample}
    backend = Backend()
    controller = LoopController(tmp_path, store=store, backend=backend, ai=FakeAI())
    job = controller.job(row, 'optimizer', config(), {}, 'Progress test')
    job.update(backend={'id':'native', 'execution':'gpu'}, status='running', started=True)
    row = store.update(OWNER, row['id'], lambda current: current.update(
        jobs=[job], settings=dict(current['settings'], run_limit_mode='proxy', run_proxy=100000)))
    for sample, expected in [(None,None),(0,0),(12345,12345),(None,12345),(12000,12345),
                             (-1,12345),(True,12345),(50000,50000)]:
        backend.sample = sample
        row = asyncio.run(controller.sync_jobs(store.read(OWNER,row['id']), launch=False))
        exposed = projection(row)['jobs'][0]
        assert exposed['proxy_evaluations'] == expected
        assert exposed['proxy_limit'] == 100000
        assert store.read(OWNER,row['id'])['jobs'][0].get('proxy_evaluations') == expected
    assert not backend.starts and not backend.stops
