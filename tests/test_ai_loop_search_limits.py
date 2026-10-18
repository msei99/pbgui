"""Independent CPU budgets and TWE ceilings do not prescribe AI experiments."""
import asyncio
import copy
from types import SimpleNamespace

import pytest

from ai_loop_search_limits import exposure_ceiling, validate_exposure_ceiling, validation_budget
from ai_loop_windows import validate_plan
from ai_loop_request import bind_request, validate_request
from api.loop_optimizer_v8 import LoopStart
from pb8_loop_controller import LoopController
from tests.test_ai_loop_windows import plan, request, bundle
from tests.test_pb8_loop_optimizer import OWNER, record
from tests.vast_direction_fixtures import direction_config
from tests.test_ai_loop_tools import environment, CHAT


@pytest.mark.parametrize('interval', [[.01, 8, .01], [1, 4, .01], [4, 4, .01], [8, 8, .01]])
def test_ceiling_permits_ai_chosen_intervals_and_fixed_controls(interval):
    """An upper limit does not silently force [maximum, maximum]."""
    config = direction_config()
    config['optimize']['bounds']['long_total_wallet_exposure_limit'] = interval
    config['bot']['long']['risk']['total_wallet_exposure_limit'] = interval[1]
    before = copy.deepcopy(config)
    validate_exposure_ceiling(config, 8)
    assert config == before


@pytest.mark.parametrize('where', ['base', 'bound', 'runtime', 'runtime_parent', 'scenario'])
def test_effective_exposure_cannot_exceed_user_ceiling(where):
    """A different overlay cannot bypass the user's actual safety limit."""
    config = direction_config()
    if where == 'base':
        config['bot']['long']['risk']['total_wallet_exposure_limit'] = 9
        config['optimize']['bounds']['long_total_wallet_exposure_limit'] = [1, 8]
    elif where == 'bound':
        config['optimize']['bounds']['long_total_wallet_exposure_limit'] = [1, 9]
    elif where == 'runtime':
        config['optimize']['fixed_runtime_overrides']['bot.long.risk.total_wallet_exposure_limit'] = 9
    elif where == 'runtime_parent':
        config['optimize']['fixed_runtime_overrides']['bot.long.risk'] = {'total_wallet_exposure_limit': 9, 'n_positions': 1}
    else:
        config['backtest'].update(suite_enabled=True, scenarios=[{'label': 'training',
            'start_date': '2025-01-01', 'end_date': '2025-02-01',
            'overrides': {'bot.long.risk.total_wallet_exposure_limit': 9}}])
    with pytest.raises(ValueError, match='exceeds the requested TWE ceiling'):
        validate_exposure_ceiling(config, 8)


@pytest.mark.parametrize('field,value', [('max_twe', 0), ('max_twe', True), ('max_twe', float('nan')),
                                       ('max_validations', 0), ('max_validations', True), ('max_validations', 1001)])
def test_template_search_limits_are_typed_and_bounded(field, value):
    """Invalid safety ceilings and total budgets never enter reviewed metadata."""
    with pytest.raises(ValueError):
        validate_plan(dict(plan(), **{field: value}))


def test_original_template_budget_and_ceiling_cannot_be_reinterpreted(record):
    """Later goals cannot replace the original reviewed controls."""
    store, row = record
    value = validate_plan(dict(plan(), max_twe=8, max_validations=137))
    req = request(value)
    row['settings']['source'] = {'loop_request': bind_request(req, store.instruction_version(OWNER, 'default'))}
    row['settings']['max_validations'] = 137
    row['settings']['goals'].update(text=req['text'], presets=[], targets={}, max_twe=8)
    assert exposure_ceiling(row['settings']) == 8
    validate_request(row['settings'], req, config=bundle(value))
    row['settings']['max_validations'] = 200
    with pytest.raises(ValueError, match='reviewed CPU training validation budget'):
        validate_request(row['settings'], req)
    row['settings']['max_validations'] = 137
    row['settings']['goals']['max_twe'] = 9
    with pytest.raises(ValueError, match='reviewed Template ceiling'):
        exposure_ceiling(row['settings'])


def test_legacy_candidate_cap_is_accepted_but_no_longer_prescribes_a_count():
    """Old definitions remain readable; new API settings never export the obsolete cap."""
    parsed = LoopStart(config_name='source', provider='chatgpt', model='test', max_rounds=2, candidates=2)
    assert 'candidates' not in parsed.model_dump()


def test_actual_loop_proposal_preserves_template_controls(environment, monkeypatch):
    """Typed user controls survive the real chat-to-approval boundary unchanged."""
    from unittest.mock import AsyncMock
    from api import loop_optimizer_v8 as api, optimize_v8
    from ai_capabilities import AICapabilityError
    from tests.test_pb8_loop_optimizer import valid_gpu_config
    service, store, _, _ = environment
    value = validate_plan(dict(plan(), max_twe=8, max_validations=137))
    req = request(value)
    config = bundle(value)
    gpu = valid_gpu_config()
    config['bot'], config['live'], config['optimize'] = gpu['bot'], gpu['live'], gpu['optimize']
    monkeypatch.setattr(optimize_v8, 'get_config', lambda *a, **k: {'config': copy.deepcopy(config), 'override_configs': {}})
    chat = api.get_ai_chat_service()
    chat.get_conversation = AsyncMock(return_value={'messages': [{'role': 'user', 'content': req['text']}]})

    async def exercise():
        """Create only an isolated review proposal, then reject contradictory controls."""
        result = await service.dispatch(OWNER, CHAT, 'propose_ai_loop_config', {
            'name': 'reviewed', 'config_name': 'source', 'settings': {
                'max_rounds': 25, 'max_runs': 25, 'execution': 'vast', 'hours': 12, 'parallel': 1}})
        saved = service.proposals[result['proposal_id']].config['settings']
        assert saved['max_validations'] == 137 and saved['goals']['max_twe'] == 8
        assert 'candidates' not in saved
        for changes, message in [({'goals': {'max_twe': 9}}, 'TWE ceiling'),
                                 ({'max_validations': 200}, 'validation budget')]:
            with pytest.raises(AICapabilityError, match=message):
                await service.dispatch(OWNER, CHAT, 'propose_ai_loop_config', {
                    'name': 'invalid', 'config_name': 'source', 'settings': {'max_rounds': 1, **changes}})
        assert not store.definitions(OWNER) and not store.list(OWNER)
    asyncio.run(exercise())


@pytest.mark.parametrize('chosen_count,total,used,success', [(5, 8, 1, True), (1, 8, 1, True), (3, 4, 2, False)])
def test_ai_selects_count_within_total_budget_not_old_per_round_limit(record, tmp_path, chosen_count, total, used, success):
    """Select more than two distinct Pareto candidates without starting real jobs."""
    store, row = record
    candidates = [{'id': f'c{index}', 'optimizer_job': 'optimizer', 'config': copy.deepcopy(row['initial_config']),
                   'overrides': {}, 'metrics': {'gain': index + 1}} for index in range(6)]
    def ready(current):
        """Keep baseline accounting and legacy settings in temporary storage."""
        current.update(phase='select', candidates=candidates, validation_count=used)
        current['settings'].update(candidates=2, max_validations=total, jev_enabled=False)
    row = store.update(OWNER, row['id'], ready)

    async def choose(current, stage, evidence):
        """Return one explicit AI allocation with a reason."""
        assert stage == 'select'
        assert evidence['max_candidates'] == min(6, total - used)
        assert evidence['validation_budget']['used'] == used
        return {'candidate_ids': [item['id'] for item in candidates[:chosen_count]],
                'reason': 'Cover distinct tradeoffs while accounting for later rounds'}

    backend = SimpleNamespace(validation_config=lambda _row, candidate: candidate['config'])
    controller = LoopController(tmp_path, store=store, backend=backend, ai=SimpleNamespace())
    controller.decision = choose
    if success:
        asyncio.run(controller._tick(row))
        saved = store.read(OWNER, row['id'])
        assert saved['validation_count'] == used + chosen_count
        assert len(saved['jobs']) == chosen_count
        assert saved['candidate_selection']['reason']
    else:
        with pytest.raises(ValueError, match='bounded number'):
            asyncio.run(controller._tick(row))
        assert store.read(OWNER, row['id'])['jobs'] == []


def test_user_only_reports_do_not_consume_training_budget(record):
    """Observer counts cannot silently constrain the next candidate selection."""
    _, row = record
    row['settings']['max_validations'] = 50
    row['validation_count'] = 7
    row['observer_jobs'] = [{'kind': 'observer_holdout'}] * 100
    assert validation_budget(row, 80)['max_candidates'] == 43


def test_continue_retains_reviewed_budget_without_round_count_formula(record, monkeypatch):
    """Continue uses the authorized total rather than rounds times a legacy count."""
    import json
    from fastapi.responses import JSONResponse
    from api import loop_optimizer_v8 as api
    from tests.test_pb8_loop_optimizer import _warm_start_observation
    store, row = record
    checkpoint = _warm_start_observation(row, 2, 1)
    checkpoint['operation'] = 'checkpoint'
    def ready(current):
        """Supply a complete exact checkpoint in temporary storage."""
        current.update(best=checkpoint, jobs=[{'operation': 'checkpoint', 'kind': 'validation', 'status': 'completed'}])
        current['settings'].update(max_validations=137, candidates=2)
    row = store.update(OWNER, row['id'], ready)
    captured = []

    async def start(body, session, definition):
        """Create a queue snapshot without starting jobs or contacting providers."""
        captured.append(body.model_dump())
        saved = store.create(OWNER, body.model_dump(), definition['bundle']['config'], {}, {}, [], {}, queued=True)
        return JSONResponse({'id': saved['id']})

    async def action(*_args):
        """Leave every native process untouched."""
        return None

    exact = {'metrics': checkpoint['metrics'], 'assessment': checkpoint['assessment']}
    monkeypatch.setattr(api, '_controller', SimpleNamespace(store=store, backend=SimpleNamespace(observations=lambda *_: exact)))
    monkeypatch.setattr(api, '_owner', lambda _: OWNER)
    monkeypatch.setattr(api, '_start_loop', start)
    monkeypatch.setattr(api, 'loop_action', action)
    monkeypatch.setattr(api, 'projection', lambda value: {'id': value['id']})
    response = asyncio.run(api.continue_loop(row['id'], api.LoopContinue(rounds=5, authorization=True), None))
    assert response.status_code == 201
    assert json.loads(response.body)['id'] != row['id']
    assert captured[0]['max_validations'] == 137
    assert 'candidates' not in captured[0]
