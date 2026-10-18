"""Template evaluation is freely interpreted while GPU capabilities remain enforced."""
import asyncio
import copy
import json
from types import SimpleNamespace

import pytest

from ai_loop_objectives import validate_objective, validate_template_rubric
from ai_loop_request import bind_request
from ai_loop_windows import validate_plan, verify_config
from pb8_loop_controller import LoopController
from pb8_loop_evidence import LoopAIPause
from pb8_loop_store import LoopStore, validate_change
from tests.test_ai_loop_windows import plan, bundle, request
from tests.test_pb8_loop_optimizer import OWNER, settings, valid_gpu_config

OBJECTIVE = {'metric': 'adg_strategy_eq', 'goal': 'max', 'reducer': 'mean'}
RUBRIC = [{'goal': 'custom', 'metrics': ['adg_strategy_eq'], 'direction': 'max', 'target': None},
          {'goal': 'custom', 'metrics': ['drawdown_worst_strategy_eq'], 'direction': 'min', 'target': .5},
          {'goal': 'custom', 'metrics': ['backtest_completion_ratio'], 'direction': 'max', 'target': 1}]


def reviewed_run(tmp_path):
    """Create only temporary reviewed definitions and config snapshots."""
    store = LoopStore(tmp_path / 'loops')
    value = validate_plan(dict(plan(), objective=OBJECTIVE))
    config = bundle(value)
    gpu = valid_gpu_config()
    config['bot'], config['live'], config['optimize'] = gpu['bot'], gpu['live'], gpu['optimize']
    config['optimize']['scoring'] = [dict(OBJECTIVE)]
    config['optimize']['backend'] = 'gpu'
    req = request(value)
    opts = settings(execution='vast')
    opts['goals'] = dict(opts['goals'], text=req['text'], presets=[], targets={})
    opts['source'] = {'loop_request': bind_request(req, store.instruction_version(OWNER, 'default'))}
    row = store.create(OWNER, opts, config, {}, {}, [], config['backtest'])
    row = store.update(OWNER, row['id'], lambda current: current.update(
        ai_model_profile={'context': 256000, 'output_tokens': 16384}))
    return store, row, value


@pytest.mark.parametrize('change', [
    {'metric': 'not_a_native_cpu_metric'}, {'metric': ['adg_strategy_eq']}, {'metric': 'unknown'},
    {'goal': 'min'}, {'goal': []}, {'reducer': 'max'}, {'reducer': 'median'},
])
def test_window_contract_rejects_unsupported_or_inconsistent_objectives(change):
    """An original request cannot smuggle a GPU-ineligible metric or reducer."""
    with pytest.raises(ValueError, match='CPU evaluation metric'):
        validate_plan(dict(plan(), objective=OBJECTIVE | change))


@pytest.mark.parametrize('field,value', [('metric', 'gain_strategy_eq'), ('metric', 'unknown'),
                                        ('goal', 'invalid'), ('reducer', 'invalid')])
def test_save_and_later_experiments_cannot_replace_reviewed_scoring(tmp_path, field, value):
    """The same validator guards optimizer proposals, saved config and every round."""
    store, row, reviewed = reviewed_run(tmp_path)
    candidate = copy.deepcopy(row['initial_config'])
    candidate['optimize']['scoring'][0][field] = value
    with pytest.raises(ValueError):
        verify_config(reviewed, candidate)
    with pytest.raises(ValueError):
        validate_change(row, candidate, {})
    store.update(OWNER, row['id'], lambda current: current.update(rubric=copy.deepcopy(RUBRIC)))
    restored = LoopStore(store.root).read(OWNER, row['id'])
    assert restored['settings']['source']['loop_request']['window_plan']['objective'] == OBJECTIVE
    validate_template_rubric(restored, restored['initial_config'], restored['rubric'])


@pytest.mark.parametrize('change', ['gain', 'direction', 'alias', 'extra'])
def test_interpretation_freely_chooses_cpu_evaluation(tmp_path, change):
    """An invalid model interpretation pauses before native jobs or history imports."""
    store, row, _ = reviewed_run(tmp_path)
    rubric = copy.deepcopy(RUBRIC)
    if change == 'gain':
        rubric[0]['metrics'] = ['gain_strategy_eq']
    elif change == 'direction':
        rubric[0]['direction'] = 'min'
    elif change == 'alias':
        rubric[0]['metrics'].append('gain_strategy_eq')
    elif change == 'extra':
        rubric.append(dict(RUBRIC[0], metrics=['sharpe_ratio_strategy_eq']))
    else:
        rubric[0]['metrics'] = []
    async def decide(*_args):
        """Supply one controlled interpretation with no provider interaction."""
        return {'rubric': rubric}
    controller = LoopController(tmp_path, store=store, backend=SimpleNamespace(), ai=SimpleNamespace())
    controller.decision = decide
    controller.backend = SimpleNamespace(existing_candidates=lambda _row: [])
    asyncio.run(controller._tick(row))
    current = store.read(OWNER, row['id'])
    assert current['rubric'] == rubric
    assert current['phase'] == 'optimize' and len(current['jobs']) == 1


def test_restored_legacy_gain_goal_remains_gain_with_adaptive_gpu_proxies(tmp_path):
    """CPU-only Gain is a valid evaluation goal and is never replaced by ADG."""
    store, row, _ = reviewed_run(tmp_path)
    req = request(plan())
    row['settings']['source']['loop_request'] = bind_request(req, store.instruction_version(OWNER, 'default'))
    row['settings']['goals']['text'] = req['text']
    row.update(phase='optimize', rubric=[dict(RUBRIC[0], metrics=['gain_strategy_eq'])])
    before = copy.deepcopy(row['rubric'])
    validate_template_rubric(row, row['initial_config'], row['rubric'])
    assert row['rubric'] == before


def test_valid_objective_and_custom_gpu_preferences_match(tmp_path):
    """Default ADG and an explicit supported custom direction use actual CPU reducers."""
    _, row, _ = reviewed_run(tmp_path)
    validate_template_rubric(row, row['initial_config'], RUBRIC)
    assert validate_objective(OBJECTIVE) == OBJECTIVE


def test_matching_interpretation_creates_only_the_requested_optimizer(tmp_path):
    """The exact ADG contract remains executable without broadening its goals."""
    store, row, _ = reviewed_run(tmp_path)
    async def decide(*_args):
        """Return one synthetic matching rubric without any provider calls."""
        return {'rubric': copy.deepcopy(RUBRIC)}
    backend = SimpleNamespace(existing_candidates=lambda _row: [])
    controller = LoopController(tmp_path, store=store, backend=backend, ai=SimpleNamespace())
    controller.decision = decide
    asyncio.run(controller._tick(row))
    current = store.read(OWNER, row['id'])
    assert current['phase'] == 'optimize' and len(current['jobs']) == 1
    assert current['jobs'][0]['config']['optimize']['scoring'] == [OBJECTIVE]
    assert current['rubric'] == RUBRIC


def test_legacy_window_metadata_does_not_pin_cpu_evaluation(tmp_path):
    """Restored Template metadata cannot force ADG or a singleton CPU objective."""
    _, row, _ = reviewed_run(tmp_path)
    rubric = [dict(RUBRIC[0], metrics=['gain_strategy_eq']),
              dict(RUBRIC[0], metrics=['sharpe_ratio_strategy_eq'])]
    validate_template_rubric(row, row['initial_config'], rubric)


def test_restored_optimizer_cannot_run_without_cpu_objective(tmp_path):
    """A malformed/restored phase cannot bypass interpretation with an empty rubric."""
    store, row, _ = reviewed_run(tmp_path)
    row.update(phase='optimize', rubric=[])
    controller = LoopController(tmp_path, store=store, backend=SimpleNamespace(), ai=SimpleNamespace())
    with pytest.raises(LoopAIPause, match='goal rules'):
        asyncio.run(controller._tick(row))


def test_real_optimizer_proposal_rejects_objective_substitution(tmp_path, monkeypatch):
    """Exercise the actual owned conversation and config proposal save boundary."""
    from ai_capabilities import AICapabilityService, AICapabilityError
    import ai_capabilities
    _, row, value = reviewed_run(tmp_path)
    chat = 'b' * 32
    root = tmp_path / 'ai'
    directory = root / 'conversations' / OWNER
    directory.mkdir(parents=True)
    (directory / (chat + '.json')).write_text(json.dumps({
        'owner': OWNER, 'conversation_id': chat,
        'messages': [{'role': 'user', 'content': request(value)['text']}]}))
    monkeypatch.setattr(ai_capabilities, 'load_ini_section', lambda _section: {})
    service = AICapabilityService(root / 'capabilities')
    monkeypatch.setattr(service, '_current_pb8_bundle', lambda _name: (None, {}, None))
    monkeypatch.setattr(service, '_validate_pb8_config', lambda _name, candidate: copy.deepcopy(candidate))
    monkeypatch.setattr(service, '_queue_preview', lambda *_args: {})
    async def scenario():
        """Build proposals in temporary storage and shut down owned resources."""
        try:
            config = copy.deepcopy(row['initial_config'])
            config.pop('pbgui')
            args = {'name': 'reviewed', 'config': config, 'scenario_template': dict(value, template='ai_loop_windows')}
            proposal = await service._propose_pb8_optimizer_config(OWNER, chat, args)
            saved = service.proposals[proposal['proposal_id']]
            assert saved.preview['reviewed_window_plan']['objective'] == OBJECTIVE
            config['optimize']['scoring'][0]['metric'] = 'gain_strategy_eq'
            with pytest.raises(AICapabilityError):
                await service._propose_pb8_optimizer_config(OWNER, chat, args)
            config['optimize']['scoring'][0]['metric'] = 'adg_strategy_eq'
            config['optimize']['bounds']['long_total_wallet_exposure_limit'][0] = 0
            with pytest.raises(AICapabilityError, match='includes 0'):
                await service._propose_pb8_optimizer_config(OWNER, chat, args)
        finally:
            await service.shutdown()
    asyncio.run(scenario())
