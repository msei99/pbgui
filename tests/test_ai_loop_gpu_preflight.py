"""Issue 449: reject incompatible effective GPU inputs before save or start."""
import asyncio
import copy
from types import SimpleNamespace

import pytest

from pb8_loop_store import LoopStore, validate_cloud_loop_config
from pb8_loop_controller import LoopController
from tests.vast_direction_fixtures import direction_config
from tests.test_pb8_loop_optimizer import OWNER, settings, record
from tests.test_ai_loop_tools import environment, CHAT


def gpu_config():
    """Represent the reported Long-on, Short-off setup with valid initial bounds."""
    config = direction_config()
    config['optimize']['backend'] = 'gpu'
    config['bot']['long']['risk']['total_wallet_exposure_limit'] = 8
    config['optimize']['bounds']['long_total_wallet_exposure_limit'] = [.01, 8, .01]
    return config


@pytest.mark.parametrize('bad', [True, False])
def test_exposure_crossing_zero_is_rejected_without_silent_correction(bad):
    """Keep maximum exposure 8 and require the user to review any correction."""
    config = gpu_config()
    if bad:
        config['optimize']['bounds']['long_total_wallet_exposure_limit'][0] = 0
    before = copy.deepcopy(config)
    if bad:
        with pytest.raises(ValueError, match=r'total_wallet_exposure_limit.*includes 0'):
            validate_cloud_loop_config(config, 'vast')
    else:
        validate_cloud_loop_config(config, 'vast')
    assert config == before


@pytest.mark.parametrize('where', ['runtime', 'scenario'])
def test_effective_overrides_cannot_disable_the_active_side(where):
    """Scenario and runtime overlays use the existing native direction projection."""
    config = gpu_config()
    if where == 'runtime':
        config['optimize']['fixed_runtime_overrides']['bot.long.risk.n_positions'] = 0
    else:
        config['backtest'].update(suite_enabled=True, scenarios=[{
            'label': 'training', 'start_date': '2025-01-01', 'end_date': '2025-02-01',
            'overrides': {'bot.long.risk.n_positions': 0}}])
    with pytest.raises(ValueError, match='GPU configuration rejected'):
        validate_cloud_loop_config(config, 'vast')


@pytest.mark.parametrize('boundary', ['definition', 'queue'])
def test_invalid_snapshot_is_not_saved_or_queued(tmp_path, boundary):
    """Direct editor/API persistence cannot bypass the AI proposal checks."""
    store = LoopStore(tmp_path / 'loops')
    config = gpu_config()
    config['optimize']['bounds']['long_total_wallet_exposure_limit'][0] = 0
    opts = settings(execution='vast')
    with pytest.raises(ValueError, match='includes 0'):
        if boundary == 'definition':
            store.save_definition(OWNER, 'invalid', opts, {'config': config}, {})
        else:
            store.create(OWNER, opts, config, {}, {}, [], config['backtest'], queued=True)
    assert not store.list(OWNER) and not store.definitions(OWNER)


def test_ai_proposal_rejects_bad_bounds_before_approval(environment, monkeypatch):
    """No approval card exists for an invalid assistant-generated GPU source."""
    from ai_capabilities import AICapabilityError
    from api import optimize_v8
    service, store, _, _ = environment
    config = gpu_config()
    config['optimize']['bounds']['long_total_wallet_exposure_limit'][0] = 0
    monkeypatch.setattr(optimize_v8, 'get_config', lambda *a, **k: {'config': copy.deepcopy(config), 'override_configs': {}})
    async def propose():
        """Use the real proposal dispatcher with temporary state and no provider."""
        with pytest.raises(AICapabilityError, match='includes 0'):
            await service.dispatch(OWNER, CHAT, 'propose_ai_loop_config', {
                'name': 'invalid', 'config_name': 'HYPE', 'settings': {'max_rounds': 10, 'execution': 'vast', 'parallel': 1, 'hours': 12}})
    asyncio.run(propose())
    assert not service.proposals and not store.definitions(OWNER)


@pytest.mark.parametrize('action', ['start', 'resume'])
def test_restored_snapshot_is_revalidated_before_start(record, tmp_path, action):
    """A legacy queued or paused invalid run cannot reach baseline work."""
    store, row = record
    config = gpu_config()
    config['optimize']['bounds']['long_total_wallet_exposure_limit'][0] = 0
    def legacy(current):
        """Simulate an old persisted snapshot accepted by a previous version."""
        current.update(initial_config=config, status='queued' if action == 'start' else 'paused')
        current['settings']['execution'] = 'vast'
    store.update(OWNER, row['id'], legacy)
    controller = LoopController(tmp_path, store=store, backend=SimpleNamespace(), ai=SimpleNamespace())
    with pytest.raises(ValueError, match='includes 0'):
        controller.action(OWNER, row['id'], action)
    assert store.read(OWNER, row['id'])['jobs'] == []


def test_unlaunched_preparation_error_is_not_replaced_by_result_attribution(record, tmp_path):
    """Never search optimizer results for an attempt that did not launch."""
    store, row = record
    cause = 'Long exposure (WE) range [0, 8] includes 0 (off) and positive values (on)'
    job = {'operation': 'not-launched', 'kind': 'optimizer', 'round': 0, 'status': 'failed',
           'started': False, 'backend': {'execution': 'vast', 'id': 'partial-preparation'}, 'error': cause}
    row = store.update(OWNER, row['id'], lambda current: current.update(phase='optimize', jobs=[job]))
    def forbidden(*args):
        """Missing results must never obscure the actual rejected field."""
        raise AssertionError('An unlaunched attempt has no optimizer result to attribute')
    controller = LoopController(tmp_path, store=store, backend=SimpleNamespace(candidates=forbidden), ai=SimpleNamespace())
    asyncio.run(controller._tick(row))
    saved = store.read(OWNER, row['id'])
    assert saved['repair_error'] == cause
    assert 'result_error' not in saved['jobs'][0]
