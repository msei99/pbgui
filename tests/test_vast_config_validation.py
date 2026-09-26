"""Pure cloud configuration validation and pre-export rejection tests."""
import copy

import pytest

from vast_config_validation import validate_cloud_config
from vast_jobs import JobStore
from vast_provider import VastError


@pytest.fixture
def config():
    """Provide an ordinary supported config without accessing user files."""
    return {'live':{'strategy_kind':'ema_anchor','approved_coins':{'long':['BTC'],'short':[]}},
            'bot':{'long':{},'short':{}}, 'backtest':{'exchanges':['binance']},
            'optimize':{'iters':512,'n_cpus':4,'scoring':[{'metric':'adg_strategy_eq','goal':'max'}],
                        'limits':[{'metric':'backtest_completion_ratio','penalize_if':'less_than','value':.99}]}}


def test_supported_config_is_not_modified(config):
    """Validation has no side effects or objective rewriting."""
    original = copy.deepcopy(config)
    assert validate_cloud_config(config) == []
    assert config == original


def test_automatic_gpu_fields_can_be_blank(config):
    """Cloud validation leaves GPU sizing for post-rental profile selection."""
    config['optimize']['backend'] = 'gpu'
    config['optimize']['gpu'] = {
        'auto_lean_parallelism': True,
        'population_size': None,
        'batch_size': None,
        'max_dispatch_candidate_bars': None,
    }
    original = copy.deepcopy(config)
    assert validate_cloud_config(config) == []
    assert config == original


@pytest.mark.parametrize('path,value', [
    ('optimize.scoring.0.metric','gain_strategy_eq'), ('optimize.scoring.0.goal','invalid'),
    ('optimize.limits.0.value','nan'), ('optimize.limits.0.penalize_if','invalid'),
    ('optimize.scoring.0.scenario','missing'), ('optimize.limits.0.stat','invalid'),
    ('optimize.iters',10), ('optimize.n_cpus',65), ('live.strategy_kind','other'),
    ('backtest.btc_collateral_cap',.2), ('backtest.exchanges',['unknown-exchange']),
    ('live.approved_coins.long',[]), ('optimize.scoring',{}),
    ('optimize.limits.0.enabled','true'),
])
def test_invalid_fields_are_reported(config, path, value):
    """Invalid syntax and unsupported GPU settings produce actionable paths."""
    parts = path.split('.')
    node = config
    for key in parts[:-1]:
        node = node[int(key)] if isinstance(node, list) else node[key]
    node[int(parts[-1]) if isinstance(node, list) else parts[-1]] = value
    errors = validate_cloud_config(config)
    assert errors
    assert all(item['path'] and item['message'] for item in errors)


def test_collects_errors_and_rejects_before_export(config, tmp_path, monkeypatch):
    """Direct queue preparation cannot bypass the same validator used by the UI."""
    config['optimize']['scoring'][0]['metric'] = 'gain_strategy_eq'
    config['bot']['long']['hsl'] = {'enabled':True}
    assert len(validate_cloud_config(config)) == 1
    from setup.vast_gpu_benchmark import prepare
    def forbidden(*args):
        """Input export must not begin for an invalid configuration."""
        raise AssertionError('Export was called')
    monkeypatch.setattr(prepare, 'select_shards', forbidden)
    root = tmp_path/'vast'
    with pytest.raises(VastError, match='gain_strategy_eq'):
        JobStore(root).prepare('invalid', config, 'hash', tmp_path, tmp_path, tmp_path, 512, 4, False)
    assert not root.exists()


def test_suite_scenario_and_reducer_validation(config):
    """Named scenario references and aggregate combinations remain explicit."""
    config['backtest'].update(suite_enabled=True,scenarios=[{'label':'training','start_date':'2024-01-01','end_date':'2024-02-01'}])
    config['optimize']['scoring'][0].update(scenario='training')
    assert validate_cloud_config(config) == []
    config['optimize']['scoring'][0]['aggregate'] = 'mean'
    assert any('Aggregate' in row['message'] for row in validate_cloud_config(config))


def test_invalid_gpu_tuning_and_bounds(config):
    """Malformed search ranges and GPU sizing fail before paying for a GPU."""
    config['optimize']['gpu'] = {'population_size':-1}
    config['optimize']['bounds'] = {'long':{'entry': [5,2]}}
    assert len(validate_cloud_config(config)) == 2


def test_unknown_image_cannot_reuse_old_rules(config):
    """New image revisions require deliberate compatibility profile maintenance."""
    errors = validate_cloud_config(config, image='different-image')
    assert errors[0]['path'] == 'worker.image'


@pytest.mark.parametrize('bound', [0.5, [0.5], [0, 1], [0, 1, .1], [0, 1, 0], [0, 1, None], [2, 2, .1]])
def test_native_bound_formats_are_accepted(config, bound):
    """Accept native fixed, continuous and stepped PB8 bounds without rewriting."""
    config['optimize']['bounds'] = {'long':{'risk':{'n_positions':bound}}}
    original = copy.deepcopy(config)
    assert validate_cloud_config(config) == []
    assert config == original


@pytest.mark.parametrize('bound', [[], [0, 1, .1, 2], [0, 1, -1], [0, 1, 'bad'], [0, float('inf'), 1], True])
def test_malformed_bounds_still_fail(config, bound):
    """Supporting stepped bounds must not hide genuinely malformed input."""
    config['optimize']['bounds'] = {'long':{'risk':{'n_positions':bound}}}
    errors = validate_cloud_config(config)
    assert errors and errors[0]['path'].endswith('n_positions')


def test_nested_pb8_bounds_leave_only_real_metric_error(config):
    """Reproduce a full nested stepped-bound config rather than two-value mocks."""
    config['optimize']['bounds'] = {
        side:{'forager':{'score_weights_volume':[0,1,.01]}, 'risk':{'n_positions':[1,10,1]},
              'strategy':{strategy:{'entry':{'ema_span_0':[10,1000,1]}}
                          for strategy in ('ema_anchor','trailing_martingale')}}
        for side in ('long','short')}
    config['optimize']['scoring'][0]['metric'] = 'gain_strategy_eq'
    original = copy.deepcopy(config)
    errors = validate_cloud_config(config)
    assert len(errors) == 1
    assert errors[0]['path'] == 'optimize.scoring.0.metric'
    assert 'adg_strategy_eq' in errors[0]['suggestions'][0]
    assert 'changes the objective' in errors[0]['suggestions'][0]
    assert 'Local' in errors[0]['suggestions'][1]
    assert config == original


@pytest.mark.parametrize('count', [1, 2, 64, 65])
def test_multicoin_worker_boundary(config, count):
    """Count distinct approved coins across sides against the pinned worker limit."""
    config['live']['approved_coins'] = {'long':[f'COIN{i}' for i in range(count)], 'short':['COIN0']}
    errors = validate_cloud_config(config)
    assert any(error['path'] == 'live.approved_coins' for error in errors) == (count > 64)


@pytest.mark.parametrize('metric', sorted(__import__('vast_config_validation').METRICS))
@pytest.mark.parametrize('group', ['scoring', 'limits'])
def test_full_gpu_contract_is_allowed_for_scoring_and_limits(config, metric, group):
    """Every name accepted by the worker's GPU validator reaches both editors."""
    config['optimize'][group][0]['metric'] = metric
    assert validate_cloud_config(config) == []


@pytest.mark.parametrize('metric', __import__('vast_config_validation')._METRIC_CONTRACT['exact_only_metrics'])
@pytest.mark.parametrize('group', ['scoring', 'limits'])
def test_exact_only_metrics_remain_rejected(config, metric, group):
    """Expanding GPU options must not allow CPU-only objectives or constraints."""
    config['optimize'][group][0]['metric'] = metric
    errors = validate_cloud_config(config)
    assert any(error['path'] == 'optimize.' + group + '.0.metric' for error in errors)


@pytest.mark.parametrize('strategy', ['ema_anchor', 'trailing_martingale'])
@pytest.mark.parametrize('sides', [('long',), ('short',), ('long', 'short')])
def test_gpu_hsl_survives_cloud_export(config, strategy, sides):
    """Pinned GPU HSL remains enabled with all risk settings unchanged in job copies."""
    from vast_jobs import native_job_config
    config['live']['strategy_kind'] = strategy
    for side in sides:
        config['live']['approved_coins'][side] = ['BTC']
        config['bot'][side]['hsl'] = {
            'enabled': True, 'red_threshold': .2,
            'panic_close_order_type': 'market', 'cooldown_minutes_after_red': 120,
        }
    original = copy.deepcopy(config)
    assert validate_cloud_config(config) == []
    exported = native_job_config(config, 512, 4, False)
    assert exported['bot'] == original['bot']
    assert config == original


def test_invalid_hsl_panic_order_is_rejected(config):
    """Supported HSL does not permit panic order types rejected by the native GPU."""
    config['bot']['long']['hsl'] = {'enabled': True, 'panic_close_order_type': 'invalid'}
    errors = validate_cloud_config(config)
    assert any(e['path'] == 'bot.long.hsl.panic_close_order_type' for e in errors)
