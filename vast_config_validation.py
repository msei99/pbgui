"""Pure validation of the pinned Vast worker profile, independent of local CUDA."""
import math
import json
from pathlib import Path

from vast_exchanges import SUPPORTED_EXCHANGES
from vast_scenarios import scenario_plan

SERVICE = 'Vast'
PROFILE_IMAGE = 'ghcr.io/msei99/pbgui-pb8-worker@sha256:b6f61c54b546640f5f00e386c10a27380e0ed8715788bcc8c4b597eedff58dbc'
PROFILE_REVISION = 'ee2b7d49fd53ef790a66a28e2c85f2a6c8faebe8'
_METRIC_CONTRACT = json.loads((Path(__file__).resolve().parent / 'setup/vast_gpu_benchmark/gpu_metric_contract.json').read_text())
METRICS = frozenset(_METRIC_CONTRACT['allowed_metrics'])
if METRICS.intersection(_METRIC_CONTRACT['exact_only_metrics']):
    raise RuntimeError('Invalid Vast GPU metric contract: exact-only metric included')
REDUCERS = {'mean', 'min', 'max', 'std', 'median'}
OPERATORS = {'greater_than', 'greater_than_or_equal', 'less_than', 'less_than_or_equal',
             'equal_to', 'not_equal', 'outside_range', 'inside_range', 'auto'}


def cloud_alternatives(path, message):
    """Offer explicit choices and their tradeoffs without mutating the config."""
    local = 'Use Execution → Local to keep this setup, subject to the local PB8 backend capabilities.'
    if path.endswith('.metric') and 'gain_strategy_eq' in message:
        return ['Choose adg_strategy_eq (goal: max) for average daily growth. This changes the objective; it is not an identical replacement for total-period gain.', local]
    if path.endswith('.metric'):
        return ['Choose a supported metric in the Scoring or Limits editor. Metrics have different meanings; do not transfer thresholds without checking their units.', local]
    if path.endswith('.hsl.enabled'):
        return ['Disable HSL in a separate GPU test configuration. This removes HSL protection and changes the strategy risk behavior.', local]
    if path == 'live.approved_coins':
        return ['Select explicit approved coin lists containing between 1 and 64 distinct coins across both sides.', local]
    if path.startswith('backtest.exchanges'):
        return ['Select supported exchanges: ' + ', '.join(SUPPORTED_EXCHANGES) + '. Local mapped perpetual data is required for every selected coin and BTC.', local]
    if path == 'backtest.btc_collateral_cap':
        return ['Set BTC collateral to 0 in a separate GPU test configuration. This changes collateral exposure.', local]
    if path == 'optimize.gpu.successive_halving.enabled':
        return ['Disable successive halving to evaluate the full history. This may increase computation time.']
    if path.startswith('optimize.bounds'):
        return ['Use a fixed number, [minimum, maximum], or [minimum, maximum, step]. Use step 0 or null for continuous bounds; keep minimum <= maximum.']
    if path == 'live.strategy_kind':
        return ['Create a separate ema_anchor or trailing_martingale configuration with that strategy’s defaults. Do not simply rename the existing strategy.', local]
    if path.startswith('backtest.scenarios') or 'override' in message.lower():
        return ['Check the named scenario and field. Cloud suites export their combined coin/exchange data; GPU parameter restrictions still apply.', local]
    return []


def validate_cloud_config(config, iterations=None, workers=None, *, use_adg=False, image=PROFILE_IMAGE, revision=PROFILE_REVISION, _check_scenarios=True):
    """Collect profile and structural errors without changing or exporting config."""
    errors = []
    def error(path, message):
        """Attach a stable field path for editor highlighting."""
        errors.append({'path':path, 'message':message, 'suggestions':cloud_alternatives(path, message)})
    def obj(value, path):
        """Keep invalid nested types from bypassing the rest of validation."""
        if not isinstance(value, dict):
            error(path, 'Expected an object.')
            return {}
        return value
    def finite(value):
        """Reject booleans, strings and non-finite numeric settings."""
        return type(value) in (int, float) and math.isfinite(value)
    if image != PROFILE_IMAGE or revision != PROFILE_REVISION:
        error('worker.image', 'No validated GPU profile exists for this worker image/revision. Update the compatibility rules before queueing.')
    source = obj(config, 'config')
    live = obj(source.get('live'), 'live')
    bt = obj(source.get('backtest'), 'backtest')
    if bt.get('offline', False) is not False:
        error('backtest.offline', 'The pinned Vast worker does not support PB8 offline provenance. Use Local for offline runs, or explicitly set offline to false.')
    opt = obj(source.get('optimize'), 'optimize')
    bot = obj(source.get('bot'), 'bot')
    if live.get('strategy_kind') not in ('ema_anchor', 'trailing_martingale'):
        error('live.strategy_kind', 'This image supports trailing_martingale and ema_anchor only.')
    for path, value in [('coin_overrides', source.get('coin_overrides')), ('optimize.enable_overrides', opt.get('enable_overrides')),
                        ('backtest.market_settings_sources', bt.get('market_settings_sources'))]:
        if value:
            error(path, 'Cloud export does not support overrides.')
    coins = obj(live.get('approved_coins'), 'live.approved_coins')
    sides = [coins.get(side) for side in ('long', 'short')]
    if any(not isinstance(side, list) or any(not isinstance(coin, str) or not coin.strip() for coin in side) for side in sides):
        error('live.approved_coins', 'Explicit approved coin lists are required.')
    elif not set(sides[0] + sides[1]) or (len(set(sides[0] + sides[1])) > 64 and not (bt.get('suite_enabled') and _check_scenarios)):
        error('live.approved_coins', 'The GPU worker requires 1–64 distinct approved coins across both sides.')
    for side in ('long', 'short'):
        values = obj(bot.get(side), 'bot.' + side)
        hsl = obj(values.get('hsl', {}), 'bot.' + side + '.hsl')
        if hsl.get('enabled'):
            error('bot.' + side + '.hsl.enabled', 'The cloud profile requires HSL disabled.')
    gpu = obj(opt.get('gpu', {}), 'optimize.gpu')
    if gpu.get('drift_rank_halt') is not None:
        error('optimize.gpu.drift_rank_halt', 'The pinned Vast worker does not support a separate rank threshold. Use Local or leave this field blank to inherit drift_halt.')
    if 'drift_objective_tolerance' in gpu and (not finite(gpu['drift_objective_tolerance']) or gpu['drift_objective_tolerance'] != 1e-6):
        error('optimize.gpu.drift_objective_tolerance', 'The pinned Vast worker does not support objective-aware drift checks. Use Local for a custom tolerance; leave the default 0.000001 for cloud compatibility.')
    halving = obj(gpu.get('successive_halving') or {}, 'optimize.gpu.successive_halving')
    if halving.get('enabled'):
        error('optimize.gpu.successive_halving.enabled', 'Successive halving is not supported by this cloud profile.')
    for key in ('population_size', 'batch_size', 'max_dispatch_candidate_bars', 'validate_per_generation', 'drift_probes', 'drift_window', 'drift_min_samples'):
        value = gpu.get(key)
        if value is not None and (type(value) is not int or value < 1):
            error('optimize.gpu.' + key, 'Use a positive integer or null for the image default.')
    for key in ('checkpoint_interval_seconds', 'drift_halt'):
        value = gpu.get(key)
        if value is not None and (not finite(value) or value <= 0):
            error('optimize.gpu.' + key, 'Use a positive finite number or null for the image default.')
    def bounds(values, path):
        """Check nested and flat optimizer bounds without interpreting strategy keys."""
        for key, value in obj(values, path).items():
            child = path + '.' + str(key)
            if isinstance(value, dict):
                bounds(value, child)
            elif finite(value):
                continue  # Native fixed scalar bound.
            elif isinstance(value, list) and 1 <= len(value) <= 3:
                edges = value[:2]
                if not all(finite(v) for v in edges) or (len(edges) == 2 and edges[0] > edges[1]):
                    error(child, 'Bounds require finite minimum/maximum with minimum <= maximum.')
                elif len(value) == 3 and value[2] is not None and (not finite(value[2]) or value[2] < 0):
                    error(child, 'Bound step must be a finite non-negative number or null.')
            else:
                error(child, 'Use a fixed number or bounds with one, two or three entries.')
    if 'bounds' in opt:
        bounds(opt['bounds'], 'optimize.bounds')
    if bt.get('btc_collateral_cap'):
        error('backtest.btc_collateral_cap', 'BTC collateral is not supported by this cloud profile.')
    iterations = opt.get('iters', 512) if iterations is None else iterations
    workers = (gpu.get('exact_workers') or opt.get('n_cpus', 4)) if workers is None else workers
    if type(iterations) is not int or not 256 <= iterations <= 10_000_000:
        error('optimize.iters', 'Choose 256–10,000,000 iterations.')
    if type(workers) is not int or not 1 <= workers <= 64:
        error('optimize.gpu.exact_workers', 'Choose 1–64 CPU workers.')
    exchanges = bt.get('exchanges', [])
    if not isinstance(exchanges, list) or not exchanges:
        error('backtest.exchanges', 'Select at least one cloud exchange: ' + ', '.join(SUPPORTED_EXCHANGES) + '.')
    else:
        for index, exchange in enumerate(exchanges):
            if not isinstance(exchange, str) or exchange not in SUPPORTED_EXCHANGES:
                error(f'backtest.exchanges.{index}', f'Unsupported cloud exchange {exchange!r}. Supported exchanges: ' + ', '.join(SUPPORTED_EXCHANGES) + '. Select an exchange using its standard name.')
    scenarios = bt.get('scenarios', [])
    if not isinstance(scenarios, list):
        error('backtest.scenarios', 'Expected a list of suite scenario objects.')
        scenarios = []
    labels = set()
    for index, scenario in enumerate(scenarios):
        path = f'backtest.scenarios.{index}'
        scenario = obj(scenario, path)
        label = scenario.get('label')
        if not isinstance(label, str) or not label.strip() or label != label.strip() or label in labels:
            error(path + '.label', 'Scenario labels must be unique, non-empty and trimmed.')
        else:
            labels.add(label)
    suite = bool(bt.get('suite_enabled'))
    objective = opt.get('objective_scenario')
    if objective and (not isinstance(objective, str) or not suite or objective not in labels):
        error('optimize.objective_scenario', 'Objective scenario must name an enabled suite scenario.')
    if suite and not scenarios:
        error('backtest.scenarios', 'Enabled suites require scenarios.')
    reducer = bt.get('reducer', {})
    if isinstance(reducer, dict):
        if any(not isinstance(value, str) or value not in REDUCERS for value in reducer.values()):
            error('backtest.reducer', 'Unsupported suite reducer.')
    elif reducer is not None:
        error('backtest.reducer', 'Expected a reducer object.')
    for group in ('scoring', 'limits'):
        entries = opt.get(group, [])
        if not isinstance(entries, list):
            error('optimize.' + group, 'Expected a list.')
            continue
        if group == 'scoring' and not entries:
            error('optimize.scoring', 'Choose at least one scoring objective.')
        for index, entry in enumerate(entries):
            path = f'optimize.{group}.{index}'
            entry = obj(entry, path)
            metric = entry.get('metric')
            if not isinstance(metric, str) or (metric not in METRICS and not (use_adg and group == 'scoring' and metric == 'gain_strategy_eq')):
                error(path + '.metric', 'Unsupported cloud metrics: ' + str(metric) + '.')
            scenario = entry.get('scenario')
            named = isinstance(scenario, str) and bool(scenario.strip())
            if 'scenario' in entry and (not suite or (scenario is not None and (not named or scenario not in labels))):
                error(path + '.scenario', 'Scenario must name an enabled suite scenario, or be null for suite aggregate.')
            basis_key = 'aggregate' if group == 'scoring' else 'stat'
            basis = entry.get('reducer', entry.get(basis_key, ''))
            if 'reducer' in entry and basis_key in entry and entry['reducer'] != entry[basis_key]:
                error(path, 'Conflicting reducer aliases.')
            if basis and (not isinstance(basis, str) or basis not in REDUCERS):
                error(path, 'Unsupported aggregate/stat.')
            if basis and (named or (group == 'scoring' and ('scenario' not in entry and objective or not suite))):
                error(path, 'Aggregate cannot be combined with a named scenario; scoring aggregates require suite mode.')
            if group == 'scoring':
                if entry.get('goal', 'max') not in ('min', 'max'):
                    error(path + '.goal', 'Goal must be min or max.')
            else:
                if 'enabled' in entry and type(entry['enabled']) is not bool:
                    error(path + '.enabled', 'Enabled must be a boolean.')
                operator = entry.get('penalize_if', 'greater_than')
                if not isinstance(operator, str) or operator not in OPERATORS:
                    error(path + '.penalize_if', 'Unsupported penalty operator.')
                if operator in ('outside_range', 'inside_range'):
                    interval = entry.get('range')
                    if not isinstance(interval, list) or len(interval) != 2 or not all(finite(value) for value in interval) or interval[0] > interval[1]:
                        error(path + '.range', 'Provide two finite range values with low <= high.')
                elif not finite(entry.get('value')):
                    error(path + '.value', 'Limit value must be a finite number.')
    if _check_scenarios:
        contexts, _, planning_errors = scenario_plan(source)
        for item in planning_errors:
            error(item['path'], item['message'])
        base_errors = {(item['path'], item['message']) for item in errors}
        if bt.get('suite_enabled'):
            for context in contexts:
                if context['path'] == 'backtest':
                    continue
                for item in validate_cloud_config(context['config'], iterations, workers, use_adg=use_adg,
                                                  image=image, revision=revision, _check_scenarios=False):
                    if (item['path'], item['message']) not in base_errors:
                        error(context['path'] + '.' + item['path'], context['label'] + ': ' + item['message'])
    return errors
