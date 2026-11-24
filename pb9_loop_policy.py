"""Native PB9 policy bindings for the shared autonomous Loop controller."""
from __future__ import annotations

import copy
import math

from loop_runtime import side_policy

SERVICE = 'PB9Loop'
POSITION = 'max_positions'
EXPOSURE = 'total_wallet_exposure_limit_ratio'


def direction(config):
    """Resolve enabled native sides using inherited risk and active search ranges."""
    active = []
    for side in ('long', 'short'):
        policy = side_policy(config, side)
        if policy.get('enabled', False) and upper(config, side, POSITION) > 0 and upper(config, side, EXPOSURE) > 0:
            active.append(side)
    if not active:
        raise ValueError('The starting config disables both directions; select Long, Short or both explicitly')
    return 'both' if len(active) == 2 else active[0]


def risk_range(config, side, key):
    """Resolve inherited native ranges, fixed selectors and step metadata separately."""
    optimize = config.get('optimize') or {}
    runtime = optimize.get('fixed_runtime_overrides') or {}
    side_key, global_key = f'{side}.risk.{key}', f'global.risk.{key}'
    authored = (((config.get('bot') or {}).get(side) or {}).get('risk') or {})
    inherited = key not in authored
    if 'bot.' + side_key in runtime:
        values = runtime['bot.' + side_key]
    elif inherited and 'bot.' + global_key in runtime:
        values = runtime['bot.' + global_key]
    else:
        values = (side_policy(config, side).get('risk') or {}).get(key, 0)
        fixed = {str(value).removeprefix('bot.') for value in optimize.get('fixed_params') or []}
        if side_key not in fixed and not (inherited and global_key in fixed):
            bounds = optimize.get('bounds') or {}
            side_bounds = ((bounds.get(side) or {}).get('risk') or {})
            global_bounds = ((bounds.get('global') or {}).get('risk') or {})
            values = side_bounds.get(key, global_bounds.get(key, values) if inherited else values)
    values = list(values[:2]) if isinstance(values, (list, tuple)) else [values]
    if not values or any(type(value) not in (int, float) or not math.isfinite(value) for value in values):
        raise ValueError('Invalid PB9 direction risk settings')
    return [min(values), max(values)]


def upper(config, side, key):
    """Return the actual high endpoint, excluding a range's optional step."""
    return risk_range(config, side, key)[1]


def initial_goals(config, settings):
    """Pin requested native direction/counts without writing legacy live or risk keys."""
    goals = settings['goals']
    selected = direction(config) if goals['direction'] == 'config' else goals['direction']
    counts = dict(goals.get('positions_by_side') or {})
    total = goals.get('positions')
    if total is not None and not counts:
        counts = ({'long': total, 'short': 0} if selected == 'long' else
                  {'long': 0, 'short': total} if selected == 'short' else
                  {'long': (total + 1) // 2, 'short': total // 2})
    for side in ('long', 'short'):
        active = selected in {'both', side}
        policy = config['bot'].setdefault(side, {})
        if goals['direction'] != 'config':
            policy['enabled'] = active
        if side in counts or not active and goals['direction'] != 'config':
            count = counts.get(side, 0) if active else 0
            policy.setdefault('risk', {})[POSITION] = count
            config['optimize'].setdefault('bounds', {}).setdefault(side, {}).setdefault('risk', {})[POSITION] = [count, count]
            config['optimize'].setdefault('fixed_runtime_overrides', {}).pop(f'bot.{side}.risk.{POSITION}', None)
        if goals.get('coins'):
            policy.setdefault('universe', {})['approved_coins'] = copy.deepcopy(goals['coins']) if active else []
    return config


def hsl_policy(settings, config, *, apply=False):
    """Preserve native scope while applying only the user's explicit HSL switch."""
    mode = settings.get('hsl_mode', 'auto')
    if mode == 'auto':
        return config
    enabled = mode == 'on'
    if mode not in {'on', 'off'}:
        raise ValueError('Unsupported HSL policy')
    for scope in ('portfolio', 'global', 'long', 'short'):
        policy = config['bot'].setdefault(scope, {}).setdefault('risk', {}).setdefault('hsl', {})
        active = enabled and (scope in {'global', 'portfolio'} or direction(config) in {'both', scope})
        selector = f'bot.{scope}.risk.hsl.enabled'
        if apply:
            policy['enabled'] = active
            config['optimize'].setdefault('fixed_runtime_overrides', {})[selector] = active
        elif policy.get('enabled') is not active or (config['optimize'].get('fixed_runtime_overrides') or {}).get(selector, active) is not active:
            raise ValueError('PB9 HSL must preserve the explicit user switch')
    def inspect_patch(patch):
        candidate = copy.deepcopy(config)
        def apply(mapping, prefix=()):
            for key, value in mapping.items():
                path = prefix + tuple(str(key).split('.'))
                if isinstance(value, dict) and '.' not in str(key):
                    apply(value, path)
                elif path[:1] == ('bot',):
                    target = candidate
                    for part in path[:-1]:
                        target = target.setdefault(part, {})
                    target[path[-1]] = copy.deepcopy(value)
        apply(patch)
        for scope in ('portfolio', 'global', 'long', 'short'):
            resolved = side_policy(candidate, scope) if scope in {'long', 'short'} else candidate['bot'].get(scope, {})
            actual = ((resolved.get('risk') or {}).get('hsl') or {}).get('enabled')
            expected = enabled and (scope in {'global', 'portfolio'} or direction(config) in {'both', scope})
            if actual is not expected:
                raise ValueError('Scenario or coin override conflicts with the explicit PB9 HSL switch')
    # Native selectors can replace a leaf or a containing policy object.
    # Replacements must retain the explicitly reviewed switch at every scope.
    for scenario in (config.get('backtest') or {}).get('scenarios') or []:
        inspect_patch(scenario.get('overrides') or {})
    inspect_patch((config.get('optimize') or {}).get('fixed_runtime_overrides') or {})
    for patch in (config.get('coin_overrides') or {}).values():
        inspect_patch(patch)
    return config


def defaults(config):
    """Expose the same editor contract as PB8 using native inherited values."""
    positions = {side: risk_range(config, side, POSITION) for side in ('long', 'short')}
    try:
        selected = direction(config)
    except ValueError:
        selected = 'disabled'
    def universe(field):
        return {side: copy.deepcopy((side_policy(config, side).get('universe') or {}).get(field) or []) for side in ('long', 'short')}
    active = [side for side in positions if selected in {'both', side}]
    return {'direction': selected, 'positions': positions,
            'total_positions': [sum(positions[side][index] for side in active) for index in (0, 1)],
            'coins': universe('approved_coins'), 'ignored_coins': universe('ignored_coins'),
            'iters': (config.get('optimize') or {}).get('iters')}


def validate_change(record, config, overrides):
    """Keep user goals, input ownership and holdouts invariant under native AI edits."""
    from loop_runtime import LoopRuntime
    from pb8_loop_store import scenario_state, windows, validate_cloud_loop_config
    from pb8_loop_config_contract import validate_worker_settings
    initial = record['initial_config']
    if not str(config.get('config_version', '')).startswith('v9.') or overrides:
        raise ValueError('PB9 Loops require a native config with inline coin overrides')
    def protected(old, new, prefix=''):
        keys = set(old or {}) | set(new or {})
        for key in keys:
            left, right = (old or {}).get(key), (new or {}).get(key)
            path = prefix + '.' + key
            if key in {'organic_csv', 'organic_sha256', 'starting_configs', 'starting_config', 'checkpoint', 'coin_sources', 'market_settings_sources', 'user'} or key.endswith(('_path', '_dir')):
                if left != right:
                    raise ValueError('Unreviewed filesystem or account input at ' + path)
            elif isinstance(left, dict) or isinstance(right, dict):
                protected(left if isinstance(left, dict) else {}, right if isinstance(right, dict) else {}, path)
            elif isinstance(left, list) or isinstance(right, list):
                old_rows = left if isinstance(left, list) else []
                new_rows = right if isinstance(right, list) else []
                for index in range(max(len(old_rows), len(new_rows))):
                    before = old_rows[index] if index < len(old_rows) else {}
                    after = new_rows[index] if index < len(new_rows) else {}
                    if isinstance(before, dict) or isinstance(after, dict):
                        protected(before if isinstance(before, dict) else {}, after if isinstance(after, dict) else {}, path + '.' + str(index))
    protected(initial, config)
    LoopRuntime('v9').prepare(config)
    validate_worker_settings(record, config)
    if not record['settings']['scenario_enabled'] and scenario_state(config) != scenario_state(initial):
        raise ValueError('Scenario Editor is disabled; its effective settings must remain unchanged')
    if not record['settings'].get('strategy_enabled', False):
        if (config['bot'].get('portfolio') or {}).get('strategy_kind') != (initial['bot'].get('portfolio') or {}).get('strategy_kind'):
            raise ValueError('Strategy changes are disabled')
    for section, keys in {'backtest': ('ohlcv_source_dir', 'hlcvs_data_dir', 'market_settings_sources'),
                          'optimize': ('starting_configs', 'starting_config', 'checkpoint')}.items():
        if any(config.get(section, {}).get(key) != initial.get(section, {}).get(key) for key in keys):
            raise ValueError('Unreviewed filesystem inputs are not allowed in a loop')
    if config.get('pbgui') != initial.get('pbgui'):
        raise ValueError('Loop execution and provenance settings must remain unchanged')
    def files(value):
        if isinstance(value, dict):
            for key, child in value.items():
                if key in {'approved_coins', 'ignored_coins'} and isinstance(child, str):
                    raise ValueError('Loop coin universes must be explicit lists, never files')
                files(child)
        elif isinstance(value, list):
            for child in value:
                files(child)
    files(config.get('bot') or {})
    files(config.get('coin_overrides') or {})
    files((config.get('backtest') or {}).get('scenarios') or [])
    hsl_policy(record['settings'], config)
    requested = copy.deepcopy(initial)
    initial_goals(requested, record['settings'])
    if direction(config) != direction(requested):
        raise ValueError('PB9 trading direction differs from the reviewed goal')
    for side in ('long', 'short'):
        expected = side_policy(requested, side)
        actual = side_policy(config, side)
        if record['settings']['goals'].get('coins') and (actual.get('universe') or {}).get('approved_coins') != (expected.get('universe') or {}).get('approved_coins'):
            raise ValueError('Configuration must preserve requested coins')
        if record['settings']['goals'].get('positions') is not None or record['settings']['goals'].get('positions_by_side'):
            risk = side_policy(config, side).get('risk') or {}
            ranges = (((config.get('optimize') or {}).get('bounds') or {}).get(side) or {}).get('risk') or {}
            count = upper(requested, side, POSITION)
            if risk.get(POSITION) != count or ranges.get(POSITION) != [count, count] or upper(config, side, POSITION) != count:
                raise ValueError('User position capacity must remain fixed')
    for row in windows(config):
        for holdout in record.get('holdouts', []) + record.get('training_exclusions', []):
            if row['start_date'] <= holdout['end_date'] and holdout['start_date'] <= row['end_date'] and set(row.get('exchanges') or []) & set(holdout.get('exchanges') or row.get('exchanges') or []):
                raise ValueError('The final holdout may not enter optimizer training')
    expected_backend = 'gpu' if record['settings']['execution'] in {'gpu', 'vast'} else 'pymoo'
    if config['optimize'].get('backend') != expected_backend:
        raise ValueError('Configuration must honor the selected execution target')
    from ai_loop_search_limits import exposure_ceiling, validate_exposure_ceiling
    validate_exposure_ceiling(config, exposure_ceiling(record['settings']))
    validate_cloud_loop_config(config, record['settings']['execution'])


def metric_contract():
    """Read optimizer metrics and reducers from the selected native gateway."""
    from loop_runtime import LoopRuntime
    metadata = LoopRuntime('v9').api().native.metadata()
    scoring = metadata['scoring']
    return {'allowed_metrics': list(scoring['metrics']),
            'allowed_reducers': list(scoring.get('reducers') or ['mean', 'min', 'max']),
            'exact_only_metrics': [], 'execution': 'vast', 'runtime': 'pb9',
            'exact_goal_metrics_restricted': False,
            'rule': 'Use native PB9 metrics. CAGR is not ADG or gain. Missing measurements are unavailable, never zero.'}


def validate_cloud(config, execution, metrics_only=False):
    """Use the existing PB9 native GPU/cloud validation rather than PB8 proxies."""
    if execution not in {'gpu', 'vast'}:
        return
    from loop_runtime import LoopRuntime
    api = LoopRuntime('v9').api()
    if execution == 'vast':
        from vast_pb9 import validate
        errors = validate(config)
        if errors:
            raise ValueError('PB9 cloud config rejected: ' + '; '.join(item['path'] + ': ' + item['message'] for item in errors[:8]))
    else:
        api.prepare_pb8_config(config)


def validate_exposure(config, ceiling):
    """Check inherited policy, ranges and every inline scenario/coin risk overlay."""
    if ceiling is None:
        return
    for side in ('long', 'short'):
        if side_policy(config, side).get('enabled', False):
            if upper(config, side, EXPOSURE) > ceiling and not math.isclose(upper(config, side, EXPOSURE), ceiling, rel_tol=1e-12):
                raise ValueError(f'{side} effective TWE exceeds the reviewed ceiling {ceiling:g}')
    def inspect(value):
        if isinstance(value, dict):
            for key, child in value.items():
                if key == EXPOSURE:
                    for number in child if isinstance(child, list) else [child]:
                        if type(number) not in (int, float) or not math.isfinite(number) or number > ceiling:
                            raise ValueError('Inline risk overlay exceeds the reviewed TWE ceiling')
                else:
                    inspect(child)
        elif isinstance(value, list):
            for child in value:
                inspect(child)
    inspect(config.get('coin_overrides') or {})
    inspect((config.get('backtest') or {}).get('scenarios') or [])


def bundle(value):
    """Validate captured native input without PB8 HSL migration or file mutation."""
    from loop_runtime import LoopRuntime
    if value.get('override_configs'):
        raise ValueError('PB9 requires inline coin overrides')
    config = copy.deepcopy(value['config'])
    config = LoopRuntime('v9').prepare(config)
    return {'config': config, 'override_configs': {}}


def metric_catalog():
    """Read supported native metric/reducer names from the current helper contract."""
    from loop_runtime import LoopRuntime
    metadata = LoopRuntime('v9').api().native.metadata()
    metrics = set(metadata['scoring']['metrics'])
    reducers = set(metadata.get('scoring', {}).get('reducers') or ['mean', 'min', 'max'])
    return metrics, reducers
