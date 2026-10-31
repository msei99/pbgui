"""Private durable PB8 loop state, experiment policy and reusable evidence."""
from __future__ import annotations

import copy
from collections import OrderedDict
import hashlib
import json
import math
from pathlib import Path
import re
import time
import threading
import uuid

from file_lock import advisory_file_lock
from secure_files import atomic_write_private_text, ensure_private_directory, read_regular_file_nofollow
from pb8_loop_storage import LoopArchive, LoopStorageError, MAX_MANIFEST_BYTES, HOT_FIELDS, manifest

SERVICE = 'PB8Loop'
TERMINAL = {'completed', 'unconfirmed', 'stopped', 'failed'}
ACTIVE = {'running', 'finishing'}
MAX_JSON_BYTES = 32 * 1024 * 1024
READ_CACHE_BYTES = 128 * 1024 * 1024
READ_CACHE_TTL = 30


def round_limit(settings):
    """Require an explicit persisted round ceiling before any automatic work."""
    value = settings.get('max_rounds')
    attempts = settings.get('max_runs')
    if type(value) is not int or type(attempts) is not int or not 1 <= value <= attempts <= 200:
        raise ValueError('Configure Maximum completed rounds (1 to maximum optimizer attempts) before starting the loop')
    return value


def optimizer_execution_evidence(job, settings, record=None):
    """Explain intentional quota cancellation without disguising real interruptions."""
    result = {key: copy.deepcopy(job.get(key)) for key in
              ('operation', 'status', 'reason', 'error', 'config', 'executed_config', 'runner',
               'limit_reached', 'proxy_evaluations', 'result_count', 'results_consumed')}
    result['backend_status'] = job.get('status')
    count, target = job.get('proxy_evaluations'), settings.get('run_proxy')
    quota_complete = (settings.get('run_limit_mode') == 'proxy' and job.get('limit_reached') == 'proxy'
                      and type(count) is int and type(target) is int and count >= target
                      and job.get('status') in {'cancelled', 'stopped', 'complete', 'completed'}
                      and not job.get('error') and job.get('results_consumed') is True
                      and (job.get('result_count') or 0) > 0)
    result.update(status='completed' if quota_complete else job.get('status'),
                  completion_reason='proxy_limit_reached' if quota_complete else None,
                  completed_evolution_proxy=count,
                  proxy_overshoot=max(0, count - target) if type(count) is int and type(target) is int else None)
    from pb8_loop_gpu_replacement import replaced_predecessor
    replacement = replaced_predecessor(record or {}, job)
    if replacement:
        result.update(status=job.get('status'), completion_reason='gpu_replaced',
                      infrastructure_interruption=True, replacement_operation=replacement['id'],
                      successor_operation=replacement.get('successor'),
                      attempt_charged=True,
                      infrastructure_reason='Authorized GPU replacement; preserved input retries in the same logical round')
    return result


def completed_round_observations(record, number):
    """Recover exact checkpoints even if AI evaluation never reached history commit."""
    history = next((item for item in record['history'] if item['round'] == number), None)
    if history is not None:
        evidence = history.get('observations') or []
    elif number == record['round'] and record['phase'] == 'evaluate':
        evidence = [item for item in record.get('observations', [])
                    if item.get('exact') and item['assessment'].get('simulation_complete') is True]
    else:
        evidence = []
    successful = {job['operation'] for job in record['jobs']
                  if job['kind'] == 'validation' and job['round'] == number
                  and job['status'] in {'completed', 'complete'}}
    return [item for item in evidence if item.get('operation') in successful
            and item['assessment'].get('comparable', True)
            and item['assessment'].get('simulation_complete', True)]


class LoopDeletionConflict(ValueError):
    """A run still owns active work and cannot be removed yet."""


class LoopInstructionConflict(ValueError):
    """Instruction selection changed since the client last read it."""


def run_deletable(record):
    """Only untouched waiting runs and fully collected terminal runs may be deleted."""
    if record['status'] == 'queued':
        return not record['jobs']
    finished = {'completed', 'complete', 'failed', 'error', 'cancelled', 'stopped'}
    return (record['status'] in TERMINAL and bool(record.get('cleanup_done'))
            and all(job['status'] in finished | {'skipped'}
                    and (job['status'] not in {'completed', 'complete'} or job.get('report_done'))
                    for job in record.get('observer_jobs', []))
            and all(not job.get('backend') or job['status'] in finished for job in record['jobs']))


def identifier(value):
    """Accept only opaque owned identifiers, never paths or selectors."""
    if not isinstance(value, str) or not re.fullmatch(r'[a-f0-9]{32}', value):
        raise ValueError('Invalid loop or owner ID')
    return value


def digest(value):
    """Fingerprint a finite JSON value for provenance and idempotency."""
    return hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False).encode()).hexdigest()


def bounded_json(value):
    """Reject oversized, nonfinite and secret-bearing model/config data."""
    encoded = json.dumps(value, allow_nan=False)
    if len(encoded.encode()) > 512 * 1024:
        raise ValueError('Configuration or decision exceeds 512 KB')
    def walk(item, depth=0):
        if depth > 24:
            raise ValueError('Configuration nesting exceeds the supported limit')
        if isinstance(item, dict):
            for key, child in item.items():
                if not isinstance(key, str) or re.search(r'password|private_key|api_key|secret|session_token|auth_token', key, re.I):
                    raise ValueError('Credentials are not permitted in loop data')
                walk(child, depth + 1)
        elif isinstance(item, list):
            for child in item:
                walk(child, depth + 1)
        elif isinstance(item, str) and any(ord(char) < 32 and char not in '\n\r\t' for char in item):
            raise ValueError('Control characters are not permitted')
    walk(value)
    return copy.deepcopy(value)


def scenario_state(config):
    """Include inherited values so disabled editor protection cannot be bypassed."""
    bt = config.get('backtest') or {}
    return {'backtest': {key: copy.deepcopy(bt.get(key)) for key in
            ('scenarios', 'suite_enabled', 'scenario_aggregation', 'suite_reducer', 'suite_metrics_reducer', 'start_date', 'end_date', 'exchanges',
             'starting_balance', 'coin_sources', 'market_settings_sources', 'maker_fee_override', 'taker_fee_override',
             'market_order_slippage_pct', 'liquidation_threshold', 'filter_by_min_effective_cost', 'dynamic_wel_by_tradability')},
            'template': copy.deepcopy((config.get('pbgui') or {}).get('scenario_template'))}


def windows(config):
    """Project effective training windows, including inherited exchanges/dates."""
    bt = config.get('backtest') or {}
    rows = bt.get('scenarios') if bt.get('suite_enabled') else None
    return [{**{key: bt.get(key) for key in ('start_date', 'end_date', 'exchanges')}, **row}
            for row in (rows or [{}])]


def configured_direction(config):
    """Resolve active PB8 optimizer sides from the configured risk search space."""
    optimize = config.get('optimize') or {}
    fixed = {str(path).removeprefix('bot.') for path in optimize.get('fixed_params', [])}
    overrides = optimize.get('fixed_runtime_overrides') or {}
    active = []
    for side in ('long', 'short'):
        risk = (config.get('bot', {}).get(side) or {}).get('risk') or {}
        bounds = (optimize.get('bounds', {}).get(side) or {}).get('risk') or {}
        def upper(key):
            selector = f'{side}.risk.{key}'
            raw = overrides.get('bot.' + selector, risk.get(key, 0))
            if 'bot.' + selector not in overrides and selector not in fixed:
                raw = bounds.get(key, raw)
            values = raw if isinstance(raw, (list, tuple)) else [raw]
            try:
                return max(float(value) for value in values)
            except (ValueError, TypeError):
                raise ValueError('The starting config has invalid direction risk settings') from None
        if upper('n_positions') > 0 and upper('total_wallet_exposure_limit') > 0:
            active.append(side)
    if not active:
        raise ValueError('The starting config disables both directions; choose Long, Short or both explicitly')
    return 'both' if len(active) == 2 else active[0]


def config_defaults(config):
    """Project inherited loop input values without disclosing the entire source config."""
    live = config.get('live') or {}
    optimize = config.get('optimize') or {}
    fixed = {str(path).removeprefix('bot.') for path in optimize.get('fixed_params', [])}
    overrides = optimize.get('fixed_runtime_overrides') or {}
    positions = {}
    for side in ('long', 'short'):
        selector = f'{side}.risk.n_positions'
        raw = (config.get('bot', {}).get(side, {}).get('risk') or {}).get('n_positions', 0)
        if 'bot.' + selector in overrides:
            raw = overrides['bot.' + selector]
        elif selector not in fixed:
            raw = (optimize.get('bounds', {}).get(side, {}).get('risk') or {}).get('n_positions', raw)
        values = raw if isinstance(raw, (list, tuple)) else [raw]
        positions[side] = [min(values), max(values)]
    try:
        direction = configured_direction(config)
    except ValueError:
        direction = 'disabled'
    active = [side for side in ('long', 'short') if direction in {'both', side}]
    total = [sum(positions[side][index] for side in active) for index in (0, 1)]
    def coins(field):
        value = live.get(field) or {}
        return {side: copy.deepcopy(value.get(side, []) if isinstance(value, dict) else value) for side in ('long', 'short')}
    return {'coins': coins('approved_coins'), 'ignored_coins': coins('ignored_coins'),
            'direction': direction, 'positions': positions, 'total_positions': total, 'iters': optimize.get('iters')}


def optimizer_start_candidate(record):
    """Identify the qualified comparison reference; never create a seed population."""
    candidate = {'id': 'baseline', 'config': record['initial_config'],
                 'overrides': record['initial_overrides'], 'optimizer_job': None}
    best = record.get('best') or {}
    if qualified_winner(best) and isinstance(best.get('candidate'), dict):
        candidate = best['candidate']
    return {'candidate_id': candidate['id'], 'optimizer_job': candidate.get('optimizer_job'),
            'bot': copy.deepcopy(candidate['config']['bot']),
            'strategy_kind': candidate['config']['live'].get('strategy_kind', 'trailing_martingale'),
            'overrides': copy.deepcopy(candidate.get('overrides', {}))}


def fresh_optimizer_config(config, seed=None):
    """Drop inherited populations/resumes without changing the experiment's search space."""
    result = copy.deepcopy(config)
    optimize = result.setdefault('optimize', {})
    for key in ('starting_configs', 'starting_config', 'checkpoint'):
        optimize.pop(key, None)
    if seed is not None:
        optimize['seed'] = seed
    pbgui = result.setdefault('pbgui', {})
    pbgui['optimize_runtime'] = {'mode': 'fresh'}
    for key in ('optimize_seed_mode', 'optimize_seed_path', 'starting_config'):
        pbgui.pop(key, None)
    return result


def hsl_policy_sides(settings, config):
    """Resolve the user-pinned HSL switch, enabling only active trading sides."""
    mode = settings.get('hsl_mode', 'auto')
    if mode not in {'on', 'off', 'auto'}:
        raise ValueError('HSL must be On, Off or Auto')
    if mode == 'auto':
        return {}
    direction = settings['goals']['direction']
    if direction == 'config':
        direction = configured_direction(config)
    return {side: mode == 'on' and direction in {'both', side} for side in ('long', 'short')}


def apply_hsl_policy(settings, config, overrides):
    """Apply an explicit HSL choice to a private initial bundle, including coin patches."""
    config, overrides = copy.deepcopy(config), copy.deepcopy(overrides)
    for side, enabled in hsl_policy_sides(settings, config).items():
        config['bot'][side].setdefault('hsl', {})['enabled'] = enabled
        optimize = config.setdefault('optimize', {})
        runtime = optimize.setdefault('fixed_runtime_overrides', {})
        _hsl_override_values(runtime, side, replacement=enabled)
        runtime[f'bot.{side}.hsl.enabled'] = enabled
        optimize.get('bounds', {}).get(side, {}).get('hsl', {}).pop('enabled', None)
        patches = list(overrides.values()) + [row.get('overrides') or {} for row in
                                             config.get('backtest', {}).get('scenarios', [])]
        for override in patches:
            _hsl_override_values(override, side, replacement=enabled)
    return config, overrides


def _hsl_override_values(override, side, replacement=None):
    """Read or replace HSL leaves, including dotted parent-subtree overrides."""
    found = []
    def walk(value, prefix=''):
        for key, child in list(value.items()):
            key_path = key.strip()
            path = prefix + '.' + key_path if prefix else key_path
            if path == f'bot.{side}.hsl.enabled':
                found.append(child)
                if replacement is not None:
                    value[key] = replacement
            elif isinstance(child, dict):
                walk(child, path)
    walk(override)
    return found


def validate_hsl_policy(record, config, overrides):
    """Reject AI/candidate attempts to bypass a fixed HSL switch at any effective layer."""
    for side, enabled in hsl_policy_sides(record['settings'], record['initial_config']).items():
        path = f'bot.{side}.hsl.enabled'
        if config['bot'][side].get('hsl', {}).get('enabled') is not enabled:
            raise ValueError(f'HSL {record["settings"]["hsl_mode"]} must remain fixed for {side}')
        runtime = config.get('optimize', {}).get('fixed_runtime_overrides', {})
        if any(value is not enabled for value in _hsl_override_values(runtime, side)):
            raise ValueError('A runtime override changes the fixed HSL mode')
        bounds = config.get('optimize', {}).get('bounds', {}).get(side, {}).get('hsl', {})
        if 'enabled' in bounds:
            values = bounds['enabled']
            if not isinstance(values, list) or any(value != enabled for value in values):
                raise ValueError('Search bounds change the fixed HSL mode')
        patches = list(overrides.values()) + [row.get('overrides') or {} for row in
                                             config.get('backtest', {}).get('scenarios', [])]
        for override in patches:
            if any(value is not enabled for value in _hsl_override_values(override, side)):
                raise ValueError('A coin or scenario override changes the fixed HSL mode')


def apply_optimizer_start(record, config, overrides):
    """Validate the AI's complete experiment without overwriting proposed bot edits."""
    start = optimizer_start_candidate(record)
    config, overrides = copy.deepcopy(config), copy.deepcopy(overrides)
    # The incumbent is comparison evidence only. A complete proposal may change
    # even a value back to its initial default; do not infer intent or copy a bot
    # from an earlier round into the new experiment.
    compatible = config['live'].get('strategy_kind', 'trailing_martingale') == start['strategy_kind']
    config = apply_run_limit(record['settings'], config)
    validate_change(record, config, overrides)
    return config, overrides, {'candidate_id': start['candidate_id'] if compatible else None,
                              'optimizer_job': start['optimizer_job'] if compatible else None,
                              'bot_modified': config['bot'] != start['bot'],
                              'overrides_modified': overrides != start['overrides'],
                              'reason': 'Fresh random population; incumbent retained for comparison only' if compatible
                              else 'Explicit strategy change uses the proposed compatible bot'}


def apply_run_limit(settings, config):
    """Copy the config and pin explicit native iteration limits for every optimizer job."""
    result = copy.deepcopy(config)
    mode = settings.get('run_limit_mode', 'config')
    if mode != 'config':
        result.setdefault('optimize', {})['iters'] = settings['run_iters'] if mode == 'iters' else 10_000_000
    # GPU completion timestamps have finite precision. A strict 1.0 boundary
    # can classify a fully completed proxy differently from Rust (0.999994662
    # versus 1.0). Only the private optimizer copy uses the previously used 0.99 floor;
    # saved configs and independent exact-comparison completeness stay intact.
    if settings.get('execution') in {'vast', 'gpu'}:
        for limit in result.get('optimize', {}).get('limits', []):
            if (isinstance(limit, dict) and limit.get('enabled', True)
                    and limit.get('metric') == 'backtest_completion_ratio'
                    and limit.get('penalize_if') == 'less_than'
                    and limit.get('value') == 1.0):
                limit['value'] = 0.99
    return result


def validate_gpu_drift(config):
    """Check PB8's static drift-evidence constraints without importing CUDA."""
    gpu = config['optimize'].get('gpu') or {}
    if not isinstance(gpu, dict):
        raise ValueError('optimize.gpu must be an object')
    values = {}
    for key, default in {'validate_per_generation': 8, 'drift_probes': 4,
                         'drift_window': 128, 'drift_min_samples': 32}.items():
        value = gpu.get(key)
        value = default if value is None else value
        if type(value) is not int or value < (0 if key == 'drift_probes' else 1):
            raise ValueError(f'optimize.gpu.{key} must be a valid integer')
        values[key] = value
    validations, probes = values['validate_per_generation'], values['drift_probes']
    if probes >= validations:
        raise ValueError('optimize.gpu.drift_probes must be less than optimize.gpu.validate_per_generation')
    halt = gpu.get('drift_halt')
    halt = .60 if halt is None else halt
    if type(halt) not in (int, float) or not math.isfinite(halt) or not 0 < halt <= 1:
        raise ValueError('optimize.gpu.drift_halt must be greater than zero and at most one')
    # PB8 v8.6 requires eight true-front samples even for a one-member front,
    # plus enough broad probes to retain eight rank-comparable observations.
    required = 8 * validations
    if probes:
        samples = math.floor(7 / halt) + 1
        required = max(required, samples, math.ceil(samples * validations / probes))
    required = max(required, values['drift_min_samples'])
    if values['drift_window'] < required:
        raise ValueError(f'optimize.gpu.drift_window must be at least {required} for the configured validation/probe allocation')


def cloud_loop_metric_contract():
    """Expose the pinned worker's metric eligibility, separate from exact goals."""
    from vast_config_validation import METRICS, REDUCERS, PROFILE_REVISION, _METRIC_CONTRACT
    return {'execution': 'vast', 'worker_revision': PROFILE_REVISION,
            'allowed_metrics': sorted(METRICS),
            'allowed_reducers': sorted(REDUCERS),
            'exact_only_metrics': sorted(_METRIC_CONTRACT['exact_only_metrics']),
            'applies_to': ['optimize.scoring', 'optimize.limits'],
            'exact_goal_metrics_restricted': False,
            'rule': 'Use only allowed_metrics and allowed_reducers for Vast.ai optimizer scoring and limits. '
                    'worst is an exact-goal evidence label, not an optimizer reducer. '
                    'For a worst-window upper limit use max; for a worst-window lower limit use min. '
                    'Exact-only metrics remain usable in the exact comparison rubric and results. '
                    'There is no automatic CPU fallback for an unsupported GPU objective.'}


def validate_cloud_loop_metrics(config, execution):
    """Reject unsupported cloud objectives before approval or job preparation."""
    if execution != 'vast':
        return
    from vast_config_validation import METRICS, REDUCERS, cloud_alternatives
    for group in ('scoring', 'limits'):
        for index, entry in enumerate(config.get('optimize', {}).get(group) or []):
            metric = entry.get('metric') if isinstance(entry, dict) else entry
            if not isinstance(metric, str) or metric not in METRICS:
                path = f'optimize.{group}.{index}.metric'
                message = f'Unsupported cloud metrics: {metric}.'
                alternatives = ' '.join(cloud_alternatives(path, message))
                raise ValueError(f'Vast.ai {path}: {message} {alternatives}')
            if isinstance(entry, dict):
                alias = 'aggregate' if group == 'scoring' else 'stat'
                if 'reducer' in entry and alias in entry and entry['reducer'] != entry[alias]:
                    raise ValueError(f'Vast.ai optimize.{group}.{index}: Conflicting reducer aliases')
                basis_key = 'reducer' if 'reducer' in entry else alias
                basis = entry.get(basis_key)
                if basis and (not isinstance(basis, str) or basis not in REDUCERS):
                    raise ValueError(f'Vast.ai optimize.{group}.{index}.{basis_key}: '
                                     f'Unsupported optimizer reducer {basis!r}; choose from {sorted(REDUCERS)}. '
                                     'Use max for a worst-window upper limit or min for a worst-window lower limit.')


def validate_cloud_loop_config(config, execution):
    """Use the native effective GPU/scenario preflight before approval and execution."""
    validate_cloud_loop_metrics(config, execution)
    if execution == 'vast':
        from vast_config_validation import validate_cloud_config
        errors = validate_cloud_config(config)
    elif execution == 'gpu':
        from vast_direction_validation import validate_gpu_directions
        errors, _warnings = validate_gpu_directions(config)
    else:
        return
    if errors:
        raise ValueError('GPU configuration rejected: ' + '; '.join(
            item['path'] + ': ' + item['message'] for item in errors[:8]))


def validate_change(record, config, overrides):
    """Permit free optimizer edits while preserving user intent and editor state."""
    bounded_json(config)
    bounded_json(overrides)
    from ai_loop_windows import bound_plan, verify_config
    plan = bound_plan(record['settings'])
    if plan is not None:
        verify_config(plan, config)
        from ai_loop_objectives import validate_template_rubric
        validate_template_rubric(record, config, record.get('rubric'))
    if not all(isinstance(config.get(key), dict) for key in ('backtest', 'bot', 'live', 'optimize')):
        raise ValueError('A complete PB8 optimizer configuration is required')
    from ai_loop_search_limits import exposure_ceiling, validate_exposure_ceiling
    validate_exposure_ceiling(config, exposure_ceiling(record['settings']))
    validate_hsl_policy(record, config, overrides)
    from pb8_metric_dependencies import metric_dependency_errors
    dependency_errors = metric_dependency_errors(config, overrides)
    if dependency_errors:
        first = dependency_errors[0]
        raise ValueError(first['path'] + ': ' + first['message'] + ' Preserve the reviewed HSL policy; choose a compatible metric.')
    if not record['settings']['scenario_enabled'] and scenario_state(config) != scenario_state(record['initial_config']):
        raise ValueError('Scenario Editor is disabled; its effective settings must remain unchanged')
    initial = record['initial_config']
    def strategy_fields(value, prefix=''):
        found = {}
        if isinstance(value, dict):
            for key, item in value.items():
                path = prefix + '.' + key
                if key == 'strategy_kind' or key.endswith('.strategy_kind'):
                    found[path] = item
                found.update(strategy_fields(item, path))
        elif isinstance(value, list):
            for index, item in enumerate(value):
                found.update(strategy_fields(item, prefix + '.' + str(index)))
        return found
    if not record['settings'].get('strategy_enabled', False):
        if strategy_fields(config) != strategy_fields(initial) or strategy_fields(overrides) != strategy_fields(record['initial_overrides']):
            raise ValueError('Strategy changes are disabled; strategy_kind must remain unchanged, including overrides')
    elif config['live'].get('strategy_kind', 'trailing_martingale') not in {'ema_anchor', 'trailing_martingale'}:
        raise ValueError('Choose a supported PB8 strategy_kind')
    # Filesystem inputs are resolved only from the reviewed initial bundle.
    for section, keys in {'backtest': ('ohlcv_source_dir', 'ohlcv_cache_dir', 'market_settings_sources'),
                          'optimize': ('starting_configs', 'starting_config', 'checkpoint')}.items():
        for key in keys:
            if config[section].get(key) != initial.get(section, {}).get(key):
                raise ValueError('Unreviewed filesystem inputs are not allowed in a loop')
    for field in ('approved_coins', 'ignored_coins'):
        value = config['live'].get(field)
        previous = initial.get('live', {}).get(field)
        if isinstance(value, str) and value != previous:
            raise ValueError('Coin selections must use inline lists, not new filesystem references')
        if isinstance(value, dict):
            for side, selection in value.items():
                inherited = previous.get(side) if isinstance(previous, dict) else None
                if isinstance(selection, str) and selection != inherited:
                    raise ValueError('Coin selections must use inline lists, not new filesystem references')
    expected = 'gpu' if record['settings']['execution'] in {'gpu', 'vast'} else 'pymoo'
    if config['optimize'].get('backend') != expected:
        raise ValueError('Configuration must honor the selected execution target')
    validate_cloud_loop_config(config, record['settings']['execution'])
    if expected == 'gpu':
        validate_gpu_drift(config)
    for row in windows(config):
        for holdout in record.get('holdouts', []) + record.get('training_exclusions', []):
            overlap = row['start_date'] <= holdout['end_date'] and holdout['start_date'] <= row['end_date']
            if overlap and set(row.get('exchanges') or []) & set(holdout.get('exchanges') or row.get('exchanges') or []):
                raise ValueError('The final holdout may not enter optimizer training')
    goals = record['settings']['goals']
    use_config = goals['direction'] == 'config'
    direction = configured_direction(initial) if use_config else goals['direction']
    if use_config and configured_direction(config) != direction:
        raise ValueError('Use config must preserve the starting configuration trading direction')
    coins = goals.get('coins') or []
    if coins:
        approved = config['live'].get('approved_coins') or {}
        for side in ('long', 'short'):
            # Suite uses one shared coin universe even for a disabled side.
            # Trading direction remains pinned by the risk/bounds checks below.
            shared_universe = config['backtest'].get('suite_enabled', False)
            expected_coins = sorted(coins) if shared_universe or direction in {'both', side} else []
            if not isinstance(approved, dict) or sorted(approved.get(side) or []) != expected_coins:
                raise ValueError('Configuration must preserve requested coins and trading direction')
    if coins:
        for window in windows(config):
            selected = window.get('coins')
            if selected is not None:
                values = sum((value for value in selected.values() if isinstance(value, list)), []) if isinstance(selected, dict) else selected
                if not isinstance(values, list) or not set(values) <= set(coins):
                    raise ValueError('Scenario coins must stay within the requested coin universe')
    for side in ('long', 'short'):
        if not use_config and direction not in {'both', side}:
            value = config['bot'][side].get('risk', {}).get('n_positions', 0)
            bounds = config['optimize'].get('bounds', {}).get(side, {}).get('risk', {}).get('n_positions', [value, value])
            if value != 0 or any(float(v) != 0 for v in bounds):
                raise ValueError('Disabled trading direction must remain disabled')
    total = goals.get('positions')
    sides = ('long', 'short') if direction == 'both' else (direction,)
    if total is not None and sum(config['bot'][s].get('risk', {}).get('n_positions', 0) for s in sides) > total:
        raise ValueError('Configured position capacity exceeds the requested total')
    expected_counts = goals.get('positions_by_side') or {}
    if total is not None and not expected_counts:
        expected_counts = {'long': total, 'short': 0} if direction == 'long' else (
            {'long': 0, 'short': total} if direction == 'short' else {'long': (total + 1) // 2, 'short': total // 2})
    for side in ('long', 'short'):
        expected_count = expected_counts.get(side) if direction in {'both', side} else (0 if not use_config or total is not None else None)
        if expected_count is None:
            continue
        risk = config['bot'][side].get('risk') or {}
        bounds = config['optimize'].get('bounds', {}).get(side, {}).get('risk', {}).get('n_positions', [risk.get('n_positions'), risk.get('n_positions')])
        fixed = config['optimize'].get('fixed_runtime_overrides', {}).get(f'bot.{side}.risk.n_positions', expected_count)
        if risk.get('n_positions') != expected_count or bounds != [expected_count, expected_count] or fixed != expected_count:
            raise ValueError('User position capacity must remain fixed in config, search bounds and runtime overrides')
        for override in overrides.values():
            from vast_scenarios import flatten_overrides
            value = flatten_overrides(override).get(f'bot.{side}.risk.n_positions', expected_count)
            if value != expected_count:
                raise ValueError('A symbol override changes the requested position capacity')
    return config


def qualified_winner(observation):
    """Require exact, complete, comparable evidence satisfying every hard target."""
    assessment = (observation or {}).get('assessment') or {}
    return bool(observation and observation.get('exact') and assessment.get('comparable')
                and assessment.get('simulation_complete') and assessment.get('hard_targets_met'))


def evaluate(metrics, rubric):
    """Apply the pinned measurable goal rules without inventing missing observations."""
    achieved, evidence = True, []
    for rule in rubric:
        values = [metrics[key] for key in rule['metrics'] if isinstance(metrics.get(key), (int, float))
                  and not isinstance(metrics[key], bool) and math.isfinite(metrics[key])]
        target = rule.get('target')
        value = values[0] if values else None
        measurable = value is not None and target is not None
        passed = measurable and (value >= target if rule['direction'] == 'max' else value <= target)
        achieved = achieved and (passed if target is not None else rule['goal'] != 'custom' and value is not None)
        evidence.append({'goal': rule['goal'], 'metric': next((k for k in rule['metrics'] if k in metrics),
                                                            next(iter(rule['metrics']), None)),
                         'value': value, 'target': target, 'confirmed': bool(passed), 'requirement': target is not None,
                         'direction': rule['direction'], 'reducer': 'worst' if rule['direction'] == 'min' else 'mean'})
    return {'score': None, 'achieved': bool(rubric and achieved and any(rule.get('target') is not None for rule in rubric)),
            'hard_targets_met': all(item['confirmed'] for item in evidence if item['requirement']),
            'comparable': bool(any(item['value'] is not None for item in evidence) and all(item['value'] is not None for item, rule in zip(evidence, rubric) if rule['metrics'])), 'goals': evidence}


class LoopStore:
    """Serialize owner-scoped loop state and append idempotent reusable evidence."""
    def __init__(self, root):
        self.root = Path(root)
        self._metadata_cache = OrderedDict()
        self._metadata_cache_lock = threading.RLock()
        self._read_cache = OrderedDict()
        self._read_cache_size = 0
        self._read_cache_lock = threading.RLock()

    def clear_read_cache(self):
        """Release retained snapshots when the API owner shuts down."""
        with self._read_cache_lock:
            self._read_cache.clear()
            self._read_cache_size = 0

    def _read_stamp(self, path):
        """Invalidate snapshots on manifest/archive replacement, edits or corruption."""
        archive_dir = path.parent / 'run_archives'
        archive = archive_dir / (path.stem + '.sqlite3')
        if path.is_symlink() or archive_dir.is_symlink() or archive.is_symlink():
            raise LoopStorageError('Loop state/archive must not be a symlink')
        stamps = []
        for item in (path, archive):
            try:
                stat = item.stat()
                stamps.append((stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns))
            except FileNotFoundError:
                stamps.append(None)
        return tuple(stamps)

    def _remember_read(self, path, stamp, record):
        """Keep immutable JSON bytes; decode independent caller values outside the lock."""
        snapshot = json.dumps(record, ensure_ascii=False, allow_nan=False, separators=(',', ':')).encode()
        size = len(snapshot)
        if size > READ_CACHE_BYTES // 2:
            return
        with self._read_cache_lock:
            old = self._read_cache.pop(path, None)
            if old:
                self._read_cache_size -= old[2]
            while self._read_cache and (len(self._read_cache) >= 16 or self._read_cache_size + size > READ_CACHE_BYTES):
                _, removed = self._read_cache.popitem(last=False)
                self._read_cache_size -= removed[2]
            self._read_cache[path] = (stamp, snapshot, size, time.monotonic() + READ_CACHE_TTL)
            self._read_cache_size += size

    def owner_dir(self, owner):
        path = self.root / identifier(owner)
        if path.is_symlink() or self.root.is_symlink():
            raise ValueError('Loop storage must not contain symlinks')
        ensure_private_directory(self.root)
        return ensure_private_directory(path)

    def _instructions_path(self, owner):
        """Keep instruction versions outside the run/config namespaces."""
        directory = self.owner_dir(owner) / 'instructions'
        if directory.is_symlink():
            raise ValueError('Instruction storage must not contain symlinks')
        ensure_private_directory(directory)
        return directory / 'versions.json'

    def instructions(self, owner):
        """Read the owner's immutable versions plus the current built-in default."""
        from pb8_loop_ai import INSTRUCTIONS
        path = self._instructions_path(owner)
        catalog = {'owner': owner, 'revision': 0, 'active': 'default', 'versions': []}
        if path.exists() or path.is_symlink():
            if path.is_symlink() or not path.is_file() or path.stat().st_size > MAX_JSON_BYTES:
                raise ValueError('Invalid instruction storage')
            catalog = json.loads(read_regular_file_nofollow(path, self.root))
            if catalog.get('owner') != owner or not isinstance(catalog.get('versions'), list):
                raise ValueError('Instruction ownership mismatch')
            for version in catalog['versions']:
                identifier(version['id'])
                self._validate_instructions(version['name'], version['text'])
                if version.get('digest') != digest(version['text']):
                    raise ValueError('Instruction version integrity mismatch')
        default = {'id': 'default', 'name': 'PBGui default', 'text': INSTRUCTIONS,
                   'digest': digest(INSTRUCTIONS), 'created_at': None}
        result = {**catalog, 'versions': [default, *catalog['versions']]}
        if result['active'] not in {item['id'] for item in result['versions']}:
            raise ValueError('Active instruction version is unavailable')
        return result

    @staticmethod
    def _validate_instructions(name, text):
        """Bound stored user instructions without treating their contents as code."""
        if not isinstance(name, str) or not 1 <= len(name.strip()) <= 80 or any(ord(c) < 32 for c in name):
            raise ValueError('Use an instruction version name of 1–80 characters')
        if not isinstance(text, str) or not text.strip() or len(text) > 64000 or '\x00' in text:
            raise ValueError('Instructions must contain 1–64000 characters without null bytes')

    def instruction_version(self, owner, version_id=None):
        """Resolve an owned saved version or the active version for a new run."""
        if version_id is not None and version_id != 'default':
            identifier(version_id)
        catalog = self.instructions(owner)
        selected = version_id or catalog['active']
        for version in catalog['versions']:
            if version['id'] == selected:
                return copy.deepcopy(version)
        raise FileNotFoundError('Instruction version not found')

    def save_instructions(self, owner, name, text, revision):
        """Atomically append and activate a new version; never overwrite older text."""
        self._validate_instructions(name, text)
        path = self._instructions_path(owner)
        with advisory_file_lock(path):
            catalog = self.instructions(owner)
            if type(revision) is not int or revision != catalog['revision']:
                raise LoopInstructionConflict('Instruction versions changed. Review the current active version and save again.')
            if any(item['name'].strip().casefold() == name.strip().casefold() for item in catalog['versions']):
                raise ValueError('Choose a new, unique instruction version name')
            versions = [item for item in catalog['versions'] if item['id'] != 'default']
            if len(versions) >= 128:
                raise ValueError('The limit of 128 saved instruction versions has been reached')
            version = {'id': uuid.uuid4().hex, 'name': name.strip(), 'text': text,
                       'digest': digest(text), 'created_at': time.time()}
            saved = {'owner': owner, 'revision': revision + 1, 'active': version['id'],
                     'versions': [*versions, version]}
            atomic_write_private_text(path, json.dumps(saved, indent=4, allow_nan=False) + '\n')
        return version

    def activate_instructions(self, owner, version_id, revision):
        """Select an existing immutable version using optimistic concurrency."""
        path = self._instructions_path(owner)
        with advisory_file_lock(path):
            catalog = self.instructions(owner)
            if type(revision) is not int or revision != catalog['revision']:
                raise LoopInstructionConflict('Instruction selection changed. Review it and try again.')
            version = self.instruction_version(owner, version_id)
            catalog.update(active=version['id'], revision=revision + 1)
            catalog['versions'] = [item for item in catalog['versions'] if item['id'] != 'default']
            atomic_write_private_text(path, json.dumps(catalog, indent=4, allow_nan=False) + '\n')
        return version

    def delete_instructions(self, owner, version_id, revision):
        """Delete an owned version atomically; existing run snapshots remain intact."""
        if version_id == 'default':
            raise ValueError('The PBGui default instructions cannot be deleted')
        identifier(version_id)
        path = self._instructions_path(owner)
        with advisory_file_lock(path):
            catalog = self.instructions(owner)
            if type(revision) is not int or revision != catalog['revision']:
                raise LoopInstructionConflict('Instruction versions changed. Review them and try again.')
            self.instruction_version(owner, version_id)
            catalog['versions'] = [item for item in catalog['versions'] if item['id'] not in {'default', version_id}]
            if catalog['active'] == version_id:
                catalog['active'] = 'default'
            catalog['revision'] = revision + 1
            atomic_write_private_text(path, json.dumps(catalog, indent=4, allow_nan=False) + '\n')
        return {'active': catalog['active'], 'revision': catalog['revision']}

    def definition_path(self, owner, name):
        """Resolve a named, owner-private configuration below its own namespace."""
        if not isinstance(name, str) or not name.strip() or len(name) > 120 or name in {'.', '..'} or any(c in name for c in '/\\') or any(ord(c) < 32 for c in name):
            raise ValueError('Invalid loop configuration name')
        directory = self.owner_dir(owner) / 'configs'
        if directory.is_symlink():
            raise ValueError('Loop configuration storage must not contain symlinks')
        ensure_private_directory(directory)
        return directory / (name + '.json')

    def definition(self, owner, name):
        """Read an owned saved definition without following filesystem links."""
        path = self.definition_path(owner, name)
        if not path.is_file() or path.is_symlink():
            raise FileNotFoundError('Loop configuration not found')
        if path.stat().st_size > MAX_JSON_BYTES:
            raise ValueError('Loop configuration is too large')
        row = json.loads(read_regular_file_nofollow(path, self.root))
        if row.get('owner') != owner or row.get('name') != name:
            raise ValueError('Loop configuration ownership mismatch')
        return row

    def save_definition(self, owner, name, settings, bundle, source, revision=None):
        """Atomically save a named snapshot and reject stale editor revisions."""
        validate_cloud_loop_config(apply_run_limit(settings, bundle['config']), settings['execution'])
        from ai_loop_search_limits import exposure_ceiling, validate_exposure_ceiling
        validate_exposure_ceiling(bundle['config'], exposure_ceiling({**settings, 'source': source}))
        path = self.definition_path(owner, name)
        with advisory_file_lock(path):
            previous = self.definition(owner, name) if path.exists() else None
            if previous is None and revision is not None:
                raise ValueError('This configuration was deleted. Create a new configuration before saving.')
            if previous and revision != previous['revision']:
                raise ValueError('This configuration changed. Open Edit again before saving.')
            row = {'name': name, 'owner': owner, 'settings': bounded_json(settings),
                   'bundle': bounded_json(bundle), 'source': bounded_json(source),
                   'revision': (previous['revision'] if previous else 0) + 1,
                   'created_at': previous['created_at'] if previous else time.time(), 'updated_at': time.time()}
            atomic_write_private_text(path, json.dumps(row, indent=4, allow_nan=False) + '\n')
        return row

    def definitions(self, owner):
        """List this owner's saved configurations with corruption diagnostics."""
        from logging_helpers import human_log
        directory = self.definition_path(owner, '_').parent
        rows = []
        for path in directory.glob('*.json'):
            try:
                rows.append(self.definition(owner, path.stem))
            except (OSError, ValueError) as exc:
                human_log(SERVICE, f'Cannot read loop configuration: {type(exc).__name__}', level='WARNING')
        return sorted(rows, key=lambda row: row['name'].casefold())

    def delete_definition(self, owner, name):
        """Delete a saved definition; preserve all run snapshots and evidence."""
        path = self.definition_path(owner, name)
        with advisory_file_lock(path):
            self.definition(owner, name)
            path.unlink()

    def path(self, owner, loop_id):
        return self.owner_dir(owner) / (identifier(loop_id) + '.json')

    def read(self, owner, loop_id, *, cache_snapshot=True, fields=None, settings_fields=None, prefer_cached_fields=False):
        """Read verified state; display projections can skip the extra raw snapshot cache."""
        result = self._read_snapshot(owner, loop_id, cache_snapshot=cache_snapshot, fields=fields,
                                     settings_fields=settings_fields, prefer_cached_fields=prefer_cached_fields)
        # Report scheduling is mandatory, including restored Template runs.
        result['observer_enabled'] = True
        if result.get('final_holdout_policy') in {'final_only', 'disabled'}:
            result['final_holdout_policy'] = 'observer_only'
            result['final_validation_status'] = 'observer_only' if result.get('observer_holdouts') else 'not_requested'
            result['holdout_retryable'] = False
        return result

    def native_job(self, owner, loop_id, operation):
        """Read only verified native navigation metadata, independently of the raw cache."""
        identifier(operation)
        path = self.path(owner, loop_id)
        if not path.is_file() or path.is_symlink():
            raise FileNotFoundError('Loop not found')
        if path.stat().st_size > MAX_JSON_BYTES:
            raise ValueError('Loop state is too large')
        self._read_stamp(path)
        header = json.loads(read_regular_file_nofollow(path, self.root))
        if header.get('owner') != owner or header.get('id') != loop_id:
            raise ValueError('Loop ownership mismatch')
        if '_loop_storage' in header:
            if header['_loop_storage'].get('version') != 1:
                raise LoopStorageError('Unsupported Loop archive format')
            identity, job = LoopArchive(path.parent, loop_id).native_job(header['_loop_storage']['root'], operation)
            if identity != {'owner': owner, 'id': loop_id}:
                raise LoopStorageError('Loop archive ownership mismatch')
            return identity, job
        job = next((item for field in ('jobs', 'observer_jobs') for item in header.get(field, [])
                    if item.get('operation') == operation), None)
        return {'owner': owner, 'id': loop_id}, ({key: job[key] for key in ('operation', 'name', 'kind', 'backend')
                                                if key in job} if job is not None else None)

    def _read_snapshot(self, owner, loop_id, *, cache_snapshot=True, fields=None, settings_fields=None, prefer_cached_fields=False):
        """Cache only verified reads; external file changes always force verification again."""
        if settings_fields is not None and fields is None:
            raise ValueError('Selective settings require an explicit display field selection')
        use_cache = cache_snapshot and (fields is None or prefer_cached_fields)
        if fields is not None:
            fields = frozenset(fields) | {'id', 'owner'}
            cache_snapshot = False
        path = self.path(owner, loop_id)
        if not path.is_file() or path.is_symlink():
            raise FileNotFoundError('Loop not found')
        if path.stat().st_size > MAX_JSON_BYTES:
            raise ValueError('Loop state is too large')
        stamp = self._read_stamp(path)
        cached_manifest = None
        if use_cache and fields is not None:
            cached_manifest = read_regular_file_nofollow(path, self.root)
            # Bind the manifest revision and cached values to the same archive/file identity.
            use_cache = self._read_stamp(path) == stamp
        if use_cache:
            snapshot = None
            # Display requests never wait behind a controller's cache mutation.
            acquired = self._read_cache_lock.acquire(blocking=fields is None)
            if acquired:
                try:
                    for key, cached in list(self._read_cache.items()):
                        if cached[3] <= time.monotonic() or key == path and cached[0] != stamp:
                            self._read_cache.pop(key)
                            self._read_cache_size -= cached[2]
                    cached = self._read_cache.get(path)
                    if cached:
                        self._read_cache.move_to_end(path)
                        self._read_cache[path] = (*cached[:3], time.monotonic() + READ_CACHE_TTL)
                        snapshot = cached[1]
                finally:
                    self._read_cache_lock.release()
            if snapshot is not None:
                result = json.loads(snapshot)
                if fields is None:
                    return result
                if settings_fields is not None and 'settings' in result:
                    result['settings'] = {key: value for key, value in result['settings'].items() if key in settings_fields}
                return {**{key: value for key, value in result.items() if key in fields},
                        '_view_revision': hashlib.sha256(cached_manifest).hexdigest()}
        raw = read_regular_file_nofollow(path, self.root)
        result = json.loads(raw)
        if result.get('owner') != owner or result.get('id') != loop_id:
            raise ValueError('Loop ownership mismatch')
        view_revision = hashlib.sha256(raw).hexdigest() if fields is not None else None
        if '_loop_storage' in result:
            header = result
            if header['_loop_storage'].get('version') != 1:
                raise LoopStorageError('Unsupported Loop archive format')
            archive = LoopArchive(self.owner_dir(owner), loop_id)
            result = (archive.load(header['_loop_storage']['root']) if fields is None
                      else archive.fields(header['_loop_storage']['root'], fields, settings_fields=settings_fields))
            if result.get('owner') != owner or result.get('id') != loop_id:
                raise LoopStorageError('Loop archive ownership mismatch')
            result.update({key: header[key] for key in HOT_FIELDS if key in header})
            if header.get('_storage_failure'):
                result.update(reason=header['_storage_failure']['message'], last_error=header['_storage_failure']['message'],
                              ai_pause={'reason': 'storage', 'message': header['_storage_failure']['message'],
                                        'details': {}, 'at': header['_storage_failure']['at'], 'phase': result['phase']})
        if cache_snapshot and self._read_stamp(path) == stamp:
            self._remember_read(path, stamp, result)
        if settings_fields is not None and 'settings' in result:
            result['settings'] = {key: value for key, value in result['settings'].items() if key in settings_fields}
        return result if fields is None else {
            **{key: value for key, value in result.items() if key in fields}, '_view_revision': view_revision}

    def write(self, record):
        """Archive immutable chunks, then atomically publish one bounded active root."""
        path = self.path(record['owner'], record['id'])
        with advisory_file_lock(path):
            try:
                legacy = None
                if path.exists():
                    previous = read_regular_file_nofollow(path, self.root)
                    if '_loop_storage' not in json.loads(previous):
                        legacy = previous
                root = LoopArchive(path.parent, record['id']).save(record, legacy)
                content = json.dumps(manifest(record, root), indent=4, allow_nan=False) + '\n'
                if len(content.encode()) > MAX_MANIFEST_BYTES:
                    raise LoopStorageError('Active Loop manifest exceeds its fixed size bound')
                atomic_write_private_text(path, content)
                self._remember_read(path, self._read_stamp(path), record)
            except (OSError, ValueError, TypeError) as exc:
                if isinstance(exc, LoopStorageError):
                    raise
                raise LoopStorageError(f'Loop persistence failed: {type(exc).__name__}') from exc

    def archive_page(self, owner, loop_id, field, offset=0, limit=50):
        """Load a bounded archive page without decoding the complete historical run."""
        if field not in {'history', 'jobs', 'observer_jobs', 'training_candidates', 'ai_decisions'}:
            raise ValueError('Unsupported Loop archive collection')
        path = self.path(owner, loop_id)
        header = json.loads(read_regular_file_nofollow(path, self.root))
        if header.get('owner') != owner or header.get('id') != loop_id:
            raise ValueError('Loop ownership mismatch')
        if '_loop_storage' not in header:
            raise ValueError('Legacy Loop is migrated on its next normal write')
        return LoopArchive(path.parent, loop_id).page(header['_loop_storage']['root'], field, offset, limit)

    def pause_storage(self, owner, loop_id, error):
        """Pause through the tiny manifest while preserving a completed AI decision root."""
        path = self.path(owner, loop_id)
        with advisory_file_lock(path):
            header = json.loads(read_regular_file_nofollow(path, self.root))
            if header.get('owner') != owner or header.get('id') != loop_id:
                raise ValueError('Loop ownership mismatch')
            if '_loop_storage' not in header:
                raise LoopStorageError('Legacy Loop storage needs recovery before migration')
            if header['status'] not in ACTIVE | {'replacing_gpu'}:
                return
            header.update(status='paused', control_generation=header['control_generation'] + 1,
                          updated_at=time.time(), _storage_failure={'message': str(error)[:1000], 'at': time.time()})
            atomic_write_private_text(path, json.dumps(header, indent=4, allow_nan=False) + '\n')

    def list_metadata(self):
        """Read manifests and reuse bounded legacy summaries until their files change."""
        result = []
        if not self.root.exists():
            return result
        for directory in self.root.iterdir():
            if not directory.is_dir() or directory.is_symlink() or not re.fullmatch(r'[a-f0-9]{32}', directory.name):
                continue
            for path in directory.glob('*.json'):
                if not re.fullmatch(r'[a-f0-9]{32}', path.stem):
                    continue
                try:
                    if path.is_symlink():
                        raise ValueError('Loop state must not be a symlink')
                    stat = path.stat()
                    stamp = (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)
                    with self._metadata_cache_lock:
                        cached = self._metadata_cache.get(path)
                        if cached and cached[0] == stamp:
                            self._metadata_cache.move_to_end(path)
                            result.append(dict(cached[1]))
                            continue
                    if stat.st_size > MAX_JSON_BYTES:
                        raise ValueError('Loop state is too large')
                    header = json.loads(read_regular_file_nofollow(path, self.root))
                    if header.get('owner') != directory.name or header.get('id') != path.stem:
                        raise ValueError('Loop ownership mismatch')
                    if '_loop_storage' in header:
                        result.append({**header, '_metadata_only': True})
                    else:
                        summary = manifest(self.read(directory.name, path.stem), None)
                        summary.pop('_loop_storage')
                        summary['_metadata_only'] = True
                        result.append(summary)
                        # Retain only small navigation/status fields, never histories.
                        # File identity and nanosecond change times invalidate edits.
                        with self._metadata_cache_lock:
                            self._metadata_cache[path] = (stamp, dict(summary))
                            self._metadata_cache.move_to_end(path)
                            while len(self._metadata_cache) > 128:
                                self._metadata_cache.popitem(last=False)
                except (OSError, ValueError) as exc:
                    from logging_helpers import human_log
                    human_log(SERVICE, f'Cannot read Loop metadata {path.name}: {type(exc).__name__}', level='WARNING')
        return sorted(result, key=lambda row: row['created_at'], reverse=True)

    def update(self, owner, loop_id, function):
        """Read, mutate and replace a record in one transaction."""
        with advisory_file_lock(self.path(owner, loop_id)):
            record = self.read(owner, loop_id)
            function(record)
            record['updated_at'] = time.time()
            self.write(record)
            return record

    def delete(self, owner, loop_id):
        """Remove owned run history atomically; retain definitions and reusable evidence."""
        path = self.path(owner, loop_id)
        with advisory_file_lock(path):
            record = self.read(owner, loop_id)
            if not run_deletable(record):
                raise LoopDeletionConflict('Stop this loop and wait for its jobs to finish collecting before deleting it.')
            path.unlink()

    def create(self, owner, settings, config, overrides, fingerprint, holdouts, comparison, queued=False, training_exclusions=None):
        """Create a durable run; chat-bound runs use reviewed default instructions only."""
        validate_cloud_loop_config(apply_run_limit(settings, config), settings['execution'])
        from ai_loop_search_limits import exposure_ceiling, validate_exposure_ceiling
        validate_exposure_ceiling(config, exposure_ceiling(settings))
        binding = (settings.get('source') or {}).get('loop_request')
        instructions = self.instruction_version(owner, 'default' if binding else None)
        if binding:
            from ai_loop_request import validate_request
            validate_request(settings, binding, instructions['digest'], config)
        loop_id = uuid.uuid4().hex
        record = {'id': loop_id, 'owner': identifier(owner), 'settings': bounded_json(settings),
                  'ai_instructions': instructions,
                  'initial_config': bounded_json(config), 'initial_overrides': bounded_json(overrides),
                  'fingerprint': fingerprint, 'holdouts': holdouts, 'comparison': comparison,
                  'training_exclusions': bounded_json(holdouts if training_exclusions is None else training_exclusions),
                  'created_at': time.time(), 'updated_at': time.time(), 'deadline': None if queued else time.time() + settings['hours'] * 3600,
                  'status': 'queued' if queued else 'running', 'phase': 'interpret', 'control_generation': 0, 'jobs': [], 'round': 0,
                  'rubric': [], 'best': None, 'history': [], 'stagnation': 0, 'ai_calls': 0,
                  'ai_tokens_reserved': 0, 'ai_usd_reserved': 0.0, 'jev_usd_reserved': 0.0,
                  'baseline_required': True, 'validation_count': 0, 'holdout_used': False, 'pending_ai': None, 'errors': 0}
        from ai_loop_windows import bound_plan
        plan = bound_plan(settings)
        record.update(observer_enabled=True, observer_jobs=[],
                      observer_holdouts=copy.deepcopy(record['training_exclusions']))
        if plan is not None:
            record.update(final_holdout_policy='observer_only',
                          final_validation_status='observer_only' if plan['use_holdout'] else 'not_requested')
            if plan['use_holdout']:
                from ai_loop_windows import HOLDOUT_EVIDENCE_POLICY
                record['holdout_evidence_policy'] = HOLDOUT_EVIDENCE_POLICY
        self.write(record)
        return record

    def list(self, owner=None):
        """Enumerate bounded metadata, ignoring corrupt records with logged diagnostics."""
        from logging_helpers import human_log
        dirs = [self.owner_dir(owner)] if owner else (list(self.root.iterdir()) if self.root.exists() else [])
        result = []
        for directory in dirs:
            if not directory.is_dir() or directory.is_symlink() or not re.fullmatch(r'[a-f0-9]{32}', directory.name):
                continue
            for path in directory.glob('*.json'):
                if path.stem == 'knowledge':
                    continue
                try:
                    result.append(self.read(directory.name, path.stem))
                except (ValueError, OSError) as exc:
                    human_log(SERVICE, f'Cannot read loop state {path.name}: {type(exc).__name__}', level='WARNING')
        return sorted(result, key=lambda row: row['created_at'], reverse=True)

    def knowledge(self, owner):
        path = self.owner_dir(owner) / 'knowledge.json'
        if not path.exists():
            return {'schema': 1, 'entries': [], 'used_holdouts': []}
        return json.loads(read_regular_file_nofollow(path, self.root))

    def reset_knowledge(self, owner):
        """Archive and clear learning, retaining Holdout accounting and original run evidence."""
        path = self.owner_dir(owner) / 'knowledge.json'
        with advisory_file_lock(path):
            if any(row['status'] not in TERMINAL for row in self.list(owner)):
                raise ValueError('Stop all queued, active and paused Loops before resetting knowledge')
            knowledge = self.knowledge(owner)
            backup = ensure_private_directory(path.parent / 'knowledge_resets') / (uuid.uuid4().hex + '.json')
            atomic_write_private_text(backup, json.dumps(knowledge, indent=4, allow_nan=False) + '\n')
            reset_at = time.time()
            count = len(knowledge['entries'])
            knowledge.update(entries=[], learning_reset_at=reset_at)
            atomic_write_private_text(path, json.dumps(knowledge, indent=4, allow_nan=False) + '\n')
        from logging_helpers import human_log
        human_log(SERVICE, f'User-requested learning reset archived {count} entries; Holdout accounting retained')
        return {'cleared_entries': count, 'reset_at': reset_at, 'backup': str(backup)}

    def remember(self, record, evidence_id, entry):
        """Append once; archive full evidence before trimming the model-context index."""
        path = self.owner_dir(record['owner']) / 'knowledge.json'
        with advisory_file_lock(path):
            knowledge = self.knowledge(record['owner'])
            archive = ensure_private_directory(path.parent / 'evidence') / (digest(evidence_id) + '.json')
            if any(item['id'] == evidence_id for item in knowledge['entries']):
                return
            if archive.exists() or archive.is_symlink():
                original = json.loads(read_regular_file_nofollow(archive, self.root))
                if original.get('id') != evidence_id or original.get('loop') != record['id']:
                    raise ValueError('Existing evidence archive identity mismatch')
                if original.get('created_at', 0) <= knowledge.get('learning_reset_at', 0):
                    return
                # The mutable 200-entry UI index can evict an old ID. That must
                # never authorize rewriting an already referenced original.
                knowledge['entries'] = (knowledge['entries'] + [original])[-200:]
                if original.get('holdout') and original['holdout'] not in knowledge['used_holdouts']:
                    knowledge['used_holdouts'].append(original['holdout'])
                atomic_write_private_text(path, json.dumps(knowledge, indent=4) + '\n')
                return
            # A round contains several full exact reports/configs. Validate each
            # observation with the existing strict per-object bound instead of
            # applying the single-config limit to the whole evidence archive.
            if isinstance(entry.get('observations'), list):
                validated = bounded_json({key: item for key, item in entry.items() if key != 'observations'})
                validated['observations'] = [bounded_json(item) for item in entry['observations']]
                if len(json.dumps(validated, allow_nan=False).encode()) > 8 * 1024 * 1024:
                    raise ValueError('Round evidence exceeds 8 MB')
            else:
                validated = bounded_json(entry)
            value = {**validated, 'id': evidence_id, 'loop': record['id'],
                     'fingerprint': record['fingerprint'], 'created_at': time.time()}
            atomic_write_private_text(archive, json.dumps(value, indent=4) + '\n')
            contradictions = value.get('contradicts') or []
            if not isinstance(contradictions, list) or not all(isinstance(key, str) for key in contradictions):
                raise ValueError('Knowledge contradictions must reference evidence IDs')
            for previous in knowledge['entries']:
                if previous['id'] in contradictions:
                    previous['superseded_by'] = evidence_id
            knowledge['entries'] = (knowledge['entries'] + [value])[-200:]
            if entry.get('holdout') and entry['holdout'] not in knowledge['used_holdouts']:
                knowledge['used_holdouts'].append(entry['holdout'])
            atomic_write_private_text(path, json.dumps(knowledge, indent=4) + '\n')

    def claim_holdouts(self, record):
        """Atomically reserve unused evaluation windows across concurrent owned loops."""
        path = self.owner_dir(record['owner']) / 'knowledge.json'
        with advisory_file_lock(path):
            knowledge = self.knowledge(record['owner'])
            reservations = knowledge.get('reserved_holdouts', [])
            if any(claim['loop_id'] == record['id'] for claim in reservations):
                return True
            previous = knowledge['used_holdouts'] + [window for claim in reservations for window in claim['windows']]
            for window in record['holdouts']:
                if any(item.get('start_date', '') <= window['end_date'] and window['start_date'] <= item.get('end_date', '') for item in previous):
                    return False
            knowledge['reserved_holdouts'] = reservations + [{'loop_id': record['id'], 'windows': copy.deepcopy(record['holdouts'])}]
            atomic_write_private_text(path, json.dumps(knowledge, indent=4) + '\n')
            return True

    def settle_holdouts(self, record, evaluated):
        """Consume evaluated windows; release only proven configuration-before-data failures."""
        path = self.owner_dir(record['owner']) / 'knowledge.json'
        with advisory_file_lock(path):
            knowledge = self.knowledge(record['owner'])
            claims = knowledge.get('reserved_holdouts', [])
            owned = [claim for claim in claims if claim['loop_id'] == record['id']]
            knowledge['reserved_holdouts'] = [claim for claim in claims if claim['loop_id'] != record['id']]
            if evaluated:
                for claim in owned:
                    for window in claim['windows']:
                        if window not in knowledge['used_holdouts']:
                            knowledge['used_holdouts'].append(window)
            atomic_write_private_text(path, json.dumps(knowledge, indent=4) + '\n')

    def context(self, record):
        """Provide bounded relevant findings and provenance, without repeated configs."""
        from ai_loop_windows import protected_holdout_plan
        protected = protected_holdout_plan(record) is not None
        entries = self.knowledge(record['owner'])['entries']
        relevant = [item for item in entries if not item.get('holdout') and not item.get('observer_only')
                    and (not protected or item.get('loop') == record['id'])
                    and item.get('kind') not in {'observer_holdout', 'observer_full_range'}
                    and item['fingerprint'].get('pb8') == record['fingerprint'].get('pb8')
                    and item.get('coins') == record['settings']['goals'].get('coins', [])][-12:]
        return [{key: item.get(key) for key in ('id', 'fingerprint', 'goals', 'hypothesis', 'knowledge', 'status', 'comparison', 'uncertainty', 'contradicts', 'superseded_by', 'created_at')}
                | {'observations': [{'metrics': observation.get('metrics'), 'assessment': observation.get('assessment'), 'operation': observation.get('operation')}
                                     for observation in item.get('observations', [])[:12]]} for item in relevant]


def authorize_native_job(root, data, loop_id):
    """Reject native starts outside the owning live loop controller."""
    from fastapi import HTTPException
    try:
        if identifier(loop_id) != identifier(data['loop_id']):
            raise ValueError('Loop ownership mismatch')
        current = LoopStore(Path(root) / 'data/loop_optimizer').read(data['loop_owner'], loop_id)
        observer = next((job for job in current.get('observer_jobs', [])
                         if job.get('observer_only') and job['operation'] == data.get('operation_id')
                         and (job.get('backend') or {}).get('id') == data.get('filename')), None) if data.get('loop_observer') else None
        if data.get('loop_observer') and observer is None:
            raise ValueError('Observer ownership mismatch')
        allowed = ACTIVE | {'completed', 'unconfirmed', 'failed', 'stopping', 'stopped'} if observer else ACTIVE
        if current['status'] not in allowed or (not observer and time.time() >= current['deadline'] - 60):
            raise ValueError('Loop is paused, stopped or expired')
    except (ValueError, KeyError, OSError, TypeError):
        raise HTTPException(status_code=409, detail='This job can only be started by its active PB8 loop') from None


def config_changes(before, after, limit=80):
    """Summarize actual edited fields while retaining full immutable job snapshots."""
    changes = []
    def walk(old, new, prefix=''):
        if len(changes) >= limit:
            return
        if isinstance(old, dict) and isinstance(new, dict):
            for key in sorted(set(old) | set(new)):
                walk(old.get(key), new.get(key), prefix + ('.' if prefix else '') + key)
        elif old != new:
            def compact(value):
                encoded = json.dumps(value, ensure_ascii=False)
                return value if len(encoded) <= 400 else {'summary': encoded[:400], 'sha256': digest(value)}
            changes.append({'path': prefix, 'before': compact(old), 'after': compact(new)})
    walk(before, after)
    return changes
