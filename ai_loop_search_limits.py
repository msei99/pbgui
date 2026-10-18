"""User search ceilings and CPU budgets, independent of AI experiment choices."""
from __future__ import annotations

import math

SERVICE = 'AILoopSearchLimits'


def validate_twe_ceiling(value):
    """Accept a finite positive exposure ceiling, never a prescribed search value."""
    if type(value) not in (int, float) or not math.isfinite(value) or not 0 < value <= 1000:
        raise ValueError('Maximum TWE must be a finite positive number no greater than 1000')
    return value


def exposure_ceiling(settings):
    """Retain the reviewed Template ceiling even if model-authored goals omit it."""
    from ai_loop_windows import bound_plan
    ceiling = (settings.get('goals') or {}).get('max_twe')
    plan = bound_plan(settings)
    reviewed = (plan or {}).get('max_twe')
    if reviewed is not None:
        if ceiling is not None and ceiling != reviewed:
            raise ValueError('Maximum TWE differs from the reviewed Template ceiling')
        ceiling = reviewed
    return validate_twe_ceiling(ceiling) if ceiling is not None else None


def validate_exposure_ceiling(config, ceiling):
    """Check base, effective bounds and overlays using native direction semantics."""
    if ceiling is None:
        return
    validate_twe_ceiling(ceiling)
    try:
        _validate_effective_exposure(config, ceiling)
    except (KeyError, TypeError, AttributeError) as exc:
        raise ValueError('Cannot verify maximum TWE: a complete valid PB8 direction configuration is required') from exc


def _validate_effective_exposure(config, ceiling):
    """Apply the same direction projection as the native GPU preflight."""
    from vast_direction_validation import materialize_direction, scenario_direction
    from vast_scenarios import scenario_plan, flatten_overrides
    effective, values, bounds = materialize_direction(config)
    # CPU baseline/validation ignores optimizer projection, so also check the
    # original bot values rather than relying on the GPU-clamped seed alone.
    views = [('base configuration', config, {}, {}), ('effective optimizer', effective, values, bounds)]
    if config.get('backtest', {}).get('suite_enabled'):
        contexts, _sources, errors = scenario_plan(config)
        if errors:
            raise ValueError('Cannot verify TWE ceiling: ' + errors[0]['path'] + ': ' + errors[0]['message'])
        for context in contexts:
            index = int(context['path'].rsplit('.', 1)[-1])
            definition = config['backtest']['scenarios'][index]
            view, seed, edges = scenario_direction(effective, values, bounds,
                flatten_overrides(definition.get('overrides') or {}), context['coins'])
            views.append((context['path'], view, seed, edges))
    for label, view, _seed, edges in views:
        for side in ('long', 'short'):
            risk = view['bot'][side]['risk']
            exposure = risk['total_wallet_exposure_limit']
            positions = risk['n_positions']
            edge = edges.get(side + '_total_wallet_exposure_limit')
            slots = edges.get(side + '_n_positions')
            high = max(exposure, edge.high if edge is not None else exposure)
            active = (view['live']['approved_coins'].get(side)
                      and max(positions, slots.high if slots is not None else positions) > 0 and high > 0)
            if active and high > ceiling and not math.isclose(high, ceiling, rel_tol=1e-12):
                raise ValueError(f'{label}: {side}.risk.total_wallet_exposure_limit effective maximum '
                                 f'{high:g} exceeds the requested TWE ceiling {ceiling:g}')


def validation_budget(record, available):
    """Let AI allocate the remaining total; obsolete per-cycle caps have no effect."""
    total, used = record['settings']['max_validations'], record.get('validation_count', 0)
    remaining = max(0, total - used)
    completed = len({item['round'] for item in record.get('history', []) if item.get('evaluated_at') is not None})
    rounds = max(0, record['settings']['max_rounds'] - completed)
    return {'total': total, 'used': used, 'remaining': remaining,
            'remaining_rounds_including_current': rounds,
            'available_candidates': available, 'max_candidates': min(remaining, available),
            'includes': 'baseline and independent CPU training validation jobs',
            'excludes': 'user-only Holdout and Full Time Range background reports',
            'allocation': 'AI chooses count and representative candidates; justify coverage, duplicates and budget for future rounds'}
