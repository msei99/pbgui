"""Persisted early stopping from verified exact Pareto snapshots, without bot changes."""

import json
import math
from bisect import bisect_left

from logging_helpers import human_log as _log

SERVICE = "VastRunner"
DEFAULTS = {'convergence_enabled': False, 'convergence_min_exact': 512,
            'convergence_patience': 512, 'convergence_tolerance_pct': 0.25}


def hypervolume(points, reference):
    """Return the exact union volume of minimization boxes in up to three axes."""
    points = [p for p in points if all(x < r for x, r in zip(p, reference))]
    if not points:
        return 0.0
    if len(reference) == 1:
        return reference[0] - min(p[0] for p in points)
    if len(reference) == 2:
        area, best = 0.0, reference[1]
        for x, y in sorted(points):
            if y < best:
                area += (reference[0] - x) * (best - y)
                best = y
        return area
    # Sweep x once, maintaining the nondominated y/z skyline and its area.
    # Re-sorting every x-prefix makes large valid fronts unnecessarily costly.
    skyline = []
    area, volume = 0.0, 0.0
    last_x = min(p[0] for p in points)
    for x, y, z in sorted(points):
        volume += (x - last_x) * area
        last_x = x
        index = bisect_left(skyline, (y, -math.inf))
        if index and skyline[index - 1][1] <= z:
            continue
        if index < len(skyline) and skyline[index][0] == y and skyline[index][1] <= z:
            continue
        end = index
        cursor = y
        old_height = reference[2] - skyline[index - 1][1] if index else 0.0
        old_area = 0.0
        while end < len(skyline) and skyline[end][1] >= z:
            next_y, next_z = skyline[end]
            old_area += (next_y - cursor) * old_height
            cursor, old_height = next_y, reference[2] - next_z
            end += 1
        right = skyline[end][0] if end < len(skyline) else reference[1]
        old_area += (right - cursor) * old_height
        area += (right - y) * (reference[2] - z) - old_area
        skyline[index:end] = [(y, z)]
    return volume + (reference[0] - last_x) * area


def advance(previous, points, exact, config):
    """Advance only on fresh exact evaluations using a frozen normalization."""
    state = dict(previous or {})
    if state.get('stop_requested') or exact <= state.get('checked_exact', -1):
        return state
    state['checked_exact'] = exact
    minimum = config['convergence_min_exact']
    if exact < minimum:
        return {**state, 'phase': 'warming', 'remaining': minimum - exact}
    if not points:
        return {'checked_exact': exact, 'phase': 'waiting', 'reason': 'Waiting for feasible exact results'}
    dimensions = len(points[0])
    if not 1 <= dimensions <= 3 or any(
            len(p) != dimensions or any(not math.isfinite(x) for x in p) for p in points):
        return {'checked_exact': exact, 'phase': 'waiting', 'reason': 'Unsupported or invalid exact objectives'}
    if state.get('dimensions', dimensions) != dimensions:
        state = {'checked_exact': exact}
    if 'scale' not in state:
        lower = [min(p[i] for p in points) for i in range(dimensions)]
        upper = [max(p[i] for p in points) for i in range(dimensions)]
        scale = [max(hi - lo, abs(lo) * 0.1, 1e-9) for lo, hi in zip(lower, upper)]
        state.update(dimensions=dimensions, origin=lower, scale=scale,
                     reference=[(hi - lo) / s + 0.1 for lo, hi, s in zip(lower, upper, scale)])
    normalized = [[(x - lo) / scale for x, lo, scale in zip(p, state['origin'], state['scale'])] for p in points]
    volume = hypervolume(normalized, state['reference'])
    if not math.isfinite(volume):
        return {'checked_exact': exact, 'phase': 'waiting', 'reason': 'Invalid quality measure'}
    baseline = state.get('baseline_hv')
    if baseline is None or volume > baseline * (1 + config['convergence_tolerance_pct'] / 100):
        state.update(baseline_hv=volume, last_improvement_exact=exact)
    stalled = exact - state['last_improvement_exact']
    state.update(phase='tracking', hypervolume=volume, stalled_exact=stalled,
                 remaining=max(0, config['convergence_patience'] - stalled))
    if stalled >= config['convergence_patience']:
        state.update(phase='stopping', stop_requested=True)
    return state


def observe(store, identifier, *, final=False):
    """Evaluate the newly verified snapshot and persist a recoverable stop request."""
    row = store.read(identifier)
    config = row.get('convergence_config', DEFAULTS)
    if not config.get('convergence_enabled'):
        return False
    points = []
    try:
        source = 'final-results' if final else 'partial-results'
        files = list((store.directory(identifier) / source / 'optimize_results').glob('*/pareto/*.json'))
        for path in files:
            if path.is_symlink() or path.stat().st_size > 4 * 1024**2:
                raise ValueError('Invalid Pareto snapshot')
            metrics = json.loads(path.read_text()).get('metrics', {})
            violation = float(metrics['constraint_violation'])
            if not math.isfinite(violation):
                raise ValueError('Invalid constraint value')
            if violation > 0 or metrics.get('liquidated'):
                continue
            # PB8 stores these as signed minimization objectives, including suite reducers.
            points.append([float(x) for x in metrics['unpenalized_objectives']])
        previous = dict(row.get('convergence') or {})
        previous.pop('final', None)
        exact = int(row.get('evaluations', 0) if final else row.get('exact_completed', 0))
        if final:
            # Inspect the final front even after a stop was already requested.
            # Keep that historical decision separate from this retrospective check.
            previous.pop('stop_requested', None)
            previous.pop('checked_exact', None)
        state = advance(previous, points, exact, config)
    except (OSError, ValueError, TypeError, KeyError) as exc:
        _log(SERVICE, 'Convergence check unavailable; automatic stop suspended', level='WARNING')
        state = {'phase': 'waiting', 'reason': 'Waiting for a valid exact result snapshot'}
    if final:
        state['threshold_reached'] = bool(state.pop('stop_requested', False))
        if state.get('phase') == 'stopping':
            state['phase'] = 'tracking'
        store.update(identifier, convergence={**(row.get('convergence') or {}), 'final': state})
        return False
    return persist_observation(store, identifier, state)


def observe_points(store, identifier, points, exact):
    """Advance from a bounded live worker snapshot without a full result archive."""
    row = store.read(identifier)
    config = row.get('convergence_config', DEFAULTS)
    if not config.get('convergence_enabled'):
        return False
    state = advance(dict(row.get('convergence') or {}), points, int(exact), config)
    return persist_observation(store, identifier, state)


def persist_observation(store, identifier, state):
    """Publish one checked state and request the existing graceful stop path."""
    store.update(identifier, convergence=state)
    if state.get('stop_requested'):
        _log(SERVICE, f'{identifier}: no significant exact Pareto improvement; collecting final results', level='INFO')
        store.control(identifier, 'stop')
        return True
    return False
