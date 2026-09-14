"""Persisted early stopping from verified exact Pareto snapshots, without bot changes."""

import json
import math

from logging_helpers import human_log as _log

SERVICE = "VastRunner"
DEFAULTS = {'convergence_enabled': False, 'convergence_min_exact': 512,
            'convergence_patience': 512, 'convergence_tolerance_pct': 0.1}


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
    coordinates = sorted({p[0] for p in points}) + [reference[0]]
    return sum((right - left) * hypervolume([p[1:] for p in points if p[0] <= left], reference[1:])
               for left, right in zip(coordinates, coordinates[1:]))


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
    if not 1 <= dimensions <= 3 or len(points) > 1000 or any(
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
        if len(files) > 1000:
            raise ValueError('Pareto snapshot exceeds convergence limit')
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
    store.update(identifier, convergence=state)
    if state.get('stop_requested'):
        _log(SERVICE, f'{identifier}: no significant exact Pareto improvement; collecting final results', level='INFO')
        store.control(identifier, 'stop')
        return True
    return False
