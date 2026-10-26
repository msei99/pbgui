"""Backend-independent exact Pareto stagnation calculation."""

import math
from bisect import bisect_left

SERVICE = "OptimizeConvergence"
DEFAULTS = {'convergence_enabled': False, 'convergence_min_exact': 512,
            'convergence_patience': 512, 'convergence_tolerance_pct': 0.25}


def validate_config(raw):
    """Validate local controls using the cloud detector's bounds, without coercion."""
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise ValueError('Convergence settings must be an object')
    config = {key: raw.get(key, value) for key, value in DEFAULTS.items()}
    if not isinstance(config['convergence_enabled'], bool):
        raise ValueError('convergence_enabled must be a boolean')
    for key, lower in [('convergence_min_exact', 256), ('convergence_patience', 128)]:
        value = config[key]
        if type(value) is not int or not lower <= value <= 10_000_000:
            raise ValueError(f'{key} must be an integer between {lower} and 10000000')
    value = config['convergence_tolerance_pct']
    if type(value) not in (int, float) or not math.isfinite(value) or not 0 < value <= 10:
        raise ValueError('convergence_tolerance_pct must be finite, above 0 and at most 10')
    return config


def convergence_complete(state):
    """Require this invocation's durable request and verified native shutdown evidence."""
    if not isinstance(state, dict):
        return False
    request = state.get('convergence') or {}
    proof = state.get('shutdown') or {}
    if not isinstance(request, dict) or not isinstance(proof, dict):
        return False
    return bool(
        state.get('returncode') == 130 and not state.get('error')
        and state.get('completion_reason') == 'convergence'
        and request.get('stop_requested') and state.get('run_id')
        and request.get('request_run_id') == state['run_id']
        and proof.get('complete') and not proof.get('errors') and not proof.get('forced')
    )


def runner_complete(state):
    """Classify ordinary completion and proven early completion consistently."""
    return bool(state and not state.get('error') and not (state.get('shutdown') or {}).get('forced')
                and ((state.get('returncode') == 0 and not (state.get('convergence') or {}).get('stop_requested'))
                     or convergence_complete(state)))


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
