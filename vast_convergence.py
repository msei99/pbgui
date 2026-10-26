"""Persisted early stopping from verified exact Pareto snapshots, without bot changes."""

import json
import math

from logging_helpers import human_log as _log

SERVICE = "VastRunner"
from optimize_convergence import DEFAULTS, advance, hypervolume


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
