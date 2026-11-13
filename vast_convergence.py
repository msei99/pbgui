"""Persisted early stopping from verified exact Pareto snapshots, without bot changes."""

import json
import math

from file_lock import advisory_file_lock
from vast_provider import VastError

from logging_helpers import human_log as _log

SERVICE = "VastRunner"
from optimize_convergence import DEFAULTS, advance, hypervolume, validate_config


def observe(store, identifier, *, final=False):
    """Evaluate the newly verified snapshot and persist a recoverable stop request."""
    with advisory_file_lock(store.directory(identifier) / '.convergence-lock'):
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
    with advisory_file_lock(store.directory(identifier) / '.convergence-lock'):
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


def update_config(store, identifier, config, expected):
    """Change one live native job's stop policy without rebinding frozen optimizer inputs."""
    try:
        config, expected = validate_config(config), validate_config(expected)
    except ValueError as exc:
        raise VastError(str(exc), 422) from exc
    with advisory_file_lock(store.directory(identifier) / '.convergence-lock'):
        row = store.read(identifier)
        control = store.read(identifier, 'control.json')
        if (row.get('loop_id') or row.get('kind') in {'worker', 'calibration'}
                or row.get('status') not in {'running', 'uploading'}
                or row.get('final_collected') or row.get('stop_reason')
                or control.get('stop') or control.get('cleanup')
                or (row.get('convergence') or {}).get('stop_requested')):
            raise VastError('Stagnation settings can only change for an active standalone optimizer before stopping', 409)
        current = validate_config(row.get('convergence_config') or {})
        if config == current:
            return row  # A lost acknowledgement must not reset the observation window.
        if expected != current:
            raise VastError('Stagnation settings changed elsewhere; review the current settings before saving', 409)
        updated = store.update(identifier, convergence_config=config, convergence={})
        _log(SERVICE, f'{identifier}: stagnation settings updated; a new observation window starts with the next verified snapshot', level='INFO')
        return updated
