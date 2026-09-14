"""Offline exact-front convergence tests, never touching real jobs or providers."""

import pytest

from vast_convergence import DEFAULTS, advance, hypervolume


@pytest.mark.parametrize('points,reference,expected', [
    ([[1]], [3], 2), ([[1, 2], [2, 1]], [3, 3], 3),
    ([[1, 2, 1], [2, 1, 1]], [3, 3, 2], 3),
    ([[1, 1], [2, 2], [1, 1]], [3, 3], 4), ([], [1, 1], 0),
])
def test_exact_hypervolume(points, reference, expected):
    """Union volume ignores duplicate/dominated points, with no double counting."""
    assert hypervolume(points, reference) == pytest.approx(expected)


def test_patience_uses_evaluations_and_survives_restart():
    """Repeated polls cannot consume patience; serialized state can resume."""
    import json
    cfg = {**DEFAULTS, 'convergence_enabled': True}
    assert advance({}, [[1, 1]], 100, cfg)['phase'] == 'warming'
    initial = advance({}, [[1, 1]], 512, cfg)
    same = advance(initial, [[1, 1]], 512, cfg)
    assert same == initial
    resumed = json.loads(json.dumps(initial))
    assert not advance(resumed, [[1, 1]], 1023, cfg).get('stop_requested')
    assert advance(resumed, [[1, 1]], 1024, cfg)['stop_requested']


def test_improved_front_resets_patience_and_scale_stays_fixed():
    """Quality, not cardinality or a moving reference point, drives improvement."""
    cfg = dict(DEFAULTS)
    initial = advance({}, [[1, 2], [2, 1]], 512, cfg)
    improved = advance(initial, [[0.9, 2], [2, 0.9]], 900, cfg)
    assert improved['last_improvement_exact'] == 900
    assert improved['scale'] == initial['scale']
    assert improved['reference'] == initial['reference']
    assert not advance(improved, [[0.9, 2], [2, 0.9]], 1024, cfg).get('stop_requested')


@pytest.mark.parametrize('points', [[], [[float('nan')]], [[1, 2], [1]], [[1, 2, 3, 4]]])
def test_missing_invalid_or_unsupported_front_never_stops(points):
    """Loss of trustworthy quality evidence resets the observation window."""
    old = advance({}, [[1]], 512, DEFAULTS)
    state = advance(old, points, 2000, DEFAULTS)
    assert state['phase'] == 'waiting'
    assert not state.get('stop_requested')


def test_observer_persists_stop_and_ignores_infeasible_points(tmp_path, monkeypatch):
    """A verified feasible front drives the owned job stop and survives recreation."""
    import json
    from vast_jobs import JobStore, write_json
    from vast_convergence import observe
    monkeypatch.setattr('vast_convergence._log', lambda *args, **kwargs: None)
    store = JobStore(tmp_path)
    identifier = 'a' * 32
    directory = store.root / 'jobs' / identifier
    front = directory / 'partial-results/optimize_results/run/pareto'
    front.mkdir(parents=True)
    write_json(directory / 'state.json', {'id': identifier, 'status': 'running', 'lease_id': 'b'*32,
        'exact_completed': 512, 'convergence_config': {**DEFAULTS, 'convergence_enabled': True}})
    write_json(directory / 'control.json', {'stop': False, 'cleanup': False})
    write_json(front / 'point.json', {'metrics': {'unpenalized_objectives': [1, 1], 'constraint_violation': 0}})
    write_json(front / 'invalid.json', {'metrics': {'unpenalized_objectives': [-100, -100], 'constraint_violation': 1}})
    assert not observe(store, identifier)
    store.update(identifier, exact_completed=1024)
    assert observe(JobStore(tmp_path), identifier)
    assert store.read(identifier, 'control.json')['stop']
    assert not store.read(identifier, 'control.json')['cleanup']
    assert store.read(identifier)['convergence']['stop_requested']
