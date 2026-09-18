"""Offline exact-front convergence tests, never touching real jobs or providers."""

import math
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


def test_live_points_continue_stagnation_after_full_backup_limit(tmp_path, monkeypatch):
    """A lightweight worker front can stop a run after archive backups are capped."""
    from vast_jobs import JobStore, write_json
    from vast_convergence import observe_points
    monkeypatch.setattr('vast_convergence._log', lambda *args, **kwargs: None)
    store = JobStore(tmp_path)
    identifier = 'd' * 32
    directory = store.root / 'jobs' / identifier
    directory.mkdir(parents=True)
    config = {**DEFAULTS, 'convergence_enabled': True}
    history = advance({}, [[1, 1, 1]], 7074, config)
    write_json(directory / 'state.json', {'id':identifier, 'status':'running',
        'downloaded_bytes':1024**3, 'convergence_config':config, 'convergence':history})
    write_json(directory / 'control.json', {'stop':False, 'cleanup':False})

    assert observe_points(store, identifier, [[1, 1, 1]], 8648)
    state = store.read(identifier)['convergence']
    assert state['checked_exact'] == 8648
    assert state['stalled_exact'] == 1574
    assert state['stop_requested'] is True
    assert store.read(identifier, 'control.json')['stop'] is True


@pytest.mark.parametrize('seed', range(12))
def test_three_axis_volume_matches_independent_cell_union(seed):
    """Incremental skyline agrees with a direct union of disjoint grid cells."""
    import itertools
    import random
    rng = random.Random(seed)
    points = [[rng.randrange(-2, 6) / 2 for _ in range(3)] for _ in range(16)]
    reference = [2.0, 2.0, 2.0]
    valid = [p for p in points if all(x < r for x, r in zip(p, reference))]
    axes = [sorted({p[i] for p in valid} | {reference[i]}) for i in range(3)]
    expected = 0.0
    for cell in itertools.product(*(list(zip(a, a[1:])) for a in axes)):
        if any(all(p[i] <= cell[i][0] for i in range(3)) for p in valid):
            expected += math.prod(right - left for left, right in cell)
    assert hypervolume(points, reference) == pytest.approx(expected)
    assert hypervolume(list(reversed(points)) + points, reference) == pytest.approx(expected)


def test_large_front_tracks_and_stops_without_size_cutoff(tmp_path, monkeypatch):
    """A valid 1,501-file snapshot is tracked and stops after unchanged patience."""
    import json
    from vast_jobs import JobStore, write_json
    from vast_convergence import observe
    monkeypatch.setattr('vast_convergence._log', lambda *args, **kwargs: None)
    store = JobStore(tmp_path)
    identifier = 'c' * 32
    directory = store.root / 'jobs' / identifier
    front = directory / 'partial-results/optimize_results/run/pareto'
    front.mkdir(parents=True)
    count = 1501
    points = [[i / count, 1 - i / count, .2] for i in range(count)]
    expected_area = sum((1 / count) * (1 + i / count) for i in range(count - 1))
    expected_area += (2 - (count - 1) / count) * (1 + (count - 1) / count)
    assert hypervolume(points, [2, 2, 2]) == pytest.approx(expected_area * 1.8)
    for i, point in enumerate(points):
        (front / f'{i}.json').write_text(json.dumps({'metrics': {
            'constraint_violation': 0, 'unpenalized_objectives': point}}))
    write_json(directory / 'state.json', {'id': identifier, 'status': 'running',
        'exact_completed': 2000, 'convergence_config': {**DEFAULTS, 'convergence_enabled': True}})
    write_json(directory / 'control.json', {'stop': False, 'cleanup': False})
    assert not observe(store, identifier)
    assert store.read(identifier)['convergence']['phase'] == 'tracking'
    store.update(identifier, exact_completed=2512)
    assert observe(store, identifier)
    assert store.read(identifier, 'control.json')['stop'] is True


def test_default_tolerance_requires_quarter_percent_improvement():
    """Sub-threshold gains accumulate without resetting patience prematurely."""
    assert DEFAULTS['convergence_tolerance_pct'] == .25
    initial = advance({}, [[1.0]], 512, DEFAULTS)
    small = advance(initial, [[.99999]], 600, DEFAULTS)
    assert small['last_improvement_exact'] == 512
    significant = advance(small, [[.99997]], 700, DEFAULTS)
    assert significant['last_improvement_exact'] == 700
