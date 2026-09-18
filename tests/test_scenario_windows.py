"""Explicit training/holdout plans survive validation and result collection."""
import copy
import json
from types import SimpleNamespace

import pytest

from scenario_templates import generate_scenario_template, ScenarioTemplateError
from scenario_windows import build_validation_plan, validation_holdouts, VALIDATION_PLAN_FILENAME


def _payload():
    """Provide two training windows surrounding a distributed holdout."""
    return {'template': 'rolling_windows', 'start_date': '2024-01-01', 'end_date': '2024-12-31',
            'reducer': {'default': 'median', 'drawdown_worst_strategy_eq': 'max'},
            'windows': [dict(id=key, label=key, role=role, start_date=start, end_date=end,
                             scenario={'coins': ['ETH']}) for key, role, start, end in [
                ('train_a', 'training', '2024-01-01', '2024-03-31'),
                ('holdout_a', 'holdout', '2024-04-01', '2024-04-30'),
                ('train_b', 'training', '2024-05-01', '2024-12-31')]]}


def test_distributed_windows_preserve_reducer_and_metadata(tmp_path):
    """Only training is exported, while holdouts remain available in result metadata."""
    payload = _payload()
    result = generate_scenario_template(payload)
    assert result['reducer'] == payload['reducer']
    assert [s['label'] for s in result['training_scenarios']] == ['train_a', 'train_b']
    assert all(s['coins'] == ['ETH'] for s in result['training_scenarios'])
    plan = build_validation_plan({'pbgui': {'scenario_template': result['provenance']}})
    (tmp_path / VALIDATION_PLAN_FILENAME).write_text(json.dumps(plan))
    assert validation_holdouts(tmp_path) == result['holdout_scenarios']
    assert plan['parameters']['windows'] == result['parameters']['windows']


@pytest.mark.parametrize('change', ['overlap', 'outside', 'duplicate', 'role', 'no_training'])
def test_reject_invalid_window_plans(change):
    """Invalid roles, bounds, names and training/holdout leakage fail before Apply."""
    payload = _payload()
    if change == 'overlap':
        payload['windows'][1]['start_date'] = '2024-03-30'
    elif change == 'outside':
        payload['windows'][0]['start_date'] = '2023-12-31'
    elif change == 'duplicate':
        payload['windows'][1]['id'] = payload['windows'][0]['id']
    elif change == 'role':
        payload['windows'][0]['role'] = 'unknown'
    else:
        for item in payload['windows']:
            item['role'] = 'holdout'
    with pytest.raises(ScenarioTemplateError):
        generate_scenario_template(payload)


def test_training_overlap_allowed_but_sweep_rejects_it():
    """Overlapping training remains useful except for chronological Sweep accounting."""
    payload = _payload()
    payload['windows'] = [payload['windows'][0], copy.deepcopy(payload['windows'][0])]
    payload['windows'][1].update(id='other', label='other')
    assert 'overlap' in generate_scenario_template(payload)['warnings'][0]
    payload['template'] = 'sweep_cycles'
    with pytest.raises(ScenarioTemplateError, match='Sweep'):
        generate_scenario_template(payload)


def test_runner_persists_generic_validation_without_fake_sweep(tmp_path, monkeypatch):
    """The detached runner stores generic holdouts without inventing a Sweep plan."""
    import pb8_optimize_runner as runner
    folder = tmp_path / 'optimize_results' / 'one'
    folder.mkdir(parents=True)
    monkeypatch.setattr(runner.psutil, 'Process', lambda *_: SimpleNamespace(open_files=lambda: [SimpleNamespace(path=str(folder / 'all_results.bin'))]))
    plan = generate_scenario_template(_payload())['provenance']
    assert runner._persist_open_sweep_plan(tmp_path, {'_validation_windows': plan, '_sweep_cycles': None})
    assert validation_holdouts(folder) == plan['holdout_scenarios']
    assert not (folder / '.pbgui_sweep_cycles.json').exists()


def test_sweep_dragged_roles_keep_two_distributed_holdouts(tmp_path):
    """Applying eight dragged Sweep windows retains six training and two holdouts."""
    from datetime import date, timedelta

    start = date(2026, 1, 16)
    windows = []
    for index in range(8):
        left = start + timedelta(days=30 * index)
        right = left + timedelta(days=29)
        prefix = 'holdout' if index == 7 else 'train'
        windows.append(dict(id=f'w{index}', label=f'{prefix}_{index+1:02d}_{left:%Y%m%d}_30d',
                            role='holdout' if index in (1, 4) else 'training',
                            start_date=left.isoformat(), end_date=right.isoformat()))
    result = generate_scenario_template(dict(template='sweep_cycles', windows=windows,
        start_date='2026-01-01', end_date='2026-09-12'))
    assert len(result['training_scenarios']) == 6
    assert all(s['label'].startswith('train_') for s in result['training_scenarios'])
    assert [s['start_date'] for s in result['holdout_scenarios']] == ['2026-02-15', '2026-05-16']
    assert all(s['label'].startswith('holdout_') for s in result['holdout_scenarios'])
    plan = build_validation_plan({'pbgui': {'scenario_template': result['provenance']}})
    (tmp_path / VALIDATION_PLAN_FILENAME).write_text(json.dumps(plan))
    assert validation_holdouts(tmp_path) == result['holdout_scenarios']
