"""Validated explicit scenario windows shared by the visual editor and saved plans."""
from __future__ import annotations

import copy
import re
from datetime import timedelta

from scenario_templates import ScenarioTemplateError, _parse_date, _bounded_number, _bounded_int

SERVICE = 'ScenarioWindows'


def preview_windows(payload: dict) -> dict:
    """Build training-only scenarios and explicit holdouts without changing reducers."""
    start, end = _parse_date(payload.get('start_date'), 'start_date'), _parse_date(payload.get('end_date'), 'end_date')
    if start > end:
        raise ScenarioTemplateError('Base start must precede end')
    windows = payload.get('windows')
    if not isinstance(windows, list) or not 1 <= len(windows) <= 64:
        raise ScenarioTemplateError('Provide between 1 and 64 windows')
    policy = payload.get('template') == 'sweep_cycles'
    cooldown = _bounded_int(payload, 'cooldown_days', 0, 0, 3650)
    normalized, ids, labels = [], set(), set()
    for item in windows:
        if not isinstance(item, dict):
            raise ScenarioTemplateError('Invalid window')
        identifier, label = item.get('id'), item.get('label')
        if not isinstance(identifier, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,80}', identifier) or identifier in ids:
            raise ScenarioTemplateError('Window IDs must be unique safe identifiers')
        if not isinstance(label, str) or not re.fullmatch(r'[A-Za-z0-9_.-]{1,120}', label) or label in labels:
            raise ScenarioTemplateError('Window labels must be unique (letters, digits, underscore, dot or dash)')
        role = item.get('role')
        if role not in ('training', 'holdout'):
            raise ScenarioTemplateError('Window role must be training or holdout')
        left, right = _parse_date(item.get('start_date'), label), _parse_date(item.get('end_date'), label)
        if not start <= left <= right <= end:
            raise ScenarioTemplateError(f'{label}: window is outside base dates or reversed')
        scenario = copy.deepcopy(item.get('scenario') or {})
        if not isinstance(scenario, dict):
            raise ScenarioTemplateError('Invalid scenario overrides')
        # Existing scenario overrides survive editing; dates and label are authoritative.
        scenario.update(label=label, start_date=left.isoformat(), end_date=right.isoformat())
        exchanges = scenario.get('exchanges')
        if exchanges is not None and (not isinstance(exchanges, list) or not exchanges or any(
                not isinstance(ex, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,40}', ex) for ex in exchanges)):
            raise ScenarioTemplateError('Invalid scenario exchanges')
        if policy and exchanges:
            raise ScenarioTemplateError('Sweep windows must inherit base exchanges')
        normalized.append(dict(id=identifier, label=label, role=role, start_date=left.isoformat(), end_date=right.isoformat(), scenario=scenario))
        ids.add(identifier)
        labels.add(label)
    training = [w for w in normalized if w['role'] == 'training']
    holdouts = [w for w in normalized if w['role'] == 'holdout']
    if not training or len(training) > 48 or len(holdouts) > 16:
        raise ScenarioTemplateError('Use 1–48 training windows and at most 16 holdouts')
    warnings = []
    for i, a in enumerate(normalized):
        for b in normalized[i + 1:]:
            ae, be = a['scenario'].get('exchanges'), b['scenario'].get('exchanges')
            shared = not ae or not be or bool(set(ae) & set(be))
            overlaps = a['start_date'] <= b['end_date'] and b['start_date'] <= a['end_date']
            if shared and overlaps and a['role'] != b['role']:
                raise ScenarioTemplateError(f"Training/Holdout overlap: {a['label']} and {b['label']}. Move or split the training window.")
            if shared and overlaps and a['role'] == b['role'] == 'training':
                warnings.append('Training windows overlap and reuse market data.')
    if policy:
        ordered = sorted(normalized, key=lambda w: w['start_date'])
        for a, b in zip(ordered, ordered[1:]):
            if _parse_date(b['start_date'], 'start') <= _parse_date(a['end_date'], 'end') + timedelta(days=cooldown):
                raise ScenarioTemplateError('Sweep windows must not overlap and must respect cooldown days')
    training.sort(key=lambda w: (w['start_date'], w['id']))
    holdouts.sort(key=lambda w: (w['start_date'], w['id']))
    reducer = copy.deepcopy(payload.get('reducer') or {'default': 'mean'})
    if not isinstance(reducer, dict) or any(not isinstance(k, str) or v not in ('mean', 'median', 'min', 'max', 'std') for k, v in reducer.items()):
        raise ScenarioTemplateError('Invalid aggregation settings')
    parameters = {k: copy.deepcopy(payload[k]) for k in ('window_days', 'stride_days', 'exchange_mode') if k in payload}
    parameters.update(start_date=start.isoformat(), end_date=end.isoformat(), training_windows=len(training), holdout_windows=len(holdouts), windows=normalized)
    if policy:
        parameters['sweep_policy'] = dict(starting_balance=_bounded_number(payload, 'starting_balance', 1000, 1, 1e9), balance_multiplier=_bounded_number(payload, 'balance_multiplier', 2, 1.01, 100), refill_cost=_bounded_number(payload, 'refill_cost', 0, 0, 1e9), cooldown_days=cooldown)
        # Legacy sidecar readers require positive nominal length/stride; explicit dates govern execution.
        maximum = max((_parse_date(w['end_date'], 'end') - _parse_date(w['start_date'], 'start')).days + 1 for w in normalized)
        parameters.update(window_days=maximum, stride_days=maximum + cooldown)
    if holdouts:
        warnings.append('Holdouts are excluded from optimizer scenarios. Distributed holdouts are not a chronological walk-forward test.')
    provenance = dict(contract_version=2, template='sweep_cycles' if policy else 'custom_windows', template_version=2,
                      parameters=parameters, holdout_scenarios=[w['scenario'] for w in holdouts])
    return dict(contract_version=2, template=provenance['template'], template_version=2, parameters=parameters,
                training_scenarios=[w['scenario'] for w in training], holdout_scenarios=provenance['holdout_scenarios'],
                reducer=reducer, provenance=provenance, warnings=list(dict.fromkeys(warnings)),
                coverage={'status': 'date_bounds_only', 'window_count': len(normalized)})


VALIDATION_PLAN_FILENAME = '.pbgui_validation_windows.json'


def build_validation_plan(config: dict) -> dict | None:
    """Extract and validate graphical windows before native config preparation."""
    template = (config.get('pbgui') or {}).get('scenario_template') or {}
    if template.get('contract_version') != 2:
        return None
    parameters = template.get('parameters') or {}
    payload = dict(parameters, template=template.get('template'))
    payload.update(parameters.get('sweep_policy') or {})
    preview = preview_windows(payload)
    backtest = config.get('backtest')
    if isinstance(backtest, dict):
        if not backtest.get('suite_enabled'):
            return None
        if (config.get('optimize') or {}).get('write_all_results') is False:
            raise ScenarioTemplateError('Visual window plans require optimize.write_all_results=true to retain validation metadata with results.')
        actual = backtest.get('scenarios')
        expected = preview['training_scenarios']
        if not isinstance(actual, list) or len(actual) != len(expected):
            raise ScenarioTemplateError('Scenario windows no longer match training. Apply the windows again.')
        by_label = {item.get('label'): item for item in expected}
        for item in actual:
            if not isinstance(item, dict):
                raise ScenarioTemplateError('Invalid training scenario')
            normalized = copy.deepcopy(item)
            normalized.setdefault('start_date', payload['start_date'])
            normalized.setdefault('end_date', payload['end_date'])
            if normalized != by_label.get(item.get('label')):
                raise ScenarioTemplateError('Scenario windows no longer match training. Apply the windows again.')
    return preview['provenance']


def validation_holdouts(result_dir) -> list[dict]:
    """Read bounded, validated result metadata; never infer holdouts from names."""
    import json
    from logging_helpers import human_log
    path = result_dir / VALIDATION_PLAN_FILENAME
    if not path.exists() or path.is_symlink():
        return []
    try:
        if path.stat().st_size > 256 * 1024:
            raise ValueError('Validation plan exceeds size limit')
        plan = build_validation_plan({'pbgui': {'scenario_template': json.loads(path.read_text(encoding='utf-8'))}})
        return copy.deepcopy(plan['holdout_scenarios']) if plan else []
    except (OSError, ValueError, TypeError, AttributeError) as exc:
        human_log(SERVICE, 'Invalid result validation windows', level='WARNING', meta={'reason': str(exc)})
        return []
