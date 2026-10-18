"""Authoritative, explicit date windows reviewed in the AI Loop Template."""
from __future__ import annotations

import json
import re
from datetime import timedelta
from pathlib import Path

from scenario_windows import preview_windows, build_validation_plan
from scenario_templates import _parse_date

SERVICE = 'AILoopWindows'
MARKER = 'PBGUI_LOOP_WINDOWS_V1:'
HOLDOUT_EVIDENCE_POLICY = 1


def validate_plan(value: dict) -> dict:
    """Validate the restricted Template contract without changing Scenario Builder rules."""
    if not isinstance(value, dict) or value.get('version') != 1:
        raise ValueError('Invalid reviewed Loop window plan')
    enabled, count = value.get('use_holdout'), value.get('training_windows')
    if type(enabled) is not bool or type(count) is not int or not 1 <= count <= 48:
        raise ValueError('Use 1–48 training windows and an explicit holdout choice')
    preview = preview_windows(value)
    windows = preview['parameters']['windows']
    training = [w for w in windows if w['role'] == 'training']
    holdouts = [w for w in windows if w['role'] == 'holdout']
    if len(training) != count or len(holdouts) != int(enabled):
        raise ValueError('Reviewed training/holdout window counts do not match')
    ordered = sorted(windows, key=lambda w: w['start_date'])
    for w in ordered:
        if (_parse_date(w['end_date'], 'end') - _parse_date(w['start_date'], 'start')).days < 1:
            raise ValueError('Each window needs at least two calendar days')
    if any(a['end_date'] >= b['start_date'] for a, b in zip(ordered, ordered[1:])):
        raise ValueError('Reviewed Loop windows must not overlap')
    if enabled and ordered[-1]['role'] != 'holdout':
        raise ValueError('Holdout must follow all training windows')
    result = dict(version=1, start_date=preview['parameters']['start_date'],
                end_date=preview['parameters']['end_date'], use_holdout=enabled,
                training_windows=count, windows=windows)
    if 'objective' in value:
        from ai_loop_objectives import validate_objective
        result['objective'] = validate_objective(value['objective'])
    if 'max_twe' in value:
        from ai_loop_search_limits import validate_twe_ceiling
        result['max_twe'] = validate_twe_ceiling(value['max_twe'])
    if 'max_validations' in value:
        if type(value['max_validations']) is not int or not 1 <= value['max_validations'] <= 1000:
            raise ValueError('CPU training validation budget must be an integer from 1 to 1000')
        result['max_validations'] = value['max_validations']
    return result


def plan_from_text(text: str) -> dict | None:
    """Read only the explicit contract in the original visible user request."""
    if MARKER not in text:
        return None
    matches = re.findall(r'^' + MARKER + r'([^\n]+)$', text, re.MULTILINE)
    if len(matches) != 1 or text.count(MARKER) != 1:
        raise ValueError('Provide exactly one reviewed Loop window plan')
    try:
        return validate_plan(json.loads(matches[0]))
    except (ValueError, TypeError, KeyError) as exc:
        raise ValueError('Invalid reviewed Loop window plan: ' + str(exc)) from exc


def reviewed_window_request(messages: list[dict]) -> tuple[dict | None, str | None]:
    """Keep the latest explicit user window plan across marker-free follow-up messages."""
    for message in reversed(messages):
        if message.get('role') != 'user' or message.get('failed') or message.get('hidden'):
            continue
        text = message.get('content')
        if not isinstance(text, str):
            continue
        plan = plan_from_text(text)
        if plan is not None:
            return plan, text
    return None, None


def conversation_window_request(conversation: dict) -> tuple[dict | None, str | None]:
    """Read the durable user binding, migrating legacy history only when absent."""
    text = conversation.get('loop_window_request')
    if text is None:
        return reviewed_window_request(conversation.get('messages', []))
    if not isinstance(text, str) or len(text) > 8000:
        raise ValueError('Invalid persisted reviewed Loop window request')
    plan = plan_from_text(text)
    if plan is None:
        raise ValueError('Invalid persisted reviewed Loop window request')
    return plan, text


def verify_config(plan: dict, config: dict) -> None:
    """Require exact reviewed windows, overrides, aggregation and holdout provenance."""
    if not isinstance(config, dict):
        raise ValueError('The reviewed optimizer configuration is unavailable')
    plan = validate_plan(plan)
    expected = preview_windows(plan)
    actual = build_validation_plan(config)
    bt = config.get('backtest') or {}
    if (actual is None or actual.get('contract_version') != 2
            or actual.get('parameters', {}).get('windows') != plan['windows']
            or bt.get('start_date') != plan['start_date'] or bt.get('end_date') != plan['end_date']
            or bt.get('scenarios') != expected['training_scenarios']
            or actual.get('holdout_scenarios') != expected['holdout_scenarios']
            or bt.get('reducer') != expected['reducer']):
        raise ValueError('Configuration differs from the reviewed Loop windows; apply the exact explicit windows')
    from ai_loop_objectives import verify_objective_config
    verify_objective_config(plan, config)
    from ai_loop_search_limits import validate_exposure_ceiling
    validate_exposure_ceiling(config, plan.get('max_twe'))


def bound_plan(settings: dict) -> dict | None:
    """Derive policy from the original request rather than model-editable settings."""
    binding = (settings.get('source') or {}).get('loop_request') or {}
    return plan_from_text(binding.get('window_request', binding.get('text', '')))


def protected_holdout_plan(record: dict) -> dict | None:
    """Scope evidence restrictions to reviewed Template Loops that request Holdout."""
    plan = bound_plan(record.get('settings') or {})
    return plan if plan is not None and plan['use_holdout'] else None


def require_holdout_evidence_policy(record: dict) -> None:
    """Do not reuse decisions made before historical evidence lineage was enforced."""
    if protected_holdout_plan(record) is not None and record.get('holdout_evidence_policy') != HOLDOUT_EVIDENCE_POLICY:
        raise ValueError('This protected Holdout run predates verified historical evidence; create a fresh reviewed Loop')


def historical_training_windows(record: dict, backtest: dict) -> list[dict] | None:
    """Prove effective artifact dates from original data, without config fallbacks."""
    plan = protected_holdout_plan(record)
    if plan is None or not isinstance(backtest, dict):
        return None
    suite = backtest.get('suite_enabled', False)
    if type(suite) is not bool:
        return None
    rows = backtest.get('scenarios') if suite else [{}]
    if not isinstance(rows, list) or not 1 <= len(rows) <= 48 or any(not isinstance(row, dict) for row in rows):
        return None
    try:
        # Adjacent reviewed Training parts can safely prove one continuous old run.
        allowed = []
        for window in sorted((w for w in plan['windows'] if w['role'] == 'training'), key=lambda w: w['start_date']):
            left, right = _parse_date(window['start_date'], 'start'), _parse_date(window['end_date'], 'end')
            if allowed and left <= allowed[-1][1] + timedelta(days=1):
                allowed[-1] = (allowed[-1][0], max(right, allowed[-1][1]))
            else:
                allowed.append((left, right))
        proof = []
        for row in rows:
            left = _parse_date(row.get('start_date', backtest.get('start_date')), 'historical start')
            right = _parse_date(row.get('end_date', backtest.get('end_date')), 'historical end')
            if left > right or not any(start <= left <= right <= end for start, end in allowed):
                return None
            proof.append({'start_date': left.isoformat(), 'end_date': right.isoformat()})
        return proof
    except (ValueError, TypeError):
        return None


def filter_historical_candidates(record: dict, candidates: list[dict]) -> list[dict]:
    """Recheck server-produced provenance at restored/bootstrap/archive boundaries."""
    if protected_holdout_plan(record) is None:
        return candidates
    return [candidate for candidate in candidates if historical_training_windows(record, {
        'suite_enabled': True, 'scenarios': (candidate.get('source') or {}).get('training_windows')}) is not None]


def stored_plan(chat_root: Path, owner: str, conversation_id: str) -> dict | None:
    """Read the original request from its owned atomic conversation snapshot."""
    from secure_files import read_regular_file_nofollow
    if any(not isinstance(value, str) or not re.fullmatch(r'[0-9a-f]{32}', value)
           for value in (owner, conversation_id)):
        raise ValueError('Invalid owned conversation')
    root = Path(chat_root) / 'conversations'
    path = root / owner / (conversation_id + '.json')
    if not path.exists():
        return None
    if path.stat().st_size > 4 * 1024 * 1024:
        raise ValueError('Conversation history is too large')
    conversation = json.loads(read_regular_file_nofollow(path, root))
    if conversation.get('owner') != owner or conversation.get('conversation_id') != conversation_id:
        raise ValueError('Invalid owned conversation snapshot')
    return conversation_window_request(conversation)[0]
