"""Verified Vast CPU training evidence without duplicate local simulations."""
from __future__ import annotations

import copy
import math
from datetime import datetime, timezone

from pb8_loop_store import digest, evaluate, qualified_winner, scenario_state
from pb8_loop_metrics import report_metric_evidence

SERVICE = 'PB8LoopNative'
VERSION = 1
TOP_COUNT = 5
IMPORT_ATTEMPTS = 3
PROTOCOL = '''
Vast research fast path: verified native exact CPU training reports replace duplicate
local training simulations. The controller ranks up to five distinct qualified configs
by the explained training criteria and hard user limits. Missing native metrics remain
unavailable. Native population coverage refers to loaded candidates, not every proxy.
Local validation allowance covers baseline/recovery only; native CPU evidence and
user-only background reports do not consume it. Never budget duplicate local training
checks or wait for observer results before planning the next optimizer run.
Holdout and Full Time Range run in the background for those five configs and remain
excluded from AI decisions, optimizer recommendations and learned evidence.
'''


def enabled(record):
    """Only versioned Vast research Loops adopt direct native CPU evidence."""
    return (record.get('optimizer_research_version') == 1
            and record.get('settings', {}).get('execution') == 'vast' and not local_fallback(record))


def local_fallback(record):
    """A failed native import changes only its own round to bounded local validation."""
    state = record.get('native_training_fallback') or {}
    return state.get('round') == record.get('round') and state.get('active') is True


def _finite(value):
    """Accept finite metric values without treating arbitrary booleans as numbers."""
    return type(value) in (int, float) and math.isfinite(value)


def _dates(value):
    """Normalize equivalent native ISO date spellings without dropping time components."""
    if isinstance(value, dict):
        result = {}
        for key, child in value.items():
            if key in {'start_date', 'end_date'} and isinstance(child, str):
                date = datetime.fromisoformat(child.replace('Z', '+00:00'))
                child = date.replace(tzinfo=timezone.utc) if date.tzinfo is None else date.astimezone(timezone.utc)
                result[key] = child.isoformat()
            else:
                result[key] = _dates(child)
        return result
    if isinstance(value, list):
        return [_dates(child) for child in value]
    return value


def conditions(config):
    """Compare simulation settings, excluding worker transport paths and display metadata."""
    backtest = config.get('backtest') or {}
    value = scenario_state(config)['backtest']
    for key in ('coins', 'ignored_coins', 'candle_interval_minutes', 'btc_collateral_cap',
                'btc_collateral_ltv_cap', 'balance_sample_divider', 'limit_order_fill_buffer_pct',
                'gap_tolerance_ohlcvs_minutes', 'volume_normalization'):
        value[key] = backtest.get(key)
    return _dates(value)


def observation(record, job, data, suite, labels, summary=None):
    """Require owned exact artifacts, matching inputs and explicit per-window safety evidence."""
    from pb8_loop_research import signature
    backend = job.get('backend') or {}
    if (not enabled(record) or job.get('kind') != 'optimizer' or not job.get('research')
            or backend.get('execution') != 'vast' or not backend.get('id')
            or job.get('research_verification', {}).get('status') != 'verified'):
        raise ValueError('Native CPU evidence requires an owned verified Vast optimizer')
    contract = data.get('optimizer_evaluation_contract') or {}
    implementation = contract.get('implementation') or {}
    metrics = data.get('metrics') or {}
    validation = metrics.get('gpu_validation') or {}
    exact = validation.get('exact_objectives')
    if (contract.get('version') != 2 or not implementation.get('python_source_sha256')
            or not isinstance(exact, list) or not exact or not all(_finite(v) for v in exact)
            or not isinstance(metrics.get('objectives'), dict)
            or len(metrics['objectives']) != len(exact)
            or not all(_finite(value) for value in metrics['objectives'].values())):
        raise ValueError('Artifact lacks verifiable exact CPU evaluation metadata')
    executed = job.get('executed_config') or job['config']
    actual_conditions, expected_conditions = conditions(data), conditions(executed)
    # PB8 adds the resolved exchange/combined coin map at exact evaluation time.
    actual_coins = actual_conditions.pop('coins')
    expected_coins = expected_conditions.pop('coins')
    def coins(value):
        if isinstance(value, dict):
            return set().union(*(coins(child) for child in value.values())) if value else set()
        return set(value) if isinstance(value, list) and all(isinstance(v, str) for v in value) else set()
    if expected_coins is None:
        expected_coins = {}
        for side, values in (executed.get('live', {}).get('approved_coins') or {}).items():
            slots = (executed.get('bot', {}).get(side, {}).get('risk') or {}).get('n_positions', 0)
            bounds = (executed.get('optimize', {}).get('bounds', {}).get(side, {}).get('risk') or {}).get('n_positions', [])
            if slots > 0 or len(bounds) >= 2 and bounds[1] > 0:
                expected_coins[side] = values
    if (actual_conditions != expected_conditions or not coins(actual_coins)
            or coins(actual_coins) != coins(expected_coins)):
        raise ValueError('Native result conditions differ from its executed optimizer input')
    if conditions(job['config']) != conditions({'backtest': record['comparison']}):
        raise ValueError('Optimizer conditions differ from the reviewed training comparison')
    backtest = data.get('backtest') or {}
    windows = backtest.get('scenarios') if backtest.get('suite_enabled') else None
    if windows:
        expected = [window.get('label') for window in windows]
        if (any(not isinstance(label, str) or not label for label in expected)
                or len(set(expected)) != len(expected) or set(labels) != set(expected)):
            raise ValueError('Native CPU windows are missing, duplicated or unidentified')
        per_report = []
        contexts = []
        effective = (data.get('suite_metrics') or {}).get('scenarios') or {}
        for window in windows:
            label = window['label']
            report = {name: block['scenarios'][label] for name, block in suite.items()
                      if isinstance(block, dict) and isinstance(block.get('scenarios'), dict)
                      and _finite(block['scenarios'].get(label))}
            contexts.append({'index': len(per_report), 'label': label, 'source': 'vast_cpu_exact',
                             'requested_start_date': window.get('start_date', backtest.get('start_date')),
                             'requested_end_date': window.get('end_date', backtest.get('end_date')),
                             'effective_start_date': (effective.get(label) or {}).get('effective_start_date'),
                             'effective_end_date': (effective.get(label) or {}).get('effective_end_date')})
            per_report.append(report)
    else:
        report = {key: value for key, value in (summary or {}).items() if _finite(value)}
        if 'liquidated_usd' not in report and type(metrics.get('liquidated')) is bool:
            report['liquidated_usd'] = int(metrics['liquidated'])
        per_report = [report]
        contexts = [{'index': 0, 'label': None, 'source': 'vast_cpu_exact',
                     'requested_start_date': backtest.get('start_date'),
                     'requested_end_date': backtest.get('end_date'),
                     'effective_start_date': None, 'effective_end_date': None}]
    for report in per_report:
        if (not _finite(report.get('backtest_completion_ratio'))
                or report.get('liquidated_usd') not in (0, 1)):
            raise ValueError('Native CPU report lacks explicit completion or liquidation evidence')
        report['liquidated'] = report['liquidated_usd']
        for name in ('exponential_fit_error', 'equity_choppiness', 'equity_jerkiness'):
            if name not in report and name + '_usd' in report:
                report[name] = report[name + '_usd']
    package = report_metric_evidence(record, per_report)
    package['metric_basis']['source'] = 'vast_cpu_exact'
    assessment = evaluate(package['metrics'], record['rubric'])
    complete = all(report['backtest_completion_ratio'] >= .999999 for report in per_report)
    safe = all(report['liquidated_usd'] == 0 for report in per_report)
    assessment.update(simulation_complete=complete, comparable=assessment['comparable'] and complete,
                      achieved=complete and all(evaluate(report, record['rubric'])['achieved'] for report in per_report),
                      hard_targets_met=safe and all(evaluate(report, record['rubric'])['hard_targets_met']
                                                   for report in per_report))
    assessment['qualification'] = 'incomplete' if not complete else ('target violated' if not assessment['hard_targets_met'] else 'comparable')
    return {**package, 'reports': len(per_report), 'exact': True, 'per_report': per_report,
            'per_report_context': contexts, 'assessment': assessment,
            'training_source': {'version': VERSION, 'kind': 'vast_cpu_exact',
                                'optimizer_operation': job['operation'], 'vast_job_id': backend['id'],
                                'input_signature': signature(executed, job['overrides']),
                                'evaluation_contract': digest(contract),
                                'dataset': record.get('fingerprint', {}).get('data')}}


def select(record, candidates):
    """Rank up to five distinct qualified configs by the explained training/user criteria."""
    import statistics
    criteria = (record.get('evaluation_method') or (record.get('optimizer_assessment') or {}).get('criteria')
                or [{'metric': rule['metrics'][0], 'direction': rule['direction'],
                     'reducer': 'mean' if rule['direction'] == 'max' else 'max'}
                    for rule in record.get('rubric', []) if rule.get('metrics') and rule.get('target') is None])
    reducers = {'mean': statistics.fmean, 'min': min, 'max': max, 'median': statistics.median}
    verified = [candidate for candidate in candidates if (candidate.get('native_training') or {}).get('exact')]
    eligible = []
    for index, candidate in enumerate(verified):
        result = candidate['native_training']
        if not qualified_winner(result):
            continue
        values = []
        for rule in criteria:
            column = [report.get(rule['metric']) for report in result['per_report']]
            if rule.get('reducer') not in reducers or not column or not all(_finite(v) for v in column):
                break
            value = reducers[rule['reducer']](column)
            values.append(value if rule['direction'] == 'min' else -value)
        else:
            if values:
                eligible.append((tuple(values), index, candidate))
    selected, seen = [], set()
    for _, _, candidate in sorted(eligible, key=lambda item: (item[0], item[1])):
        identity = digest([candidate['config']['bot'], candidate.get('overrides') or {},
                           candidate['config'].get('live', {}).get('strategy_kind')])
        if identity in seen:
            continue
        seen.add(identity)
        selected.append(candidate)
        if len(selected) == TOP_COUNT:
            break
    qualified_count = sum(qualified_winner(candidate['native_training']) for candidate in verified)
    populations = {}
    for candidate in candidates:
        counts = populations.setdefault(candidate.get('optimizer_job'), {'offered': 0, 'verified': 0, 'qualified': 0})
        counts['offered'] += 1
        if candidate.get('native_training', {}).get('exact'):
            counts['verified'] += 1
            counts['qualified'] += int(qualified_winner(candidate['native_training']))
    return selected, {'source': 'vast_cpu_exact', 'offered': len(candidates), 'verified': len(verified),
                      'scope': 'loaded native candidates, not all proxy evaluations',
                      'populations': populations,
                      'qualified': qualified_count, 'selected': len(selected), 'criteria': criteria,
                      'ranking_available': bool(selected) or qualified_count == 0,
                      'unavailable': len(candidates) - len(verified),
                      'errors': list(dict.fromkeys(c.get('native_training_error') for c in candidates
                                                  if c.get('native_training_error')))[:5]}


def distinct(candidates):
    """Preserve candidate order while removing identical effective bot settings."""
    result, seen = [], set()
    for candidate in candidates:
        identity = digest([candidate['config']['bot'], candidate.get('overrides') or {},
                           candidate['config'].get('live', {}).get('strategy_kind')])
        if identity not in seen:
            result.append(candidate)
            seen.add(identity)
    return result
