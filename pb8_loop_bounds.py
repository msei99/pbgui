"""Measured search-bound pressure from qualified independent training candidates."""
from __future__ import annotations

import math
import statistics

from pb8_loop_exploration import _tunable_ranges
from pb8_loop_store import digest

SERVICE = 'PB8LoopBounds'
VERSION = 3
EDGE_FRACTION = .1


def _number(value):
    """Reject booleans and nonfinite observations."""
    return type(value) in (int, float) and math.isfinite(value)


def _leaf(value, path):
    """Read a dotted native config path without aliases or default zeroes."""
    for key in path.split('.'):
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    return value


def active_ranges(config, goals):
    """Exclude fixed parameters, disabled controllers and single-coin selection controls."""
    fixed = {str(p).removeprefix('bot.') for p in config.get('optimize', {}).get('fixed_params', [])}
    ranges = _tunable_ranges(config)
    strategy = config.get('live', {}).get('strategy_kind')
    result = {}
    for path, limits in ranges.items():
        side, _, tail = path.partition('.')
        if any(path == p or path.startswith(p + '.') for p in fixed):
            continue
        if '.hsl.' in path and not _leaf(config.get('bot', {}), side + '.hsl.enabled'):
            continue
        if '.forager.' in path and len(goals.get('coins') or []) == 1:
            continue
        if '.strategy.' in path and isinstance(strategy, str) and not tail.startswith('strategy.' + strategy + '.'):
            continue
        if '.unstuck.' in path and _leaf(config.get('bot', {}), side + '.unstuck.loss_allowance_pct') == 0:
            continue
        result[path] = limits
    return result


def _criteria(record):
    """Use previously explained CPU criteria, or an unambiguous untargeted rubric metric."""
    criteria = record.get('evaluation_method') or (record.get('optimizer_assessment') or {}).get('criteria')
    if criteria:
        return criteria
    for rule in record.get('rubric') or []:
        if rule.get('target') is None and len(rule.get('metrics') or []) == 1:
            return [{'metric': rule['metrics'][0], 'direction': rule['direction'],
                     'reducer': rule.get('reducer', 'mean')}]
    return []


def _rank(option, criteria):
    """Compare exact CPU reports only; unavailable criteria do not create rankings."""
    values = []
    for rule in criteria:
        reports = option.get('per_report') or []
        column = [r.get(rule['metric']) for r in reports]
        if not column or not all(_number(v) for v in column):
            return None
        reducer = {'mean': statistics.fmean, 'min': min, 'max': max, 'median': statistics.median}.get(rule.get('reducer'))
        if reducer is None:
            return None
        value = reducer(column)
        values.append(value if rule['direction'] == 'min' else -value)
    return tuple(values) if values else None


def diagnose(record, options, research=None):
    """Aggregate all eligible nominations against their executed bounds, without proxy/holdout data."""
    optimizers = {j['operation']: j for j in record.get('jobs', []) if j.get('kind') == 'optimizer'}
    latest = max(optimizers.values(), key=lambda j: j.get('round', -1), default=None)
    if latest is None:
        return {'version': VERSION, 'parameters': [], 'candidate_count': 0, 'signal_count': 0}
    from pb8_loop_planning import planning_context
    from pb8_loop_research import enabled, evidence as research_evidence
    if enabled(record) and research is None:
        research = research_evidence(record)
    planning = planning_context(record, research) if enabled(record) else {}
    anchor = optimizers.get(planning.get('reference_operation'), latest)
    reference = anchor.get('executed_config') or anchor['config']
    goals = record.get('settings', {}).get('goals') or {}
    ranges = active_ranges(reference, goals)
    criteria = _criteria(record)
    candidates = []
    excluded_overrides = 0
    for job in record.get('jobs', []):
        operation = job.get('operation')
        option = options.get(operation)
        if (job.get('kind') != 'validation' or job.get('diagnostic_only') or not option
                or job.get('status', 'complete') not in {'complete', 'completed'}):
            continue
        assessment = option.get('assessment') or {}
        if any(assessment.get(k) is not True for k in ('comparable', 'hard_targets_met', 'simulation_complete')):
            continue
        candidate = job.get('candidate') or {}
        source = optimizers.get(candidate.get('optimizer_job'))
        # Sparse coin overrides cannot safely be interpreted as base-bot measurements.
        if source is None:
            continue
        if candidate.get('overrides'):
            excluded_overrides += 1
            continue
        config = candidate.get('config') or {}
        executed = source.get('executed_config') or source['config']
        candidates.append((operation, config, active_ranges(executed, goals), _rank(option, criteria), source))
    ranked = sorted(c[3] for c in candidates if c[3] is not None)
    cutoff = ranked[max(0, math.ceil(len(ranked) / 4) - 1)] if ranked else None
    # Only completed physical repeats with independent qualified CPU evidence count.
    # Use persisted completion checks; current outcomes may precede the trial cache.
    from pb8_loop_research import trial_completed
    verified = set()
    cached = record.get('optimizer_trials') or {}
    for _, _, executed_ranges, _, source in candidates:
        operation = source['operation']
        if ((source.get('research') or {}).get('purpose') not in {'replicate', 'confirm'}
                or executed_ranges != ranges):
            continue
        trial = cached.get(operation)
        if trial is not None:
            complete = trial_completed(trial)
        else:
            budget = (source.get('research') or {}).get('budget') or {}
            proxy_complete = (budget.get('mode') == 'proxy'
                              and _number(budget.get('value')) and budget['value'] > 0
                              and _number(source.get('proxy_evaluations'))
                              and source['proxy_evaluations'] >= budget['value'])
            complete = (source.get('research_verification', {}).get('status') == 'verified'
                        and not source.get('error')
                        and source.get('status') in {'complete', 'completed', 'cancelled', 'stopped'}
                        and (proxy_complete or budget.get('mode') in {'hours', 'iters'}
                             and source.get('status') in {'complete', 'completed'}))
        if complete:
            verified.add(operation)
    rows = []
    for path, (low, high) in sorted(ranges.items()):
        measured = []
        for operation, config, executed_ranges, rank, source in candidates:
            if executed_ranges.get(path) != [low, high]:
                continue
            value = _leaf(config.get('bot', {}), path)
            if not _number(value) or not low <= value <= high:
                continue
            position = (value - low) / (high - low)
            measured.append((operation, position, rank is not None and rank <= cutoff, source, value))
        verified_sources = {r[3]['operation'] for r in measured if r[3]['operation'] in verified}
        good = [r for r in measured if r[2]] if cutoff is not None else measured
        for edge in ('lower', 'upper'):
            near = [r for r in good if (r[1] <= EDGE_FRACTION if edge == 'lower' else r[1] >= 1 - EDGE_FRACTION)]
            if not near:
                continue
            protected = (edge == 'upper' and path.endswith('.risk.total_wallet_exposure_limit')
                         and _number(goals.get('max_twe')) and high >= goals['max_twe'])
            sources = {r[3]['operation'] for r in near}
            rows.append({'id': digest([path, low, high, edge]), 'parameter': path, 'bounds': [low, high],
                         'edge': edge, 'qualified_count': len(measured), 'good_count': len(good),
                         'near_good_count': len(near), 'source_trial_count': len(sources),
                         'nearest_value': (min if edge == 'lower' else max)(r[4] for r in near),
                         'nearest_distance_fraction': min(r[1] if edge == 'lower' else 1 - r[1] for r in near),
                         'incumbent_near': any(r[0] == (record.get('best') or {}).get('operation') for r in near),
                         'protected': protected,
                         'zero_lower_edge': edge == 'lower' and low == 0,
                         'verification_count': len(verified_sources),
                         'retention_basis': ('user_ceiling' if protected else
                                             'zero_lower_edge' if edge == 'lower' and low == 0 else
                                             'completed_verification' if verified_sources else None),
                         'strength': 'repeated' if len(sources) >= 2 else 'single_trial',
                         'quality_basis': 'top_quartile_with_ties' if cutoff is not None else 'qualified_only'})
    # Previously measured outward tests resolve a hypothesis without pretending
    # that one lower short-run score proves the expanded region is harmful.
    from pb8_loop_research import budget
    requested = budget(record) if planning else {}
    measured = {item['operation'] for group in (research or {}).get('groups') or []
                if group.get('criteria') == planning.get('criteria')
                and all((group.get('budget') or {}).get(k) == requested.get(k) for k in ('mode', 'value'))
                for item in group.get('per_trial_best') or []}
    tested_ranges = {operation: active_ranges(job.get('executed_config') or job['config'], goals)
                     for operation, job in optimizers.items() if operation in measured}
    for row in rows:
        edge = 1 if row['edge'] == 'upper' else 0
        raw = _leaf(reference.get('optimize', {}).get('bounds', {}), row['parameter'])
        step = raw[2] if planning and isinstance(raw, list) and len(raw) > 2 and _number(raw[2]) and raw[2] > 0 else None
        movement = .1 * (row['bounds'][1] - row['bounds'][0])
        # A broad positive quantized interval can have its lower edge less than
        # 10% of the width from zero. Offer its positive step floor instead of
        # forcing a negative EMA span/size merely to satisfy the batch gate.
        positive_floor = bool(edge == 0 and step and 0 < row['bounds'][0] <= step)
        if edge == 0 and step and row['bounds'][0] > 0:
            movement = min(movement, max(0, row['bounds'][0] - step))
        row['minimum_adjustment'] = movement
        row['suggested_endpoint'] = row['bounds'][edge] + (movement if edge else -movement)
        row['positive_step_floor'] = positive_floor
        threshold = movement
        tests = []
        for operation, values in tested_ranges.items():
            if row['parameter'] not in values:
                continue
            displacement = values[row['parameter']][edge] - row['bounds'][edge]
            distance = displacement if edge else -displacement
            if distance > 0 and (distance >= threshold or math.isclose(distance, threshold, rel_tol=1e-12, abs_tol=0)):
                tests.append(operation)
        row['outward_test_count'] = len(tests)
        row['outward_test_operations'] = tests[-3:]
        row['batch_required'] = bool(planning and not tests and not row['protected']
                                     and not row['zero_lower_edge'] and not positive_floor)
    bound_signature = digest(ranges)
    unchanged = 0
    for job in sorted(optimizers.values(), key=lambda j: j.get('round', -1), reverse=True):
        if active_ranges(job.get('executed_config') or job['config'], goals) != ranges:
            break
        unchanged += 1
    from pb8_loop_parameter_regions import regions
    parameter_regions = regions(record, criteria)
    return {'version': VERSION, 'parameters': rows, 'candidate_count': len(candidates),
            'parameter_regions': parameter_regions,
            'parameter_regions_summary': {
                'available': parameter_regions['available'],
                'source_count': len(parameter_regions['profiles']),
                'preview': [{key: profile[key] for key in ('optimizer_operation', 'qualified_distinct_count', 'elite_count',
                                                         'criterion', 'requested_budget', 'actual_proxy_evaluations')} | {
                    'parameters': profile['parameters'][:3]} for profile in parameter_regions['profiles']],
                'path': '/data/evidence/boundary_diagnostics/parameter_regions'},
            'reference_operation': anchor['operation'], 'planning': planning,
            'excluded_override_count': excluded_overrides,
            'override_coverage': 'Coin-override candidates are excluded; their effective parameter values are unavailable',
            'signal_count': len(rows), 'bounds_signature': bound_signature,
            'unchanged_bounds_trials': unchanged, 'edge_fraction': EDGE_FRACTION,
            'criteria': criteria, 'coverage': 'qualified selected CPU nominations, not the proxy population'}


def validate_response(record, diagnosis, variant, *, stagnation_after=None):
    """Require a measured response; sustained pressure cannot be deferred by scoring changes forever."""
    rows = diagnosis.get('parameters') or []
    if not rows:
        return
    responses = variant.get('boundary_response')
    if not isinstance(responses, list) or len(responses) != len(rows):
        raise ValueError('Address every boundary signal in variant.boundary_response: {id, action: adjust|verify|retain, reason}')
    by_id = {}
    for response in responses:
        if (not isinstance(response, dict) or not isinstance(response.get('id'), str)
                or response['id'] in by_id or response.get('action') not in {'adjust', 'verify', 'retain'}
                or not isinstance(response.get('reason'), str) or not response['reason'].strip()):
            raise ValueError('Boundary responses require unique signal IDs, action and a reason')
        by_id[response['id']] = response
    if set(by_id) != {r['id'] for r in rows}:
        raise ValueError('Boundary response IDs must match the measured signals')
    goals = record.get('settings', {}).get('goals') or {}
    proposed = active_ranges(variant['config'], goals)
    experiment = variant.get('experiment') or {}
    if record.get('optimizer_research_version') == 1 and (
            experiment.get('purpose') in {'replicate', 'confirm'}
            or any(response['action'] == 'verify' for response in by_id.values())):
        raise ValueError('Unchanged boundary verification is forbidden; adjust or retain with a distinct optimizer experiment')
    source_ranges = None
    if experiment.get('purpose') in {'replicate', 'confirm'}:
        source = next((j for j in record.get('jobs', []) if j.get('kind') == 'optimizer'
                       and j.get('operation') == experiment.get('source_operation')), None)
        if source is None:
            raise ValueError('Boundary verification requires an owned optimizer source')
        source_ranges = active_ranges(source.get('executed_config') or source['config'], goals)
    tests = False
    sustained = False
    batch = (record.get('optimizer_research_version') == 1 and diagnosis.get('version', 0) >= 3
             and (any(r.get('action') == 'adjust' for r in by_id.values())
                  or (record.get('stagnation', 0) if stagnation_after is None else stagnation_after) >= 2))
    for row in rows:
        response = by_id[row['id']]
        action = response['action']
        if batch and row.get('batch_required') and action != 'adjust':
            raise ValueError('Address all relevant untested boundary signals together; adjust every batch_required endpoint, not one bound per round')
        interval = proposed.get(row['parameter'])
        reference = source_ranges.get(row['parameter']) if source_ranges is not None else row['bounds']
        edge = 1 if row['edge'] == 'upper' else 0
        width = reference[1] - reference[0] if reference is not None else 0
        displacement = interval[edge] - reference[edge] if interval is not None and reference is not None else 0
        distance = displacement if edge == 1 else -displacement
        threshold = row.get('minimum_adjustment', .1 * width) if source_ranges is None else .1 * width
        changed = width > 0 and distance > 0 and (distance >= threshold or math.isclose(distance, threshold, rel_tol=1e-12, abs_tol=0))
        if action == 'adjust' and row['protected']:
            raise ValueError('Use retain or verify for a protected boundary')
        if action == 'adjust' and not changed:
            raise ValueError('Boundary adjust must expand the pressured endpoint outward by the diagnosed minimum adjustment (10% or the positive quantization floor)')
        if row['protected'] and interval is not None and interval[1] > row['bounds'][1]:
            raise ValueError('Never expand a protected boundary beyond the user ceiling')
        if action == 'retain' and (interval is None) != (reference is None):
            raise ValueError('Boundary retain must preserve the reference endpoint')
        if action == 'retain' and interval is not None and interval[edge] != reference[edge]:
            raise ValueError('Boundary retain must preserve the reference endpoint')
        if action == 'verify' and experiment.get('purpose') not in {'replicate', 'confirm'}:
            raise ValueError('Boundary verify requires a controlled replicate or confirm experiment')
        if action == 'verify' and (reference is None or interval != reference):
            raise ValueError('Boundary verify must preserve the source bounds')
        if not row['protected'] and reference is not None:
            strong = (row['strength'] == 'repeated' and row['incumbent_near']
                      and row['quality_basis'] == 'top_quartile_with_ties'
                      and not (row['edge'] == 'lower' and row['bounds'][0] == 0)
                      and not (record.get('optimizer_research_version') == 1 and row.get('outward_test_count', 0))
                      and not row.get('positive_step_floor', False)
                      and not row.get('verification_count', 0))
            sustained |= strong
            tests |= strong and action in {'adjust', 'verify'}
    stagnation = record.get('stagnation', 0) if stagnation_after is None else stagnation_after
    if sustained and diagnosis.get('unchanged_bounds_trials', 0) >= 3 and stagnation >= 2 and not tests:
        raise ValueError('Repeated boundary pressure after stagnation requires an adjust or controlled verify experiment, not another scoring-only deferral')
