"""Compare free evaluation criteria against the same exact training candidates."""
from __future__ import annotations

import math

SERVICE = 'AILoopEvaluation'


def evaluation_comparison(decision, options):
    """Validate a ranking revision and recompute every option from its exact reports."""
    criteria = decision.get('evaluation_criteria')
    if criteria is None:
        return None
    if not isinstance(criteria, list) or not 1 <= len(criteria) <= 24:
        raise ValueError('Explain 1–24 measurable evaluation criteria')
    normalized = []
    for rule in criteria:
        if (not isinstance(rule, dict) or not isinstance(rule.get('metric'), str)
                or not 0 < len(rule['metric']) <= 100
                or not isinstance(rule.get('direction'), str) or rule['direction'] not in {'min', 'max'}
                or not isinstance(rule.get('reducer'), str) or rule['reducer'] not in {'mean', 'min', 'max', 'median'}
                or not isinstance(rule.get('reason'), str) or not rule['reason'].strip()):
            raise ValueError('Evaluation criteria require metric, direction, reducer and reason')
        normalized.append({key: rule[key] for key in ('metric', 'direction', 'reducer', 'reason')})
    explanations = decision.get('candidate_assessments')
    policy = decision.get('candidate_assessment_policy')
    if policy is not None:
        if (not isinstance(policy, dict) or set(policy) != {'scope', 'reason'}
                or policy['scope'] != 'all_training_candidates'
                or not isinstance(policy['reason'], str) or not policy['reason'].strip()
                or explanations is not None):
            raise ValueError('Use one common assessment policy with scope=all_training_candidates and reason, '
                             'or explicit assessments for every candidate')
        # Only the repeated prose is compressed. Every option still undergoes
        # the same exact finite-value checks and per-report recomputation below.
        explanations = dict.fromkeys(options, policy['reason'])
    if (not isinstance(explanations, dict) or set(explanations) != set(options)
            or any(not isinstance(value, str) or not value.strip() for value in explanations.values())):
        raise ValueError('Reassess every supplied training candidate under the same evaluation criteria')
    import statistics
    reducers = {'mean': statistics.fmean, 'min': min, 'max': max, 'median': statistics.median}
    table = {}
    for operation, option in options.items():
        reports = option.get('per_report') or []
        if not reports:
            raise ValueError('Changing evaluation requires exact per-report evidence for every training candidate')
        values = []
        for rule in normalized:
            observations = [report.get(rule['metric']) for report in reports]
            if any(type(value) not in (int, float) or not math.isfinite(value) for value in observations):
                raise ValueError('Evaluation metric is unavailable in a training report: ' + rule['metric'])
            values.append(reducers[rule['reducer']](observations))
        table[operation] = {'values': values, 'reason': explanations[operation]}
    return {'criteria': normalized, 'candidates': table}
