"""Bounded training-only feedback for changed optimizer experiments."""
from __future__ import annotations

from pb8_loop_exploration import _limits, _scoring
from pb8_loop_store import digest

SERVICE = 'PB8LoopPlanning'
VERSION = 1


def scoring_key(config):
    """Objective order alone is not a new Pareto scoring experiment."""
    return digest(sorted(digest(item) for item in _scoring(config)))


def planning_context(record, research=None):
    """Compare all measured trials, exposing four recent outcomes and one reference."""
    from pb8_loop_research import budget, enabled, evidence
    if not enabled(record):
        return {}
    research = research if research is not None else evidence(record)
    criteria = (research.get('previous_assessment') or {}).get('criteria') or []
    jobs = {j['operation']: j for j in record.get('jobs', []) if j.get('kind') == 'optimizer'}
    requested = budget(record)
    measured = {}
    for group in research.get('groups') or []:
        work = group.get('budget') or {}
        if (not criteria or group.get('criteria') != criteria
                or any(work.get(k) != requested.get(k) for k in ('mode', 'value'))):
            continue
        for item in group.get('per_trial_best') or []:
            values = item.get('values') or []
            from pb8_loop_research import finite
            if item.get('operation') in jobs and len(values) == len(criteria) and all(finite(v) for v in values):
                measured[item['operation']] = list(values)

    def rank(operation):
        """Use the existing ordered criteria, never a new scalar or random-seed axis."""
        return tuple(v if c['direction'] == 'min' else -v
                     for c, v in zip(criteria, measured[operation]))

    reference = min(measured, key=lambda op: (rank(op), jobs[op].get('round', -1), op), default=None)
    ordered = sorted(jobs, key=lambda op: (jobs[op].get('round', -1), op))
    latest = ordered[-1] if ordered else None
    stalled = 0
    same_scoring = 0
    for operation in reversed(ordered):
        if operation not in measured:
            continue
        if reference is None or rank(operation) < rank(reference) or operation == reference:
            break
        stalled += 1
    if latest:
        for operation in reversed(ordered):
            if operation not in measured:
                continue
            if scoring_key(jobs[operation].get('executed_config') or jobs[operation]['config']) != scoring_key(
                    jobs[latest].get('executed_config') or jobs[latest]['config']):
                break
            same_scoring += 1
    recent = []
    for operation in ordered[-4:]:
        status = ('unavailable' if operation not in measured or reference is None else
                  'below_reference' if rank(operation) > rank(reference) else 'matches_reference')
        recent.append({'operation': operation, 'round': jobs[operation].get('round', -1) + 1,
                       'values': measured.get(operation), 'comparison': status})
    return {'version': VERSION, 'reference_operation': reference or 'initial',
            'reference_values': measured.get(reference), 'criteria': criteria,
            'latest_operation': latest, 'recent_trials': recent,
            'trials_without_improvement': stalled, 'unchanged_scoring_trials': same_scoring,
            'scoring_change_required': stalled >= 2 and same_scoring >= 3,
            'basis': 'Best measured qualified fixed-budget trial under the existing ordered research criteria; '
                     'a planning reference, not proof of robust superiority. Unmeasured trials are unavailable.',
            'policy': 'Branch from measured successful settings. Do not inherit unsuccessful changes by default. '
                      'Address relevant untested boundary signals together, not one endpoint per round. '
                      'Change the scoring when required; choose its metrics and Pareto combination from the supported catalog. '
                      'No repetition, seed evaluation, population reuse or changed work budget.'}


def validate_plan(record, variant, context):
    """Reject automatic widening of losing descendants and stale-scoring deferrals."""
    if not context:
        return
    jobs = {j['operation']: j for j in record.get('jobs', []) if j.get('kind') == 'optimizer'}
    latest = jobs.get(context.get('latest_operation'))
    if latest is None:
        return
    latest_config = latest.get('executed_config') or latest['config']
    config = variant['config']
    if context.get('scoring_change_required') and scoring_key(config) == scoring_key(latest_config):
        raise ValueError('Stagnation with three unchanged-scoring trials requires a different effective scoring, not another bounds-only step')
    reference = jobs.get(context.get('reference_operation'))
    recent = context.get('recent_trials') or []
    if not reference or not recent or recent[-1]['comparison'] != 'below_reference':
        return
    from pb8_loop_bounds import active_ranges
    goals = record['settings'].get('goals') or {}
    original = active_ranges(reference.get('executed_config') or reference['config'], goals)
    previous = active_ranges(latest_config, goals)
    proposed = active_ranges(config, goals)
    carried = all(proposed.get(path) == interval or (
        path in proposed and proposed[path][0] <= interval[0] and proposed[path][1] >= interval[1])
        for path, interval in previous.items() if interval != original.get(path))
    widening = set(previous) == set(proposed) and all(
        proposed[path][0] <= interval[0] and proposed[path][1] >= interval[1]
        for path, interval in previous.items()) and previous != proposed
    # Explicitly opened strategy/scenario/HSL alternatives remain available.
    from pb8_loop_research import experiment_signature
    import copy
    bounds_only = copy.deepcopy(config)
    bounds_only.setdefault('optimize', {})['bounds'] = copy.deepcopy(latest_config.get('optimize', {}).get('bounds', {}))
    if (carried and widening and scoring_key(config) == scoring_key(latest_config)
            and _limits(config) == _limits(latest_config)
            and experiment_signature(record, bounds_only) == experiment_signature(record, latest_config)):
        raise ValueError('Do not further widen an underperforming optimizer branch; rebase on measured successful settings or test a distinct scoring, limits or permitted strategy hypothesis')
