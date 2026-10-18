"""Pure validation of materially different AI Loop search proposals."""
import json
import math

from pb8_loop_store import apply_run_limit, configured_direction, digest

SERVICE = 'PB8LoopExploration'
MATERIAL_BOUND_FRACTION = 0.25
MIN_BOUND_CHANGES = 2
LIMIT_CHANGE_FRACTION = 0.20
NOVEL_AFTER_STAGNATION = 2


def _template_search_coverage(record):
    """Describe observed proxy coverage without imposing experiment quotas."""
    from ai_loop_windows import bound_plan
    if record.get('settings', {}).get('execution') not in {'gpu', 'vast'} or bound_plan(record['settings']) is None:
        return None
    from vast_config_validation import METRICS, REDUCERS
    protected = {metric for rule in record.get('rubric', []) if rule.get('target') is not None
                 for metric in rule.get('metrics', [])}
    def optional(config):
        """Hard goal filters cannot count as optional-filter exploration."""
        return {key: value for key, value in _limits(config).items() if json.loads(key)[0] not in protected}
    history_rounds = {row['round'] for row in record.get('history', []) if row.get('observations')}
    jobs = [job for job in record.get('jobs', []) if job.get('kind') == 'optimizer'
            and ((job.get('results_consumed') and job.get('result_count', 0) > 0)
                 or (job.get('started') and job.get('round') in history_rounds))]
    jobs.sort(key=lambda job: job.get('round', -1))
    configs = [job.get('executed_config') or job['config'] for job in jobs]
    scores = [_scoring(config) for config in configs]
    filters = [optional(config) for config in configs]
    tested = sorted({row['metric'] for score in scores for row in score})
    return {'required': False, 'catalog': {'metrics': sorted(METRICS), 'reducers': sorted(REDUCERS),
            'goals': ['min', 'max'], 'combinations': 'AI chooses any supported combination; no fixed shortlist'},
            'tested_scoring_signatures': list(dict.fromkeys(digest(score) for score in scores)),
            'tested_metrics': tested, 'untested_metrics': sorted(set(METRICS) - set(tested)),
            'require_untested_metric': False,
            'require_new_optional_limits': False,
            'tested_optional_limit_signatures': list(dict.fromkeys(digest(value) for value in filters)),
            'optional_limits_reference': filters[-1] if filters else {},
            'protected_limit_metrics': sorted(protected)}


def _ranges(value, prefix=''):
    """Collect finite numeric bounds without counting their quantization step."""
    result = {}
    if not isinstance(value, dict):
        return result
    for key, item in value.items():
        path = f'{prefix}.{key}' if prefix else key
        if isinstance(item, dict):
            result.update(_ranges(item, path))
        elif isinstance(item, (list, tuple)) and len(item) >= 2:
            low, high = item[:2]
            if (type(low) in (int, float) and type(high) in (int, float)
                    and math.isfinite(low) and math.isfinite(high) and high > low):
                result[path] = [low, high]
    return result


def _tunable_ranges(config):
    """Exclude inactive sides and bounds shadowed by fixed runtime overrides."""
    direction = configured_direction(config)
    active = {'long', 'short'} if direction == 'both' else {direction}
    optimize = config.get('optimize', {})
    fixed = [key.removeprefix('bot.') for key in (optimize.get('fixed_runtime_overrides') or {})]
    return {path: limits for path, limits in _ranges(optimize.get('bounds', {})).items()
            if path.split('.')[0] in active and not any(path == key or path.startswith(key + '.') for key in fixed)
            and not path.endswith('.n_positions') and not path.endswith('.hsl.enabled')}


def _basis(config, entry, scoring=False):
    """Resolve native scenario/reducer aliases so spelling changes cannot count."""
    metric = entry['metric']
    scenario = entry.get('scenario', config['optimize'].get('objective_scenario') if scoring else None)
    reducers = config.get('backtest', {}).get('reducer') or {}
    reducer = next((entry[k] for k in ('reducer', 'aggregate', 'stat', 'scenario_stat', 'field')
                    if entry.get(k) is not None), None)
    if scenario is not None:
        reducer = None
    elif reducer is None:
        reducer = reducers.get(metric) or reducers.get(metric.rsplit('_', 1)[0]) or reducers.get('default', 'mean')
    return scenario, reducer


def _scoring(config):
    """Compare objective sets, ignoring order, duplicates and presentation metadata."""
    result = {}
    for entry in config.get('optimize', {}).get('scoring', []):
        if not isinstance(entry, dict) or not isinstance(entry.get('metric'), str):
            raise ValueError('Scoring exploration requires explicit metric/goal objective objects')
        metric = entry['metric']
        goal = entry.get('goal', 'min' if metric.startswith(('drawdown', 'equity_choppiness',
                         'equity_jerkiness', 'exponential_fit_error')) else 'max')
        scenario, reducer = _basis(config, entry, scoring=True)
        spec = {'metric': metric, 'goal': goal, 'scenario': scenario, 'reducer': reducer}
        result[digest(spec)] = spec
    return [result[key] for key in sorted(result)]


def _limits(config):
    """Normalize enabled filters and collapse redundant scalar restrictions."""
    result = {}
    for entry in config.get('optimize', {}).get('limits', []):
        if not isinstance(entry, dict) or entry.get('enabled', True) is False:
            continue
        if not isinstance(entry.get('metric'), str):
            raise ValueError('Limit exploration requires named metrics')
        scenario, reducer = _basis(config, entry)
        operator = entry.get('penalize_if', 'greater_than')
        key = json.dumps([entry['metric'], operator, scenario, reducer])
        value = entry.get('range') if operator in {'inside_range', 'outside_range'} else entry.get('value')
        values = value if isinstance(value, list) else [value]
        if not values or any(type(v) not in (int, float) or not math.isfinite(v) for v in values):
            raise ValueError('Limit exploration requires finite thresholds')
        if operator in {'inside_range', 'outside_range'}:
            if len(value) != 2 or value[0] > value[1]:
                raise ValueError('Limit exploration requires an ordered two-value interval')
            intervals = result.setdefault(key, [])
            if value not in intervals:
                intervals.append(value)
            intervals.sort()
            continue
        if key in result and operator in {'greater_than', 'less_than'}:
            value = (min if operator == 'greater_than' else max)(result[key], value)
        result[key] = value
    for key, value in list(result.items()):
        operator = json.loads(key)[1]
        if operator == 'outside_range':
            result[key] = [[max(v[0] for v in value), min(v[1] for v in value)]]
        elif operator == 'inside_range':
            merged = []
            for low, high in value:
                if merged and low <= merged[-1][1]:
                    merged[-1][1] = max(merged[-1][1], high)
                else:
                    merged.append([low, high])
            result[key] = merged
    return result


def _material_limits(before, after):
    """A new/removed filter or a 20-percent threshold change is a distinct experiment."""
    return _material_limit_values(_limits(before), _limits(after))


def _material_limit_values(old, new):
    """Compare normalized optional filters using the same materiality threshold."""
    if old.keys() != new.keys():
        return True
    for key, value in old.items():
        left = [endpoint for interval in value for endpoint in interval] if isinstance(value, list) else [value]
        right = [endpoint for interval in new[key] for endpoint in interval] if isinstance(new[key], list) else [new[key]]
        if len(left) != len(right):
            return True
        if any(abs(a - b) >= LIMIT_CHANGE_FRACTION * max(abs(a), 1e-12)
               or math.isclose(abs(a - b) / max(abs(a), 1e-12), LIMIT_CHANGE_FRACTION, rel_tol=1e-12)
               for a, b in zip(left, right) if a != b):
            return True
    return False


def _differences(before, after):
    """Classify meaningful changes in effective bounds, scoring or search filters."""
    old, new = _tunable_ranges(before), _tunable_ranges(after)
    bounds = [path for path, (low, high) in old.items() if path in new
              and max(abs(new[path][0] - low), abs(new[path][1] - high)) / (high - low)
              >= MATERIAL_BOUND_FRACTION]
    approaches = []
    if old and len(bounds) >= min(MIN_BOUND_CHANGES, len(old)):
        approaches.append('bounds')
    if _scoring(before) != _scoring(after):
        approaches.append('scoring')
    if _material_limits(before, after):
        approaches.append('limits')
    return approaches, bounds


def _search_state(config):
    """Fingerprint effective search inputs without seeds or hardware/step noise."""
    return {'bounds': _tunable_ranges(config), 'scoring': _scoring(config), 'limits': _limits(config)}


def _protect_hard_filters(record, config):
    """Keep explicit goal filters at least as strict as their authorized effective floor."""
    targets = {}
    for rule in record.get('rubric', []):
        if rule.get('target') is not None:
            for metric in rule.get('metrics', []):
                targets.setdefault(metric, []).append(rule)
    if not targets:
        return
    initial = apply_run_limit(record['settings'], record['initial_config'])
    new = _limits(config)
    for key, value in _limits(initial).items():
        metric, operator, _, _ = json.JSONDecoder().raw_decode(key)[0]
        for rule in targets.get(metric, []):
            target = rule['target']
            if operator == 'greater_than' and rule['direction'] == 'min':
                preserved = key in new and new[key] <= target
            elif operator == 'less_than' and rule['direction'] == 'max':
                preserved = key in new and new[key] >= min(value, target)
            else:
                preserved = key in new and new[key] == value
            if not preserved:
                raise ValueError('Search exploration must preserve the hard user-goal filter for ' + metric)


def evaluated_stagnation(record, winner_operation):
    """Include the current evaluation exactly once when deciding whether novelty is due."""
    previous = (record.get('best') or record.get('baseline') or {}).get('operation', 'baseline')
    if winner_operation is not None and winner_operation != previous:
        return 0
    recorded = any(item.get('round') == record.get('round') for item in record.get('history', []))
    return record.get('stagnation', 0) + (0 if recorded else 1)


def exploration_policy(record):
    """Describe the next experiment relative to the last actual optimizer input."""
    jobs = [job for job in record.get('jobs', []) if job.get('kind') == 'optimizer']
    reference_job = max(enumerate(jobs), key=lambda item: (item[1].get('round', -1), item[0]))[1] if jobs else None
    reference = ((reference_job.get('executed_config') or reference_job.get('config'))
                 if reference_job else record['initial_config'])
    ranges = _tunable_ranges(reference)
    history = {item['round']: item for item in record.get('history', [])}
    validated = {job['operation'] for job in record.get('jobs', [])
                 if job.get('kind') == 'validation' and job.get('status') in {'complete', 'completed'}}
    experiments = []
    for job in jobs:
        config = job.get('executed_config') or job['config']
        exact = [o for o in history.get(job.get('round'), {}).get('observations', [])
                 if (o.get('exact') or o.get('operation') in validated) and not o.get('observer_only')]
        experiments.append({'round': job.get('round', 0) + 1, 'operation': job.get('operation'),
                            'search_signature': digest(_search_state(config)),
                            'scoring': _scoring(config), 'limits': _limits(config),
                            'exact_candidate_gains': [o['metrics']['gain_strategy_eq'] for o in exact
                                                     if 'gain_strategy_eq' in o.get('metrics', {})],
                            'exact_training_metrics': [o.get('metrics', {}) for o in exact]})
    policy = {'mode': 'broad', 'allowed_approaches': ['bounds', 'scoring', 'limits'],
            'minimum_material_bound_changes': min(MIN_BOUND_CHANGES, len(ranges)),
            'material_fraction': MATERIAL_BOUND_FRACTION,
            'limit_change_fraction': LIMIT_CHANGE_FRACTION,
            'novel_after_non_improving_rounds': NOVEL_AFTER_STAGNATION,
            'stagnation': record.get('stagnation', 0),
            'recent_experiments': experiments[-12:],
            'tested_search_signatures': list(dict.fromkeys(item['search_signature'] for item in experiments)),
            'reference_operation': reference_job.get('operation') if reference_job else None,
            'reference_scoring': _scoring(reference), 'reference_limits': _limits(reference),
            'reference_bounds': ranges}
    coverage = _template_search_coverage(record)
    if coverage is not None:
        policy['scoring_exploration'] = coverage
        policy['mode'] = 'adaptive'
        policy['minimum_material_bound_changes'] = 0
        policy['material_fraction'] = None
        policy['limit_change_fraction'] = None
        policy['novel_after_non_improving_rounds'] = None
        policy['experiment_selection'] = 'AI chooses order, combinations and controlled repetitions from training evidence'
        policy['scoring_reasoning_requirements'] = [
            'State each metric direction and training reducer/basis and why the combination may serve the user request.',
            'ADG measures daily compounding, not arithmetic mean total-period Gain across windows.',
            'Multiple objectives expose tradeoffs on a Pareto front; explain candidate selection using independent CPU training evidence.',
            'Compare actual training evidence, including failed hypotheses; never infer results for untested variants.',
            'Use only training windows; never select a scoring or limit from Holdout or Full Time Range results.']
    return policy


def validate_exploration(record, config, *, stagnation_after=None):
    """Reject cosmetic follow-up edits before consuming another optimizer attempt."""
    jobs = [job for job in record.get('jobs', []) if job.get('kind') == 'optimizer']
    previous = max(enumerate(jobs), key=lambda item: (item[1].get('round', -1), item[0]))[1] if jobs else None
    reference = (previous.get('executed_config') or previous['config']) if previous else record['initial_config']
    _protect_hard_filters(record, config)
    coverage = _template_search_coverage(record)
    if coverage is not None:
        # Repeated seeds and single-axis experiments can establish whether a result repeats.
        # The AI chooses the experiment; hard filters are still checked above.
        return _differences(reference, config)[0]
    approaches, bounds = _differences(reference, config)
    state = _search_state(reference)
    if not state['bounds'] and not state['scoring'] and not state['limits']:
        return []  # A completely fixed legacy search has no available experiment controls.
    if not approaches:
        raise ValueError(
            f'Broad exploration requires a different search approach; found {len(bounds)} material bound changes. '
            'Change two related tunable bounds by 25% of their previous width (one if only one exists), '
            'or change the effective scoring objectives/basis, or add/remove an optional search filter '
            'or move its threshold by at least 20%. Seeds, mutation, steps, ordering and labels do not count. '
            'Preserve hard user-goal filters and explain the experiment.')
    stagnation = record.get('stagnation', 0) if stagnation_after is None else stagnation_after
    if stagnation >= NOVEL_AFTER_STAGNATION:
        for job in jobs:
            tested = job.get('executed_config') or job['config']
            if not _differences(tested, config)[0]:
                raise ValueError('After two non-improving rounds choose an untested, materially different '
                                 'search approach; this proposal repeats a previous search. Tiny bound noise '
                                 'or a fresh seed does not make it new.')
    return bounds if 'bounds' in approaches else approaches
