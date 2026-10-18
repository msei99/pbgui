"""Validate GPU search capabilities without imposing Template evaluation goals."""
from __future__ import annotations

SERVICE = 'AILoopObjectives'


def validate_objective(value):
    """Accept a measurable CPU goal, including metrics unavailable to GPU proxies."""
    from vast_config_validation import _METRIC_CONTRACT
    metrics = set(_METRIC_CONTRACT['supported_metrics']) | set(_METRIC_CONTRACT['exact_only_metrics'])
    if (not isinstance(value, dict) or set(value) != {'metric', 'goal', 'reducer'}
            or not isinstance(value.get('metric'), str)
            or not isinstance(value.get('goal'), str)
            or value.get('metric') not in metrics
            or value.get('goal') not in {'max', 'min'}
            or value.get('reducer') != ('mean' if value.get('goal') == 'max' else 'max')):
        raise ValueError('Template objective must use a supported CPU evaluation metric and reducer: mean for max, max for min; the evaluation goal cannot be substituted')
    return dict(value)


def scoring_objectives(config):
    """Validate every effective proxy metric/basis against the full GPU contract."""
    from vast_config_validation import METRICS, REDUCERS
    from pb8_loop_exploration import _scoring
    optimize = config.get('optimize')
    if not isinstance(optimize, dict):
        raise ValueError('GPU Template requires explicit optimizer scoring')
    rows = optimize.get('scoring')
    if not isinstance(rows, list) or not rows:
        raise ValueError('GPU Template requires explicit optimizer scoring')
    for row in rows:
        if not isinstance(row, dict) or row.get('enabled') is False:
            raise ValueError('GPU Template requires explicit active scoring entries')
        if not isinstance(row.get('metric'), str) or row['metric'] not in METRICS:
            raise ValueError('Optimizer scoring must use a supported GPU proxy metric; CPU-only evaluation metrics cannot be GPU objectives')
        if not isinstance(row.get('goal'), str) or row['goal'] not in {'min', 'max'}:
            raise ValueError('GPU proxy scoring requires an explicit min or max direction')
    result = _scoring(config)
    labels = {row.get('label') for row in config.get('backtest', {}).get('scenarios', [])}
    for row in result:
        if not isinstance(row['goal'], str) or row['goal'] not in {'min', 'max'}:
            raise ValueError('GPU proxy scoring requires min or max')
        if row['scenario'] is not None:
            if not isinstance(row['scenario'], str) or row['scenario'] not in labels:
                raise ValueError('GPU proxy scoring references an unknown training scenario')
        elif not isinstance(row['reducer'], str) or row['reducer'] not in REDUCERS:
            raise ValueError('GPU proxy scoring requires a supported reducer')
    return result


def verify_objective_config(plan, config):
    """Accept legacy metadata and validate GPU support without imposing CPU criteria."""
    objective = plan.get('objective')
    if objective is not None:
        validate_objective(objective)
        scoring_objectives(config)


def validate_template_rubric(record, config, rubric=None):
    """Validate interpreted and restored preferences before any GPU job or decision."""
    from ai_loop_windows import bound_plan
    plan = bound_plan(record.get('settings', {}))
    if plan is None or record['settings'].get('execution') not in {'gpu', 'vast'}:
        return
    verify_objective_config(plan, config)
    scoring_objectives(config)
    if rubric is None or (not rubric and record.get('phase') in {None, 'interpret'}):
        return
    # The ordinary rubric validator checks the AI's interpretation of the user request.
    # Legacy window-plan objective metadata must not impose a second metric allowlist.
    from pb8_loop_ai import validate_rubric
    validate_rubric(rubric, record['settings']['goals'])
