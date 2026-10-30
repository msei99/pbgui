"""Provider-independent immutable configuration contract for AI Loop decisions."""
from __future__ import annotations

import copy

from jsonschema import Draft202012Validator

SERVICE = 'PB8Loop'


def variant_config_schema(record):
    """Expose exact reviewed values before generation without freezing optimizer search."""
    from ai_loop_windows import bound_plan, verify_config
    from pb8_loop_store import scenario_state

    initial = record['initial_config']
    schema = {'type': 'object', 'properties': {key: {'type': 'object'}
              for key in ('backtest', 'bot', 'live', 'optimize')},
              'required': ['backtest', 'bot', 'live', 'optimize'], 'additionalProperties': True}
    fixed = {}
    if not record['settings']['scenario_enabled']:
        fixed.update(scenario_state(initial)['backtest'])
    plan = bound_plan(record['settings'])
    if plan is not None:
        verify_config(plan, initial)
        fixed.update({key: initial['backtest'].get(key)
                      for key in ('start_date', 'end_date', 'scenarios', 'reducer', 'suite_enabled')})
    if fixed:
        schema['properties']['backtest'].update(
            properties={key: {'const': copy.deepcopy(value)} for key, value in fixed.items()},
            required=[key for key in fixed if key in initial['backtest']])
        schema['properties']['pbgui'] = {'const': copy.deepcopy(initial.get('pbgui', {}))}
        if 'pbgui' in initial:
            schema['required'].append('pbgui')
    return schema


def validate_variant_config(record, config):
    """Enforce the same contract locally even without native structured-output support."""
    validator = Draft202012Validator(variant_config_schema(record))
    error = next(validator.iter_errors(config), None)
    if error is not None:
        # Report schema locations, never credential-bearing configuration values.
        path = '.'.join(str(part) for part in error.absolute_path) or 'config'
        raise ValueError('Configuration differs from the fixed Loop configuration contract at '
                         + path + '; preserve approved values and required fields')
