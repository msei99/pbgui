"""Provider-independent immutable configuration contract for AI Loop decisions."""
from __future__ import annotations

import copy

from jsonschema import Draft202012Validator

SERVICE = 'PB8Loop'
WORKER_SETTINGS = ('gpu', 'n_cpus', 'population_size')


def worker_reference(record):
    """Use a trusted restoration snapshot for existing runs, otherwise their reviewed input."""
    restored = record.get('worker_runtime')
    if isinstance(restored, dict) and restored.get('version') == 1 and isinstance(restored.get('settings'), dict):
        return {'optimize': restored['settings']}
    return record['initial_config']


def worker_settings_schema(config):
    """Keep user-owned execution settings, including Auto/null and legacy aliases, fixed."""
    optimize = config.get('optimize') or {}
    return {'type': 'object',
            'properties': {key: {'const': copy.deepcopy(optimize.get(key))} for key in WORKER_SETTINGS},
            'required': [key for key in WORKER_SETTINGS if key in optimize]}


def validate_worker_settings(record, config):
    """Enforce execution ownership at backend boundaries, independently of any provider."""
    validator = Draft202012Validator(worker_settings_schema(worker_reference(record)))
    error = next(validator.iter_errors(config.get('optimize') or {}), None)
    if error is not None:
        path = '.'.join(str(part) for part in error.absolute_path) or 'worker settings'
        raise ValueError('Worker execution settings are fixed at optimize.' + path
                         + '; preserve the approved settings, including Auto/null. '
                         'Change strategy bounds, scoring or permitted search filters instead.')


def copy_worker_settings(source, target):
    """Inherit reviewed worker settings rather than an optimizer winner's hardware edits."""
    result = copy.deepcopy(target)
    optimize = result.setdefault('optimize', {})
    for key in WORKER_SETTINGS:
        if key in source.get('optimize', {}):
            optimize[key] = copy.deepcopy(source['optimize'][key])
        else:
            optimize.pop(key, None)
    return result


def variant_config_schema(record):
    """Expose exact reviewed values before generation without freezing optimizer search."""
    from ai_loop_windows import bound_plan, verify_config
    from pb8_loop_store import scenario_state

    initial = record['initial_config']
    schema = {'type': 'object', 'properties': {key: {'type': 'object'}
              for key in ('backtest', 'bot', 'live', 'optimize')},
              'required': ['backtest', 'bot', 'live', 'optimize'], 'additionalProperties': True}
    schema['properties']['optimize'] = worker_settings_schema(worker_reference(record))
    if record.get('optimizer_research_version') == 1:
        schema['properties']['optimize']['properties']['seed'] = {'const': None}
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
