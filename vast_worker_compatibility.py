"""Offline feature checks and export adaptation for the exact rental revision."""

import copy

from vast_scenarios import flatten_overrides

SERVICE = 'Vast'
HSL_BUDGET_SCALING_REVISIONS = frozenset({
    'b9d6692ea41266ecb4ab81d40bb5e5baef29b50a',
})
_HSL_BUDGET_SCALING_PATH = 'hsl.scale_budget_with_excess_allowance'


def _is_budget_scaling_path(path: str) -> bool:
    """Match an exact HSL path component, including the portfolio block."""
    return path == _HSL_BUDGET_SCALING_PATH or path.endswith('.' + _HSL_BUDGET_SCALING_PATH)


def _leaves(value, prefix=''):
    """Inspect atomic dotted subtree overrides as well as nested leaves."""
    for key, item in flatten_overrides(value).items():
        path = prefix + key
        if isinstance(item, dict):
            yield from _leaves(item, path + '.')
        else:
            yield path, item


def _config_roots(config: dict, *, include_inactive: bool = False):
    """Locate policy-bearing base, fixed and scenario config fragments."""
    yield 'bot', config.get('bot') or {}
    optimize = config.get('optimize') or {}
    if isinstance(optimize, dict):
        yield 'optimize.fixed_runtime_overrides', optimize.get('fixed_runtime_overrides') or {}
    backtest = config.get('backtest') or {}
    if isinstance(backtest, dict) and (include_inactive or backtest.get('suite_enabled')):
        scenarios = backtest.get('scenarios') or []
        if isinstance(scenarios, list):
            for index, scenario in enumerate(scenarios):
                if isinstance(scenario, dict):
                    yield f'backtest.scenarios.{index}.overrides', scenario.get('overrides') or {}


def _remove_disabled_hsl(value: dict, prefix: str = '') -> bool:
    """Drop only explicit false policy leaves, preserving other subtree fields."""
    changed = False
    if not isinstance(value, dict):
        return changed
    for key, item in list(value.items()):
        if not isinstance(key, str):
            continue
        path = prefix + key.strip()
        if item is False and _is_budget_scaling_path(path):
            del value[key]
            changed = True
        elif isinstance(item, dict) and _remove_disabled_hsl(item, path + '.'):
            # Avoid retaining an empty atomic override that could replace HSL.
            if not item:
                del value[key]
            changed = True
    return changed


def config_for_worker(config: dict, revision: str) -> dict:
    """Adapt a new export copy; source configs and frozen jobs stay unchanged."""
    result = copy.deepcopy(config)
    if revision not in HSL_BUDGET_SCALING_REVISIONS:
        for _prefix, root in _config_roots(result, include_inactive=True):
            _remove_disabled_hsl(root)
    return result


def worker_compatibility_errors(config: dict, revision: str, *, for_preparation: bool = False) -> list[dict]:
    """Reject unsupported requested HSL policy in base, fixed and suite inputs.

    The caller validates the rental's immutable image/revision pair. Capability
    is explicit by source revision; schema versions do not establish support.
    Only preparation may accept false, since config_for_worker removes it from
    the export copy. Frozen snapshots must already match the worker schema.
    """
    if not isinstance(config, dict):
        return [{'path': 'config', 'message': 'Cannot verify worker compatibility: expected a config object.'}]
    if revision in HSL_BUDGET_SCALING_REVISIONS:
        return []
    errors = []
    for prefix, root in _config_roots(config):
        try:
            leaves = list(_leaves(root))
        except ValueError:
            errors.append({'path': prefix, 'message': 'Cannot verify worker compatibility: expected valid override paths.'})
            continue
        for path, value in leaves:
            if (_is_budget_scaling_path(path)
                    and not (for_preparation and value is False)):
                errors.append({
                    'path': prefix + '.' + path,
                    'message': ('This worker does not recognize the disabled HSL budget scaling field. Prepare the job again to remove it from the Cloud copy.'
                                if value is False else
                                'This worker does not support HSL budget scaling. Use Local or prepare this job for a compatible worker image.'),
                })
    return errors
