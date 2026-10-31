"""CPU-only metric dependencies matching PB8 b9d6692 config.hsl validation.

GPU name eligibility is a separate contract. A supported name can still be
unavailable in the effective HSL mode or in a particular suite scenario.
"""
from __future__ import annotations

import copy
import json
from pathlib import Path

from vast_scenarios import flatten_overrides

SERVICE = 'PB8Loop'
_CONTRACT = json.loads((Path(__file__).resolve().parent /
                       'setup/vast_gpu_benchmark/gpu_metric_contract.json').read_text())


def _object(value):
    """Leave malformed section reporting to the structural validator."""
    return value if isinstance(value, dict) else {}


def metric_requirement(metric):
    """Resolve native aliases, currencies and statistics before HSL scope checks."""
    aliases = _CONTRACT['metric_aliases']

    def canonical(name):
        """PB8 canonical_metric_name preserves a trailing statistic."""
        for stat in _CONTRACT['stat_suffixes']:
            marker = '_' + stat
            if name.endswith(marker):
                return aliases.get(name[:-len(marker)], name[:-len(marker)]) + marker
        return aliases.get(name, name)

    name = canonical(metric.strip())
    if not name.endswith(('_usd', '_btc')):
        for prefix in ('usd_', 'btc_'):
            if name.startswith(prefix):
                name = canonical(name[len(prefix):])
                break
    name = name.removesuffix('_usd').removesuffix('_btc')
    name = canonical(name)
    for stat in _CONTRACT['stat_suffixes']:
        if name.endswith('_' + stat):
            name = name[:-len(stat)-1]
            break
    if name.startswith(('hard_stop_time_in_yellow', 'hard_stop_time_in_orange')):
        return 'removed'
    if name.startswith(('hard_stop_', 'drawdown_worst_ema_strategy_eq',
                        'drawdown_worst_mean_1pct_ema_strategy_eq')):
        return name.rsplit('_', 1)[1] if name.endswith(('_long', '_short')) else 'any'
    return None


def _apply(config, overrides):
    """Apply nested/dotted scalar patches only to a private validation copy."""
    for path, value in flatten_overrides(overrides).items():
        parts = path.split('.')
        node = config
        for part in parts[:-1]:
            node = node.setdefault(part, {})
            if not isinstance(node, dict):
                raise ValueError('Invalid metric dependency override path: ' + path)
        node[parts[-1]] = copy.deepcopy(value)


def _side_enabled(config, side):
    """Match native config-only checks, including declared coin policies."""
    bot = _object(config.get('bot'))
    base = bool(_object(_object(bot.get(side)).get('hsl')).get('enabled', False))
    if _object(config.get('live')).get('hsl_signal_mode', 'coin').strip().lower() != 'coin':
        return base
    patches = _object(config.get('coin_overrides'))
    datasets = _object(config.get('backtest')).get('coins')
    if isinstance(datasets, dict):
        selected = {coin for coins in datasets.values() if isinstance(coins, list) for coin in coins}
        # The config-only validator has no market aliases: retain base policies
        # for dataset members without an explicit override.
        return any(_patch_enabled(patches.get(coin), side, base) for coin in selected)
    return base or any(_patch_enabled(patch, side, base) for patch in patches.values())


def _patch_enabled(patch, side, base):
    """Read one canonical inline coin policy without opening filesystem paths."""
    bot = _object(_object(patch).get('bot'))
    return bool(_object(_object(bot.get(side)).get('hsl')).get('enabled', base))


def metric_dependency_errors(config, coin_overrides=None):
    """Validate only enabled objectives/limits consuming each effective scenario.

    This is shared by all Loop providers and cloud preflight. It never edits the
    configuration, enables HSL, changes a metric or starts a native process.
    """
    entries = []
    optimize = _object(config.get('optimize'))
    for group in ('scoring', 'limits'):
        values = optimize.get(group)
        for index, entry in enumerate(values if isinstance(values, list) else []):
            if isinstance(entry, str):
                entry = {'metric': entry}
            if not isinstance(entry, dict) or (group == 'limits' and entry.get('enabled', True) is False):
                continue
            metric = entry.get('metric')
            if isinstance(metric, str) and metric_requirement(metric):
                target = entry.get('scenario', optimize.get('objective_scenario') if group == 'scoring' else None)
                if isinstance(target, str):
                    target = target.strip() or None
                entries.append((f'optimize.{group}.{index}.metric', metric, target))
    if not entries:
        return []
    base = copy.deepcopy(config)
    patches = _object(base.get('coin_overrides'))
    for coin, patch in _object(coin_overrides).items():
        if isinstance(patch, dict):
            target = copy.deepcopy(_object(patches.get(coin)))
            try:
                _apply(target, patch)
            except ValueError as exc:
                return [{'path': 'coin_overrides', 'message': str(exc)}]
            patches[coin] = target
    if patches:
        base['coin_overrides'] = patches
    try:
        _apply(base, optimize.get('fixed_runtime_overrides') or {})
    except ValueError as exc:
        return [{'path': 'optimize.fixed_runtime_overrides', 'message': str(exc)}]
    bt = _object(base.get('backtest'))
    scenarios = bt.get('scenarios') if bt.get('suite_enabled') else None
    contexts = [(None, base)]
    if isinstance(scenarios, list) and scenarios:
        contexts = []
        for scenario in scenarios:
            if not isinstance(scenario, dict):
                continue  # Structural validation reports malformed scenarios.
            effective = copy.deepcopy(base)
            try:
                _apply(effective, scenario.get('overrides') or {})
            except ValueError as exc:
                return [{'path': 'backtest.scenarios', 'message': str(exc)}]
            contexts.append((scenario.get('label'), effective))
    errors = []
    for label, effective in contexts:
        mode = _object(effective.get('live')).get('hsl_signal_mode', 'coin')
        mode = mode.strip().lower() if isinstance(mode, str) else None
        for path, metric, target in entries:
            if label is not None and target is not None and target != label:
                continue
            required = metric_requirement(metric)
            message = None
            if required == 'removed':
                message = 'is removed for HSL; choose a supported objective/limit'
            elif mode not in ('coin', 'pside', 'unified'):
                message = 'requires a valid live.hsl_signal_mode (coin, pside or unified)'
            elif required in ('long', 'short'):
                if mode == 'unified':
                    message = 'has no side controller in unified HSL; use the portfolio metric'
                elif not _side_enabled(effective, required):
                    message = 'has no enabled side controller in this HSL scenario'
            else:
                enabled = (bool(_object(_object(effective.get('bot')).get('hsl')).get('enabled', False))
                           if mode == 'unified' else any(_side_enabled(effective, side) for side in ('long', 'short')))
                if not enabled:
                    message = 'has no enabled controller in this HSL scenario'
            if message:
                context = f'Scenario {label!r}: ' if label is not None else ''
                errors.append({'path': path, 'message': context + metric + ' ' + message + '.',
                               'suggestions': ['Preserve the reviewed HSL policy. Choose a metric available in every scenario it consumes.']})
    return errors
