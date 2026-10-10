"""CPU-only direction contract for PB8 00ce7d0; no installed PB8 imports.

Only the four side-gating values are projected. Other GPU capability checks
remain upstream. Source locations and independent expectations live in
tests/fixtures/vast_direction_contract.json.
"""
from __future__ import annotations

import copy
from dataclasses import dataclass
from decimal import Decimal
import math

from vast_scenarios import flatten_overrides, scenario_plan

SERVICE = 'Vast'
SIDES = ('long', 'short')
FIELDS = ('total_wallet_exposure_limit', 'n_positions')
DIRECTION_SUGGESTIONS = [
    'Long-only: keep Long active with strictly positive effective exposure and valid positions; keep Short disabled in the effective bounds, runtime values and every scenario.',
    'Long + Short: approve both sides\' coins and keep both effective exposures strictly positive and positions at least 1; use valid position bounds for the dataset.',
]


@dataclass(frozen=True)
class DirectionBound:
    """Represent the pinned PB8 bound and its seed projection."""

    low: float
    high: float
    step: float | None = None

    @classmethod
    def parse(cls, raw):
        """Accept structural-validator-approved native bound formats."""
        if not isinstance(raw, list):
            return cls(_number(raw), _number(raw))
        if not 1 <= len(raw) <= 3:
            raise ValueError('Direction bounds need one, two or three entries.')
        low = _number(raw[0])
        high = _number(raw[1]) if len(raw) > 1 else low
        if low > high:
            raise ValueError('Direction bound minimum exceeds maximum.')
        step = _number(raw[2]) if len(raw) == 3 and raw[2] is not None else None
        if step is not None and (step <= 0 or (high != low and step > high - low)):
            step = None
        return cls(low, high, step)

    def project(self, value, digits):
        """Clamp stepped values first; round continuous values before clamping."""
        value = _number(value)
        if self.step:
            clamped = max(self.low, min(self.high, value))
            index = min(int((self.high - self.low + 1e-9) / self.step),
                        int((clamped - self.low) / self.step + .5))
            value = self.low + max(0, index) * self.step
            places = max(0, -Decimal(str(self.low)).as_tuple().exponent,
                         -Decimal(str(self.step)).as_tuple().exponent)
            if places:
                value = round(value, places)
        elif digits is not None and value != 0:
            value = round(value, digits - int(math.floor(math.log10(abs(value)))) - 1)
        return max(self.low, min(self.high, value))


def _number(value):
    """Keep invalid gates from masquerading as a disabled side."""
    if type(value) not in (int, float) or not math.isfinite(value):
        raise ValueError('Expected a finite numeric direction value.')
    return float(value)


def _at(config, path):
    """Read an existing canonical path, including parent override paths."""
    node = config
    for part in path:
        node = node[part]
    return node


def _set(config, path, value):
    """Assign only existing paths in our private configuration copy."""
    _at(config, path)
    node = config
    for part in path[:-1]:
        node = node[part]
    node[path[-1]] = copy.deepcopy(value)


def _path(side, field):
    """Return the canonical PB8 side gate path."""
    return ('bot', side, 'risk', field)


def _selector_matches(path, selector):
    """Match PB8 prefix/suffix selectors with whole-segment wildcards."""
    parts = tuple(part.strip() for part in selector.split('.') if part.strip())
    if parts and parts[0] in SIDES:
        parts = ('bot', *parts)
    if len(parts) == 3 and parts[:1] == ('bot',) and parts[-1] in FIELDS:
        parts = (*parts[:2], 'risk', parts[-1])
    if not parts or len(parts) > len(path):
        return False
    return any(all(a == '*' or a == b for a, b in zip(parts, candidate))
               for candidate in (path[:len(parts)], path[-len(parts):]))


def _raw_bounds(config):
    """Resolve flat native keys and canonical nested risk bounds."""
    raw = config.get('optimize', {}).get('bounds', {})
    result = {}
    for side in SIDES:
        for field in FIELDS:
            key = f'{side}_{field}'
            if key in raw:
                result[key] = raw[key]
                continue
            nested = raw.get(side, {})
            if isinstance(nested, dict):
                risk = nested.get('risk', {})
                if isinstance(risk, dict) and field in risk:
                    result[key] = risk[field]
                elif field in nested:
                    result[key] = nested[field]
    return result


def _override_path(raw):
    """Resolve side-qualified and legacy flat gate paths like PB8."""
    parts = tuple(part.strip() for part in raw.split('.'))
    if not all(parts):
        raise ValueError('Override paths must not be empty.')
    if parts[0] in SIDES:
        parts = ('bot', *parts)
    if len(parts) == 3 and parts[0] == 'bot' and parts[1] in SIDES and parts[2] in FIELDS:
        parts = (*parts[:2], 'risk', parts[2])
    return parts


def _apply(config, overrides):
    """Apply normalized overrides without inventing missing paths."""
    paths = []
    for raw, value in overrides.items():
        path = _override_path(raw)
        _set(config, path, value)
        paths.append(path)
    return paths


def gpu_sides(config):
    """Mirror gpu_side_enabled: positive exposure, rounded positions, coins."""
    approved = config['live']['approved_coins']
    return {side for side in SIDES if approved.get(side)
            and _number(_at(config, _path(side, FIELDS[0]))) > 0
            and round(_number(_at(config, _path(side, FIELDS[1])))) > 0}


def materialize_direction(config):
    """Project fixed_params, side-collapse, seed and runtime pins in that order.

    Returns private effective config, vector values and effective check bounds.
    Globally dead PB8 parameters only concern close retracement, never these gates.
    """
    effective = copy.deepcopy(config)
    opt = config.get('optimize', {})
    raw = _raw_bounds(config)
    bounds = {key: DirectionBound.parse(value) for key, value in raw.items()}
    selectors = opt.get('fixed_params') or []
    if not isinstance(selectors, list) or any(not isinstance(s, str) for s in selectors):
        raise ValueError('optimize.fixed_params must be a list of selectors.')
    digits = opt.get('round_to_n_significant_digits', 6)
    if digits is not None and type(digits) is not int:
        raise ValueError('round_to_n_significant_digits must be an integer or null.')
    for side in SIDES:
        for field in FIELDS:
            key, path = f'{side}_{field}', _path(side, field)
            value = _number(_at(config, path))
            if key in bounds and any(_selector_matches(path, s) for s in selectors):
                bounds[key] = DirectionBound(value, value)
        enabled = all(bounds.get(f'{side}_{field}', DirectionBound(0, 0)).high > 0
                      for field in FIELDS)
        if not enabled:
            for field in FIELDS:
                key = f'{side}_{field}'
                if key in bounds:
                    bound = bounds[key]
                    bounds[key] = DirectionBound(bound.low, bound.low, bound.step)
    base = {}
    for side in SIDES:
        for field in FIELDS:
            key, path = f'{side}_{field}', _path(side, field)
            if key in bounds:
                base[key] = bounds[key].project(_at(config, path), digits)
                _set(effective, path, base[key])
    runtime = opt.get('fixed_runtime_overrides') or {}
    if not isinstance(runtime, dict):
        raise ValueError('optimize.fixed_runtime_overrides must be an object.')
    paths = _apply(effective, runtime)
    for side in SIDES:
        for field in FIELDS:
            key, path = f'{side}_{field}', _path(side, field)
            # _gpu_fixed_bound_context pins exact optimizer paths, not parents.
            if path in paths:
                value = _number(_at(effective, path))
                base[key], bounds[key] = value, DirectionBound(value, value)
    return effective, base, bounds


def scenario_direction(effective, base, bounds, overrides, coins=None):
    """Build an early or late scenario, then shadow existing bound keys only."""
    scenario = copy.deepcopy(effective)
    if coins is not None:
        scenario['live']['approved_coins'] = {side: list(coins) for side in SIDES}
    paths = _apply(scenario, overrides)
    values, edges = dict(base), dict(bounds)
    for side in SIDES:
        for field in FIELDS:
            key, path = f'{side}_{field}', _path(side, field)
            if key in bounds and any(path[:len(parent)] == parent for parent in paths):
                value = _number(_at(scenario, path))
                values[key], edges[key] = value, DirectionBound(value, value)
    return scenario, values, edges


def validate_gpu_directions(config, *, coin_counts=None):
    """Return separate errors/hints; counts are authoritative only when supplied.

    Declared dataset sizes never grant an unconditional multicoin exemption.
    Count-sensitive findings are hints until dataset preparation determines them.
    """
    errors, warnings = [], []

    def issue(path, message, label='Base configuration', *, uncertain=False, bound_keys=None):
        """Produce the same actionable contract in the editor and snapshot gate."""
        finding = {
            'path': path, 'message': f'{label}: {message}',
            'suggestions': list(DIRECTION_SUGGESTIONS),
        }
        if uncertain:
            finding['pending_coin_count'] = True
        if bound_keys:
            finding['bound_keys'] = bound_keys
        (warnings if uncertain else errors).append(finding)

    try:
        effective, base, bounds = materialize_direction(config)
        if not gpu_sides(effective):
            issue('bot', 'GPU foundation requires at least one enabled side.')
        contexts, _, planning_errors = scenario_plan(config)
        if planning_errors:
            # Snapshot gates also need these errors; the editor reports them
            # before entering this direction projection.
            return errors + planning_errors, warnings
        suite = bool(config.get('backtest', {}).get('suite_enabled'))
        scenes = []
        for context in contexts if suite else []:
            index = int(context['path'].rsplit('.', 1)[-1])
            definition = config['backtest']['scenarios'][index]
            overrides = flatten_overrides(definition.get('overrides') or {})
            early, _, _ = scenario_direction(effective, base, bounds, overrides)
            if not gpu_sides(early):
                issue(context['path'] + '.overrides',
                      'GPU foundation requires at least one enabled side in the base approved-coin context.',
                      context['label'])
            late, values, edges = scenario_direction(effective, base, bounds, overrides, context['coins'])
            count = (coin_counts or {}).get(context['path'])
            scenes.append((context, late, values, edges, count, len(context['coins'])))
        max_declared = max((scene[-1] for scene in scenes), default=len(set(
            sum((config['live']['approved_coins'][side] for side in SIDES), []))))
        actual = [scene[-2] for scene in scenes]
        known_multi = bool(actual) and all(type(n) is int and n >= 1 for n in actual) and max(actual) > 1
        known_single = bool(actual) and all(n == 1 for n in actual)
        possible_max = max([max_declared, *(n for n in actual if type(n) is int)], default=max_declared)
        maybe_multi = bool(scenes) and possible_max > 1 and not all(actual)
        if known_multi or maybe_multi:
            topologies = {tuple(sorted(gpu_sides(scene[1]))) for scene in scenes}
            if len(topologies) != 1 or () in topologies:
                issue('backtest.scenarios', 'GPU multicoin suites require the same nonempty enabled-side topology in every scenario.',
                      uncertain=not known_multi)

        def directional(values, edges, approved, enabled, count, declared, prefix, label, conditional=False):
            """Mirror every branch of _validate_directional_search_space."""
            for side in SIDES:
                expo_key, pos_key = (f'{side}_{field}' for field in FIELDS)
                expo = edges.get(expo_key, DirectionBound(values.get(expo_key, 0), values.get(expo_key, 0)))
                pos = edges.get(pos_key, DirectionBound(values.get(pos_key, 0), values.get(pos_key, 0)))
                path = prefix + 'bot.' + side + '.risk.'
                can_activate = bool(approved.get(side)) and expo.high > 0 and pos.high > 0
                side_name = side.title()
                exposure_range = f'[{expo.low:g}, {expo.high:g}]'
                positions_range = f'[{pos.low:g}, {pos.high:g}]'
                if can_activate != (side in enabled):
                    crossing = [field for field, edge in zip(FIELDS, (expo, pos)) if edge.low <= 0 < edge.high]
                    detail = []
                    if FIELDS[0] in crossing:
                        detail.append(f'{side_name} exposure (WE) range {exposure_range} includes 0 (off) and positive values (on).')
                    if FIELDS[1] in crossing:
                        detail.append(f'{side_name} positions range {positions_range} includes 0 (off) and positive values (on).')
                    if not detail:
                        detail.append(f'The effective start values (WE {values.get(expo_key, 0):g}, positions {values.get(pos_key, 0):g}) and search ranges (WE {exposure_range}, positions {positions_range}) disagree about whether {side_name} is on.')
                    issue(path + (crossing[0] if crossing else FIELDS[0]),
                          f'{side_name} must stay on or off during GPU optimization. ' + ' '.join(detail) +
                          ' To keep it on, use a WE minimum above 0 and at least 1 position, with matching start values. To keep it off, fix WE or positions at 0.',
                          label, uncertain=conditional,
                          bound_keys=[f'{side}.risk.{field}' for field in (crossing or FIELDS)] if not prefix else None)
                elif side in enabled:
                    if expo.low <= 0:
                        issue(path + FIELDS[0],
                              f'{side_name} must stay on or off during GPU optimization. {side_name} exposure (WE) range {exposure_range} includes 0 (off) and positive values (on). '
                              'To keep it on, set the WE minimum above 0. To keep it off, fix WE at 0.',
                              label, uncertain=conditional,
                              bound_keys=[f'{side}.risk.{FIELDS[0]}'] if not prefix else None)
                    position_error = ((pos.low, pos.high) != (1, 1) if count == 1 else
                                      not 1 <= pos.low <= pos.high <= count if count else
                                      pos.low < 1 or pos.high != 1 or pos.low != 1)
                    if position_error:
                        uncertain = conditional or (count is None and declared > 1 and pos.low >= 1)
                        issue(path + FIELDS[1], f'GPU positions must be pinned at 1 for one coin or within [1, coin_count] for multiple coins; effective bounds are [{pos.low:g}, {pos.high:g}].' +
                              (' Prepared coin count is not known yet.' if uncertain else ''), label, uncertain=uncertain,
                              bound_keys=[f'{side}.risk.{FIELDS[1]}'] if not prefix else None)
        if not known_multi:
            approved = effective['live']['approved_coins']
            enabled = {side for side in SIDES if approved.get(side)
                       and base.get(f'{side}_{FIELDS[0]}', 0) > 0 and base.get(f'{side}_{FIELDS[1]}', 0) > 0}
            if not enabled:
                issue('bot', 'GPU bounds would disable both sides.', uncertain=maybe_multi)
            count = 1 if known_single or max_declared == 1 else (coin_counts or {}).get('backtest')
            directional(base, bounds, approved, enabled, count, max_declared, '', 'Base configuration', maybe_multi)
        for context, scenario, values, edges, count, declared in scenes:
            count = count or (1 if declared == 1 else None)
            directional(values, edges, scenario['live']['approved_coins'], gpu_sides(scenario), count, declared,
                        context['path'] + '.', context['label'])
    except (AttributeError, KeyError, TypeError, ValueError, OverflowError) as exc:
        issue('optimize.bounds', 'Cannot resolve GPU direction inputs: ' + str(exc))
        if isinstance(exc, (AttributeError, KeyError, TypeError)):
            errors[-1]['uncheckable'] = True
    return errors, warnings
