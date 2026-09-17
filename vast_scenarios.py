"""Pure cloud suite planning matching the pinned PB8 scenario input contract."""
from __future__ import annotations

import copy
import math

from vast_exchanges import SUPPORTED_EXCHANGES

SERVICE = 'Vast'
SCENARIO_FIELDS = {'label', 'start_date', 'end_date', 'coins', 'ignored_coins',
                   'exchanges', 'coin_sources', 'overrides'}
# Pinned gpu_backend.GPU_SUPPORTED_SUITE_NON_BOT_OVERRIDE_PATHS, excluding
# coin_overrides: external/sparse per-coin bundles need separate export support.
GPU_NON_BOT_OVERRIDES = {
    'backtest.dynamic_wel_by_tradability', 'backtest.filter_by_min_effective_cost',
    'backtest.liquidation_threshold', 'backtest.maker_fee_override',
    'backtest.market_order_slippage_pct', 'backtest.starting_balance', 'backtest.taker_fee_override',
    'live.forager_score_hysteresis_pct', 'live.hedge_mode', 'live.hsl_signal_mode',
    'live.market_order_near_touch_threshold', 'live.market_orders_allowed',
    'live.max_realized_loss_pct', 'live.pnls_max_lookback_days',
}


def flatten_overrides(value, prefix=''):
    """Flatten nested documents with PB8's dotted-key and atomic-map semantics."""
    if not isinstance(value, dict):
        raise ValueError('overrides must be an object')
    result = {}
    for key, item in value.items():
        if not isinstance(key, str) or not key.strip():
            raise ValueError('override paths must be non-empty strings')
        key = key.strip()
        path = prefix + key
        if isinstance(item, dict) and '.' not in key and path.split('.')[0] != 'coin_overrides':
            nested = flatten_overrides(item, path + '.')
        else:
            nested = {path: item}
        if result.keys() & nested.keys():
            raise ValueError('duplicate override path: ' + ', '.join(sorted(result.keys() & nested.keys())))
        result.update(nested)
    return result


def scenario_plan(config):
    """Describe active scenarios and merged source assignments without editing input.

    PB8 shares the union of coins/exchanges across dataset preparation, while
    explicit scenario coins replace the base selection for that scenario. Empty
    exchanges inherit; null coins inherit, but an explicit empty coin list does not.
    """
    errors, contexts, sources = [], [], {}
    bt = config.get('backtest') or {}
    live = config.get('live') or {}
    if not isinstance(bt, dict) or not isinstance(config.get('live'), dict):
        return [], {}, []  # The top-level validator reports malformed sections.

    def error(path, message):
        """Keep structured paths for editor highlighting and preparation errors."""
        errors.append({'path': path, 'message': message})

    def coin_list(value, path):
        """Require explicit lists; never turn a filename into an implicit universe."""
        if not isinstance(value, list) or any(not isinstance(x, str) or not x.strip() or x != x.strip() for x in value):
            error(path, 'Use an explicit list of coin names, without empty entries or filenames.')
            return []
        return sorted(set(value))

    def exchange_list(value, path):
        """Validate every scenario venue before it reaches mapping paths or CCXT."""
        if isinstance(value, str):
            value = [value]
        if not isinstance(value, list) or not value:
            error(path, 'Select at least one exchange: ' + ', '.join(SUPPORTED_EXCHANGES) + '.')
            return []
        valid = []
        for index, exchange in enumerate(value):
            if not isinstance(exchange, str) or exchange not in SUPPORTED_EXCHANGES:
                error(f'{path}.{index}', f'Unsupported exchange {exchange!r}; supported: ' + ', '.join(SUPPORTED_EXCHANGES) + '.')
            elif exchange not in valid:
                valid.append(exchange)
        return valid

    def merge_sources(value, path, label):
        """Reject inconsistent suite-wide assignments, as PB8 does before preload."""
        if value is None:
            return
        if not isinstance(value, dict):
            error(path, label + ': coin_sources must map coin names to exchanges.')
            return
        for coin, exchange in value.items():
            if exchange is None:
                continue
            if not isinstance(coin, str) or not coin.strip() or coin != coin.strip():
                error(path, label + ': coin_sources contains an invalid coin name.')
                continue
            if not isinstance(exchange, str) or exchange not in SUPPORTED_EXCHANGES:
                error(path + '.' + coin, f'{label}: unsupported source exchange {exchange!r}.')
            elif coin in sources and sources[coin] != exchange:
                error(path + '.' + coin, f'{label}: {coin} is assigned to {exchange}, but another suite assignment requires {sources[coin]}. PB8 requires one consistent source per coin.')
            else:
                sources[coin] = exchange

    approved = live.get('approved_coins') or {}
    base_coins = sorted(set(approved.get('long', []) + approved.get('short', []))) if (
        isinstance(approved, dict) and all(isinstance(approved.get(s), list)
        and all(isinstance(x, str) for x in approved[s]) for s in ('long', 'short'))) else []
    base_exchanges = bt.get('exchanges', [])
    merge_sources(bt.get('coin_sources'), 'backtest.coin_sources', 'Base configuration')
    active = bt.get('scenarios') if bt.get('suite_enabled') else None
    if not isinstance(active, list) or not active:
        return [{'label': 'Base configuration', 'path': 'backtest', 'coins': base_coins,
                 'exchanges': base_exchanges, 'config': config}], sources, errors
    for index, scenario in enumerate(active):
        path = f'backtest.scenarios.{index}'
        if not isinstance(scenario, dict):
            continue  # Structural validator reports this.
        label = f'Scenario {index + 1} {scenario.get("label", "")!r}'
        unknown = set(scenario) - SCENARIO_FIELDS
        if unknown:
            error(path, label + ': unknown fields: ' + ', '.join(sorted(unknown))
                  + '. Allowed fields: ' + ', '.join(sorted(SCENARIO_FIELDS)) + '.')
        coins = base_coins if scenario.get('coins') is None else coin_list(scenario['coins'], path + '.coins')
        if not 1 <= len(coins) <= 64:
            error(path + '.coins', label + f': GPU requires 1–64 selected coins; this scenario has {len(coins)}. Omit coins or use null to inherit the base selection; [] selects no coins.')
        if scenario.get('ignored_coins') is not None:
            coin_list(scenario['ignored_coins'], path + '.ignored_coins')
        exchanges = exchange_list(scenario.get('exchanges') or base_exchanges, path + '.exchanges')
        merge_sources(scenario.get('coin_sources'), path + '.coin_sources', label)
        effective = copy.deepcopy(config)
        effective['backtest'].update(exchanges=exchanges)
        effective['live']['approved_coins'] = {'long': coins, 'short': coins}
        for field in ('start_date', 'end_date'):
            if scenario.get(field):
                effective['backtest'][field] = scenario[field]
        try:
            overrides = flatten_overrides({} if scenario.get('overrides') is None else scenario['overrides'])
        except ValueError as exc:
            error(path + '.overrides', label + ': ' + str(exc))
            overrides = {}
        for key, value in overrides.items():
            parts = key.split('.')
            is_bot = len(parts) >= 3 and parts[0] == 'bot' and parts[1] in ('long', 'short')
            if not is_bot and key not in GPU_NON_BOT_OVERRIDES:
                error(path + '.overrides.' + key, label + f': override {key!r} is outside the cloud GPU scenario scope. Use the scenario coins/exchanges/date fields for data selection; per-coin override bundles are not exported.')
                continue
            target = effective
            for part in parts[:-1]:
                target = target.get(part) if isinstance(target, dict) else None
            if not isinstance(target, dict) or parts[-1] not in target:
                error(path + '.overrides.' + key, label + f': override path {key!r} does not exist in the base configuration. Use its canonical PB8 parameter path.')
                continue
            old = target[parts[-1]]
            if (isinstance(value, (dict, list)) or type(value) not in (str, bool, int, float, type(None))
                    or (isinstance(value, float) and not math.isfinite(value))
                    or (type(old) is str and type(value) is not str)
                    or (type(old) is bool and type(value) is not bool)
                    or (type(old) in (int, float) and type(value) not in (int, float))):
                error(path + '.overrides.' + key, label + f': override {key!r} must have a compatible finite scalar value.')
                continue
            target[parts[-1]] = copy.deepcopy(value)
        contexts.append({'label': label, 'path': path, 'coins': coins, 'exchanges': exchanges, 'config': effective})
    return contexts, sources, errors
