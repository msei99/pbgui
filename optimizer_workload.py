"""Conservative config-only estimates of full-candidate candle workload."""
from datetime import date
import math
from pathlib import Path

from logging_helpers import human_log as _log

SERVICE = "OptimizerWorkload"


def _coins(value):
    """Accept explicit lists only; dynamic selections cannot be estimated."""
    if not isinstance(value, list) or any(not isinstance(x, str) or not x.strip() for x in value):
        raise ValueError("Explicit coins required")
    return {x.strip().upper() for x in value}


def estimate_coin_candles(config):
    """Sum inclusive date-only scenario bars times distinct selected coins.

    Counts one full candidate, without warm-up, data availability, exchange
    multiplication, holdouts or optimizer iteration/population multipliers.
    Return None when required inputs cannot be resolved conservatively.
    """
    try:
        if not isinstance(config, dict):
            return None
        bt, live = config.get('backtest'), config.get('live')
        if not isinstance(bt, dict) or not isinstance(live, dict):
            return None
        interval = bt.get('candle_interval_minutes', 1)
        if isinstance(interval, bool) or not isinstance(interval, (int, float)) or not math.isfinite(interval) or interval <= 0:
            return None
        approved = live.get('approved_coins')
        ignored = live.get('ignored_coins') or {}
        if not isinstance(approved, dict) or not isinstance(ignored, dict):
            return None
        base = set().union(*(_coins(approved.get(side, [])) - _coins(ignored.get(side, [])) for side in ('long', 'short')))
        scenarios = bt.get('scenarios') if bt.get('suite_enabled') else [{}]
        if not isinstance(scenarios, list) or not scenarios:
            return None
        total = 0
        for scenario in scenarios:
            if not isinstance(scenario, dict):
                return None
            overrides = scenario.get('overrides') or {}
            if not isinstance(overrides, dict) or any(str(key).split('.')[0] in ('live', 'backtest') for key in overrides):
                return None
            coins = base if scenario.get('coins') is None else _coins(scenario['coins'])
            coins = coins - _coins(scenario.get('ignored_coins') or [])
            if not coins:
                return None
            start = date.fromisoformat(scenario.get('start_date') or bt['start_date'])
            end = date.fromisoformat(scenario.get('end_date') or bt['end_date'])
            days = (end - start).days + 1
            if days <= 0:
                return None
            total += math.ceil(days * 1440 / interval) * len(coins)
        return total
    except (KeyError, TypeError, ValueError, OverflowError):
        return None


def estimate_snapshot(path, root):
    """Read an existing contained config snapshot without changing runtime data."""
    path, root = Path(path), Path(root)
    try:
        if not path.exists():
            return None
        path.resolve().relative_to(root.resolve())
        if any(part.is_symlink() for part in (path, *path.parents)) or path.stat().st_size > 16 * 1024**2:
            return None
        from pb8_config import load_pb8_config
        return estimate_coin_candles(load_pb8_config(path))
    except (OSError, ValueError, TypeError, RuntimeError) as exc:
        _log(SERVICE, 'Cannot estimate optimizer snapshot: ' + type(exc).__name__, level='WARNING')
        return None
