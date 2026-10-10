"""Complete synthetic PB8 direction inputs, never production configurations."""
import copy


def direction_config():
    """Supply a single-coin Long-only config with all four direction bounds."""
    return {
        'live': {'strategy_kind': 'ema_anchor', 'approved_coins': {'long': ['BTC'], 'short': []}},
        'bot': {side: {'risk': {'total_wallet_exposure_limit': 1.0 if side == 'long' else 0.0,
                              'n_positions': 1 if side == 'long' else 0}}
                for side in ('long', 'short')},
        'backtest': {'exchanges': ['binance']},
        'optimize': {'iters': 512, 'n_cpus': 4, 'scoring': [{'metric': 'adg_strategy_eq', 'goal': 'max'}],
                     'limits': [], 'fixed_params': [], 'fixed_runtime_overrides': {},
                     'bounds': {f'{side}_{field}': [value, value]
                                for side, value in [('long', 1), ('short', 0)]
                                for field in ('total_wallet_exposure_limit', 'n_positions')}},
    }


def complete_direction(config):
    """Add realistic gates to older tests concerned with unrelated features."""
    result = copy.deepcopy(config)
    base = direction_config()
    for side in ('long', 'short'):
        result.setdefault('bot', {}).setdefault(side, {}).setdefault('risk', base['bot'][side]['risk'])
    bounds = result.setdefault('optimize', {}).setdefault('bounds', {})
    for side in ('long', 'short'):
        for field in ('total_wallet_exposure_limit', 'n_positions'):
            value = result['bot'][side]['risk'][field]
            bounds.setdefault(f'{side}_{field}', [value, value])
    return result
