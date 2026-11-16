"""Native PB9 result readers and projections shared by local and archived views."""
from __future__ import annotations

import math
import os
import stat
import zipfile
from pathlib import Path

import numpy as np
from fastapi import HTTPException


def load_equity_arrays(path):
    """Read bounded native numeric artifacts without following a file symlink."""
    path = Path(path)
    if not path.exists():
        raise HTTPException(404, 'Native PB9 equity artifact not found')
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(descriptor, 'rb') as stream:
            if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                raise HTTPException(422, 'Expected a regular PB9 equity artifact')
            with zipfile.ZipFile(stream) as archive:
                if sum(item.file_size for item in archive.infolist()) > 256 * 1024 * 1024:
                    raise HTTPException(413, 'PB9 equity artifact exceeds display limit')
                # Verify declared array storage before numpy allocates from an untrusted header.
                for name in ('schema_version', 'timestamp_ms', 'usd_total_balance', 'usd_total_equity'):
                    member = archive.getinfo(name + '.npy')
                    with archive.open(member) as value:
                        version = np.lib.format.read_magic(value)
                        reader = {(1, 0): np.lib.format.read_array_header_1_0,
                                  (2, 0): np.lib.format.read_array_header_2_0}.get(version)
                        if reader is None:
                            raise ValueError('Unsupported NPY format')
                        shape, _, dtype = reader(value, max_header_size=10000)
                        count = math.prod(shape)
                        if (len(shape) != (0 if name == 'schema_version' else 1)
                                or dtype.kind not in ('iu' if name in {'schema_version', 'timestamp_ms'} else 'f')
                                or dtype.itemsize > 8 or dtype.hasobject
                                or count * dtype.itemsize > member.file_size - value.tell()):
                            raise ValueError('Invalid native NPY storage declaration')
            stream.seek(0)
            with np.load(stream, allow_pickle=False) as archive:
                if archive['schema_version'].shape != () or int(archive['schema_version']) != 1:
                    raise HTTPException(422, 'Unsupported native PB9 equity schema')
                arrays = [archive[key] for key in ('timestamp_ms', 'usd_total_balance', 'usd_total_equity')]
    except (OSError, ValueError, KeyError, zipfile.BadZipFile) as exc:
        raise HTTPException(422, 'Invalid native PB9 equity artifact') from exc
    times, balance, equity = arrays
    if (any(array.ndim != 1 or len(array) != len(times) for array in arrays)
            or times.dtype.kind not in 'iu' or np.any(np.diff(times) <= 0)
            or any(array.dtype.kind != 'f' or np.isinf(array).any() for array in (balance, equity))
            or not np.isfinite(equity).all()):
        raise HTTPException(422, 'Invalid native PB9 equity shape, clock or values')
    return times, balance, equity


def summarize_native_result(directory, config, authored, analysis, arrays, *, config_name, display_name, modified):
    """Keep native formulas and inheritance identical in local and archive tables."""
    bt, bot = config.get('backtest', {}), config.get('bot', {})
    metrics = {key: value for key, value in analysis.items() if isinstance(value, (float, int)) and math.isfinite(value)}
    def metric(*keys, default=0):
        return next((metrics[key] for key in keys if key in metrics), default)
    coins = authored.get('bot', {}).get('global', {}).get('universe', {}).get('approved_coins', [])
    final_balance = final_equity = adg = gain = None
    if arrays is not None:
        times, balances, equities = arrays
        if len(times):
            final_balance = float(balances[-1]) if math.isfinite(float(balances[-1])) else None
            final_equity = float(equities[-1])
            start = float(bt.get('starting_balance') or 0)
            gain = final_equity / start if start > 0 else None
            days = (int(times[-1]) - int(times[0])) / 86400000
            if days > 0 and equities[0] > 0 and final_equity > 0:
                exponent = math.log(final_equity / float(equities[0])) / days
                adg = math.expm1(exponent) if exponent < 700 else None
    coins = sorted({coin for side in ('long', 'short')
                    for coin in (authored.get('bot', {}).get(side, {}).get('universe', {}).get('approved_coins')
                                 or authored.get('bot', {}).get('global', {}).get('universe', {}).get('approved_coins') or [])
                    if isinstance(coin, str)})
    item = {**metrics, 'path': str(directory), 'config_name': config_name,
            'backtest_version': 'v9', 'config_version': config.get('config_version'),
            'strategy': bot.get('portfolio', {}).get('strategy_kind', ''),
            'exchange': ', '.join(bt.get('exchanges', [])), 'exchange_dir': ', '.join(bt.get('exchanges', [])), 'exchanges': bt.get('exchanges', []),
            'display_name': display_name, 'result_name': directory.name, 'metrics': metrics, 'coins_text': ', '.join(coins),
            'start_date': analysis.get('effective_start_date', bt.get('start_date')), 'end_date': analysis.get('effective_end_date', bt.get('end_date')), 'coins': coins,
            'modified': modified, 'result_time': directory.name,
            'starting_balance': metric('starting_balance_usd', 'starting_balance', default=bt.get('starting_balance', 0)),
            'final_balance': final_balance, 'final_equity': final_equity, 'gain': gain, 'adg': adg,
            'sharpe_ratio': metric('sharpe_ratio_strategy_equity', default=None),
            'drawdown_worst': metric('drawdown_max_fraction_strategy_equity', default=None),
            'liquidated': analysis.get('liquidated') is True,
            'analysis': analysis}
    for side in ('long', 'short'):
        values = {**authored.get('bot', {}).get('global', {}).get('risk', {}), **authored.get('bot', {}).get(side, {}).get('risk', {})}
        enabled = authored.get('bot', {}).get(side, {}).get('enabled') is True
        item['twe_' + side] = values.get('total_wallet_exposure_limit_ratio', 0) if enabled else 0
        item['n_positions_' + side] = values.get('max_positions', 0) if enabled else 0
        item['pos_' + side] = item['n_positions_' + side]
    return item
