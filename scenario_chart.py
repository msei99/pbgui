"""Bounded, read-only daily OHLCV context for visual scenario editing."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import re
from collections import OrderedDict
from threading import RLock
from time import monotonic

from logging_helpers import human_log as _log

SERVICE = 'ScenarioChart'
# Compact daily summaries only; no minute arrays or open files are retained.
_DAY_CACHE = OrderedDict()
_DAY_CACHE_LOCK = RLock()
_DAY_CACHE_LIMIT = 16000
_DAY_CACHE_TTL = 300

DATASETS = ('1m', 'candles_1m', '1m_api', 'candles_1m_api')


def _identifier(value: object) -> str:
    """Reject paths and control characters before resolving local data."""
    if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,100}', value) or value in ('.', '..'):
        raise ValueError('Invalid market identifier')
    return value


def _root(exchange: str) -> Path:
    """Use the existing local Market Data storage root without network calls."""
    # Resolve the documented storage alias without creating symlinks on a read.
    from market_data import _EXCHANGE_ALIASES
    name = _identifier(exchange).lower()
    return Path(__file__).resolve().parent / 'data' / 'ohlcv' / _EXCHANGE_ALIASES.get(name, name)


def _mapped_coins(root: Path, exchange: str) -> dict[str, str]:
    """Resolve local source names using the maintained USDT perpetual mapping."""
    import json
    path = root.parent.parent / 'coindata' / _identifier(exchange) / 'mapping.json'
    if not path.exists() or path.is_symlink() or path.parent.is_symlink():
        return {}
    try:
        if path.stat().st_size > 16 * 1024**2:
            raise ValueError('Mapping exceeds size limit')
        rows = json.loads(path.read_text(encoding='utf-8'))
        if not isinstance(rows, list):
            raise ValueError('Expected a market mapping list')
        return {row['ccxt_symbol'].replace('/', '_'): row['coin'] for row in rows
                if isinstance(row, dict) and row.get('quote') == 'USDT' and row.get('swap') is True
                and isinstance(row.get('ccxt_symbol'), str) and isinstance(row.get('coin'), str)}
    except (ValueError, OSError) as exc:
        _log(SERVICE, 'Reference coin mapping unavailable', level='WARNING', meta={'reason':str(exc)})
        return {}


def sources(exchange: str) -> list[dict]:
    """List local candle sources, excluding symlinks and non-candle datasets."""
    root = _root(exchange)
    result = []
    mapped = _mapped_coins(root, exchange)
    if root.is_symlink():
        return result
    for dataset in DATASETS:
        directory = root / dataset
        if directory.is_symlink() or not directory.is_dir():
            continue
        for coin in sorted(directory.iterdir()):
            if len(result) >= 2000:
                break
            if coin.is_symlink() or not coin.is_dir() or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,100}', coin.name):
                continue
            row = {'coin': coin.name, 'dataset': dataset}
            if coin.name in mapped:
                row['base_coin'] = mapped[coin.name]
            result.append(row)
    return result


def daily_chart(exchange: str, dataset: str, coin: str, start: str, end: str) -> dict:
    """Read one daily archive at a time, returning daily candles and missing days."""
    import numpy as np
    import zipfile
    left, right = date.fromisoformat(start), date.fromisoformat(end)
    if left > right or (right - left).days > 12000:
        raise ValueError('Chart range must be ordered and at most 12,000 days')
    if dataset not in DATASETS:
        raise ValueError('Unsupported candle dataset')
    root = _root(exchange)
    directory = root / dataset / _identifier(coin)
    if root.is_symlink() or (root / dataset).is_symlink() or directory.is_symlink():
        raise ValueError('Symlink candle sources are not supported')
    candles, failed = {}, 0
    paths = sorted(directory.glob('*.npz')) if directory.is_dir() else []
    if len(paths) > 16000:
        raise ValueError('Candle source exceeds file limit')
    grouped = {}
    for path in paths:
        match = re.match(r'^(\d{4})-?(\d{2})-?(\d{2})(?:\D|$)', path.stem)
        if not match:
            continue
        key = '-'.join(match.groups())
        if not start <= key <= end:
            continue
        grouped.setdefault(key, []).append(path)
    for key, day_paths in sorted(grouped.items()):
        cache_key = (str(directory), key)
        try:
            signature = tuple((p.name, p.lstat().st_mtime_ns, p.lstat().st_ctime_ns,
                               p.lstat().st_size, p.is_symlink()) for p in day_paths)
        except OSError:
            signature = None
        with _DAY_CACHE_LOCK:
            cached = _DAY_CACHE.get(cache_key)
            if cached and cached[0] == signature and monotonic() - cached[1] < _DAY_CACHE_TTL:
                _DAY_CACHE.move_to_end(cache_key)
                if cached[2] is not None:
                    candles[key] = dict(cached[2])
                continue
        day_failed = failed
        chunks = []
        for path in day_paths:
            try:
                if path.is_symlink() or path.stat().st_size > 32 * 1024**2:
                    raise ValueError('Oversized or linked candle archive')
                with zipfile.ZipFile(path) as archive:
                    if sum(info.file_size for info in archive.infolist()) > 64 * 1024**2:
                        raise ValueError('Oversized candle payload')
                with np.load(path, allow_pickle=False) as archive:
                    array = archive['candles'] if 'candles' in archive else archive[archive.files[0]]
                if len(array) > 100000 or not len(array):
                    raise ValueError('Invalid candle count')
                names = array.dtype.names
                if names:
                    def field(*choices):
                        """Read a supported structured candle column."""
                        return np.asarray(array[next(n for n in choices if n in names)], dtype=float)
                    ts, op, hi, lo, cl = field('ts', 't'), field('o', 'open'), field('h', 'high'), field('l', 'low'), field('c', 'close')
                    volume = field('v', 'bv', 'volume') if set(names) & {'v','bv','volume'} else np.zeros(len(array))
                else:
                    if array.ndim != 2 or array.shape[1] < 5:
                        raise ValueError('Invalid candle shape')
                    ts, op, hi, lo, cl = [np.asarray(array[:, i], dtype=float) for i in range(5)]
                    volume = np.asarray(array[:, 5], dtype=float) if array.shape[1] > 5 else np.zeros(len(array))
                valid = np.isfinite(ts) & np.isfinite(op) & np.isfinite(hi) & np.isfinite(lo) & np.isfinite(cl) & np.isfinite(volume)
                day_ms = int(datetime.combine(date.fromisoformat(key), datetime.min.time(), timezone.utc).timestamp() * 1000)
                valid &= (ts >= day_ms) & (ts < day_ms + 86400000)
                indices = np.flatnonzero(valid)
                if not len(indices):
                    continue
                indices = indices[np.argsort(ts[indices])]
                chunks.append(np.column_stack((ts[indices] // 60000, op[indices], hi[indices],
                                               lo[indices], cl[indices], volume[indices])))
            except (OSError, ValueError, KeyError, StopIteration, zipfile.BadZipFile, IndexError):
                failed += 1
        if chunks:
            rows = np.concatenate(chunks)
            # Last occurrence wins across archives, matching the minute merge semantics.
            _, reversed_indices = np.unique(rows[::-1, 0], return_index=True)
            rows = rows[len(rows) - 1 - reversed_indices]
            candles[key] = {'date': key, 'open': float(rows[0, 1]), 'high': float(rows[:, 2].max()),
                            'low': float(rows[:, 3].min()), 'close': float(rows[-1, 4]),
                            'volume': float(rows[:, 5].sum()), 'minutes': len(rows)}
        if signature is not None and failed == day_failed:
            with _DAY_CACHE_LOCK:
                _DAY_CACHE[cache_key] = (signature, monotonic(), dict(candles[key]) if key in candles else None)
                _DAY_CACHE.move_to_end(cache_key)
                while len(_DAY_CACHE) > _DAY_CACHE_LIMIT:
                    _DAY_CACHE.popitem(last=False)
    if failed:
        _log(SERVICE, f'{failed} local candle files could not be read', level='WARNING')
    days = [(left + timedelta(days=i)).isoformat() for i in range((right-left).days+1)]
    return {'candles': [candles[k] for k in sorted(candles)], 'missing_days': [k for k in days if k not in candles],
            'incomplete_days': [k for k in sorted(candles) if candles[k]['minutes'] < 1440], 'unreadable_files': failed,
            'source': {'exchange': exchange, 'dataset': dataset, 'coin': coin}, 'resolution': '1d', 'reference_only': True}
