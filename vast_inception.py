"""Resolve authoritative first-candle metadata locally for isolated PB8 workers."""

import asyncio
import gzip
import hashlib
import json
import math
from pathlib import Path
import re
import shlex
import tempfile
import time

from logging_helpers import human_log
from vast_jobs import PROJECT
from vast_provider import VastError

from vast_exchanges import CCXT_EXCHANGES, SUPPORTED_EXCHANGES, quote_currency

SERVICE = 'VastRunner'
CACHE_VERSION = 2  # Resolver format used by the pinned PB8 worker revision.
CACHE_FILES = (
    'first_ohlcv_timestamps_unified.json',
    'first_ohlcv_timestamps_unified_exchange_specific.json',
    'first_ohlcv_timestamps_unified_exchange_specific_symbols.json',
)


def required_markets(manifest, mapping_root):
    """Resolve exactly the exported datasets through PBGui's market mappings."""
    datasets = set()
    for item in manifest['files']:
        name = item['path']
        if not isinstance(name, str):
            raise VastError('Invalid input manifest path', 422)
        if not name.startswith('ohlcv/'):
            continue
        parts = name.split('/')
        if (len(parts) != 5 or parts[1] not in SUPPORTED_EXCHANGES or parts[2] != '1m'
                or any(part in ('', '.', '..') or '\\' in part or any(ord(c) < 32 for c in part) for part in parts)):
            raise VastError('Invalid OHLCV dataset path', 422)
        datasets.add((parts[1], parts[3]))
    if not datasets:
        raise VastError('No OHLCV datasets available for inception metadata', 422)
    result = {}
    for exchange in sorted({ex for ex, _ in datasets}):
        path = Path(mapping_root) / exchange / 'mapping.json'
        if not path.resolve().is_relative_to(Path(mapping_root).resolve()):
            raise VastError('Invalid market mapping path', 422)
        if not path.is_file():
            raise VastError('Missing market mapping for ' + exchange, 422)
        try:
            rows = json.loads(path.read_text())
        except (ValueError, OSError):
            raise VastError('Invalid market mapping for ' + exchange, 422) from None
        if not isinstance(rows, list):
            raise VastError('Invalid market mapping for ' + exchange, 422)
        selected = {}
        for _, dataset in sorted(pair for pair in datasets if pair[0] == exchange):
            matches = [row for row in rows if isinstance(row, dict)
                       and row.get('quote') == quote_currency(exchange) and row.get('swap') is True and row.get('linear') is True
                       and isinstance(row.get('ccxt_symbol'), str)
                       and row['ccxt_symbol'].replace('/', '_') == dataset]
            if len(matches) != 1 or not isinstance(matches[0].get('coin'), str) or not matches[0]['coin']:
                raise VastError('Missing or ambiguous inception mapping: ' + exchange + '/' + dataset, 422)
            row = matches[0]
            if (not re.fullmatch(r'[A-Za-z0-9_:.+-]+', row['coin'])
                    or not re.fullmatch(r'[A-Za-z0-9_:.+/-]+', row['ccxt_symbol'])):
                raise VastError('Invalid inception market identifier', 422)
            if row['coin'] in selected and selected[row['coin']] != row['ccxt_symbol']:
                raise VastError('Ambiguous inception coin mapping', 422)
            selected[row['coin']] = row['ccxt_symbol']
        result[exchange] = selected
    return result


async def bitget_first_candles(client, symbol):
    """Find Bitget listing history with bounded backward monthly pages, then daily refinement.

    Like pinned PB8's resolver, use `until` rather than `since`: Bitget's
    historical endpoint pages backwards. Never use a local shard as listing age.
    """
    month_ms = 30 * 86400000
    candles = await client.fetch_ohlcv(symbol, timeframe='1M', params={'limit': 200})
    if not candles:
        return []
    first = min(candles, key=lambda item: item[0])
    low, high = 0, max(client.milliseconds(), max(item[0] for item in candles) + month_ms)
    for _ in range(64):
        if high - low <= month_ms:
            break
        midpoint = (low + high) // 2
        page = await client.fetch_ohlcv(symbol, timeframe='1M', params={'until': int(midpoint), 'limit': 200})
        if not page:
            low = midpoint
            continue
        candidate = min(page, key=lambda item: item[0])
        if candidate[0] >= high:
            break
        first, high = candidate, candidate[0]
    for _ in range(128):
        until = max(0, int(first[0]) - 1)
        if not until:
            break
        page = await client.fetch_ohlcv(symbol, timeframe='1M', params={'until': until, 'limit': 200})
        if not page:
            break
        candidate = min(page, key=lambda item: item[0])
        if candidate[0] >= first[0]:
            raise VastError('Bitget inception pagination did not advance for ' + symbol, 422)
        first = candidate
    else:
        raise VastError('Bitget inception pagination limit reached for ' + symbol, 422)
    daily = await client.fetch_ohlcv(symbol, timeframe='1d',
                                     params={'until': int(first[0]) + 32 * 86400000, 'limit': 200})
    return [min(daily, key=lambda item: item[0])] if daily else [first]


async def fetch_inception(markets):
    """Use PB8's first-daily-candle requests, releasing clients even on failure."""
    import ccxt.async_support as ccxt
    timestamps, symbols = {}, {}
    for exchange, coins in markets.items():
        identifier = 'binanceusdm' if exchange == 'binance' else exchange
        client = getattr(ccxt, CCXT_EXCHANGES[exchange])({'enableRateLimit': True, 'timeout': 30000})
        try:
            # Identical request horizons to pinned PB8 procedures.py. A local
            # dataset's first day is NOT an authoritative listing timestamp.
            since = 1 if exchange == 'binance' else 1514764800000
            for coin, symbol in coins.items():
                if exchange == 'bitget':
                    candles = await bitget_first_candles(client, symbol)
                elif exchange == 'kucoin':
                    candles = await client.fetch_ohlcv(symbol, since=since, timeframe='1d',
                                                       limit=1, params={'to': client.milliseconds()})
                else:
                    candles = await client.fetch_ohlcv(symbol,
                        since=1609459200000 if exchange == 'hyperliquid' else since,
                        timeframe='1M' if exchange == 'okx' else '1w' if exchange == 'hyperliquid' else '1d')
                stamp = candles[0][0] if candles else None
                if (type(stamp) not in (int, float) or not math.isfinite(stamp)
                        or not 1262304000000 < stamp <= time.time() * 1000):
                    raise VastError('No authoritative first candle for ' + exchange + '/' + coin, 422)
                timestamps.setdefault(coin, {})[identifier] = int(stamp)
                symbols.setdefault(coin, {})[identifier] = symbol
        finally:
            await client.close()
    unified = {coin: min(values.values()) for coin, values in timestamps.items()}
    return {'version': CACHE_VERSION, 'files': dict(zip(CACHE_FILES, (unified, timestamps, symbols)))}


def stage_inception(connection):
    """Install complete first-timestamp caches before starting any optimizer process."""
    try:
        manifest = connection.store.read(connection.identifier, 'input/manifest.json')
        markets = required_markets(manifest, PROJECT / 'data/coindata')
        async def bounded_fetch():
            """Bound local preparation and cancel/close exchange clients on timeout."""
            return await asyncio.wait_for(fetch_inception(markets), timeout=180)
        snapshot = asyncio.run(bounded_fetch())
    except Exception as exc:
        human_log(SERVICE, 'First-candle metadata preparation failed; optimizer was not started',
                  level='ERROR', meta={'error_type': type(exc).__name__})
        if isinstance(exc, VastError):
            raise
        raise VastError('First-candle metadata could not be fetched on PBGui; optimizer was not started') from None
    payload = json.dumps(snapshot, allow_nan=False).encode()
    if len(payload) > 8 * 1024**2:
        raise VastError('Inception metadata exceeds transfer limit', 422)
    helper = (Path(__file__).parent / 'setup/vast_gpu_benchmark/inception_cache.py').read_text()
    checksum = hashlib.sha256(payload).hexdigest()
    with tempfile.TemporaryFile() as stream:
        stream.write(gzip.compress(payload))
        stream.seek(0)
        connection.command('/usr/local/bin/python -c ' + shlex.quote(helper)
                           + ' ' + shlex.quote(connection.remote_root) + ' ' + checksum,
                           stdin=stream, timeout=60)
    human_log(SERVICE, 'First-candle metadata installed for ' + str(len(snapshot['files'][CACHE_FILES[0]]))
              + ' coins; remote inception lookups are no longer needed', level='INFO')
