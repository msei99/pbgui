"""Resolve authoritative first-candle metadata locally for isolated PB8 workers."""

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
from pbgui_purefunc import pb8dir
from secure_files import read_regular_file_nofollow
from vast_jobs import PROJECT
from vast_provider import VastError

from vast_exchanges import SUPPORTED_EXCHANGES, quote_currency

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


def load_local_inception(markets):
    """Build a complete resolver-v2 snapshot from PB8's local public cache."""
    configured = pb8dir()
    if not configured:
        raise VastError('Local PB8 first-candle cache is unavailable; configure the PB8 directory', 422)
    root = Path(configured).expanduser().resolve() / 'caches'
    try:
        version = int(read_regular_file_nofollow(root / 'first_ohlcv_timestamps_unified.version', root).strip())
        specific = json.loads(read_regular_file_nofollow(root / CACHE_FILES[1], root))
        symbols = json.loads(read_regular_file_nofollow(root / CACHE_FILES[2], root))
    except (OSError, RuntimeError, ValueError, TypeError):
        raise VastError('Local PB8 first-candle cache is missing or invalid; prepare it before renting', 422) from None
    if version != CACHE_VERSION or not isinstance(specific, dict) or not isinstance(symbols, dict):
        raise VastError('Local PB8 first-candle cache version or structure is incompatible', 422)
    selected_specific, selected_symbols = {}, {}
    for exchange, coins in markets.items():
        identifier = 'binanceusdm' if exchange == 'binance' else exchange
        for coin, symbol in coins.items():
            timestamp = specific.get(coin, {}).get(identifier) if isinstance(specific.get(coin), dict) else None
            cached_symbol = symbols.get(coin, {}).get(identifier) if isinstance(symbols.get(coin), dict) else None
            if (type(timestamp) not in (int, float) or not math.isfinite(timestamp)
                    or not 1262304000000 < timestamp <= time.time() * 1000 or cached_symbol != symbol):
                raise VastError(
                    f'Local first-candle cache is missing or outdated for {exchange}/{coin}; prepare it before renting',
                    422,
                )
            selected_specific.setdefault(coin, {})[identifier] = timestamp
            selected_symbols.setdefault(coin, {})[identifier] = symbol
    if not selected_specific:
        raise VastError('No local first-candle metadata for the exported markets', 422)
    unified = {coin: min(values.values()) for coin, values in selected_specific.items()}
    return {'version': CACHE_VERSION, 'files': dict(zip(CACHE_FILES, (unified, selected_specific, selected_symbols)))}


def stage_inception(connection):
    """Install complete first-timestamp caches before starting any optimizer process."""
    try:
        manifest = connection.store.read(connection.identifier, 'input/manifest.json')
        markets = required_markets(manifest, PROJECT / 'data/coindata')
        snapshot = load_local_inception(markets)
    except Exception as exc:
        human_log(SERVICE, 'Local first-candle metadata preparation failed; optimizer was not started',
                  level='ERROR', meta={'error_type': type(exc).__name__})
        if isinstance(exc, VastError):
            raise
        raise VastError('Local first-candle metadata could not be prepared on PBGui; optimizer was not started') from None
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
    human_log(SERVICE, 'Local first-candle metadata installed for ' + str(len(snapshot['files'][CACHE_FILES[0]]))
              + ' coins; remote inception lookups are no longer needed', level='INFO')
