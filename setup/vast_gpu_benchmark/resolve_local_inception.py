"""Resolve cloud-job first candles with local PB8, isolated from its live caches."""

import asyncio
from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import sys
import tempfile


async def resolve(request):
    """Fetch only the missing exchange/coin pairs into a temporary PB8 cache."""
    pb8_dir = Path(request['pb8_dir']).resolve()
    if not (pb8_dir / 'src/procedures.py').is_file():
        raise RuntimeError('PB8 procedures are unavailable')
    markets = request['markets']
    if not isinstance(markets, dict):
        raise ValueError('Invalid market requirements')
    sys.path.insert(0, str(pb8_dir / 'src'))
    from procedures import get_first_timestamps_unified

    with tempfile.TemporaryDirectory(prefix='pbgui-vast-inception-') as directory:
        previous = Path.cwd()
        try:
            os.chdir(directory)
            with redirect_stdout(io.StringIO()):
                for exchange, coins in sorted(markets.items()):
                    identifier = 'binanceusdm' if exchange == 'binance' else exchange
                    await get_first_timestamps_unified(sorted(coins), exchange=identifier)
            root = Path(directory) / 'caches'
            if (root / 'first_ohlcv_timestamps_unified.version').read_text().strip() != '2':
                raise RuntimeError('PB8 resolver cache version is incompatible')
            specific = json.loads((root / 'first_ohlcv_timestamps_unified_exchange_specific.json').read_text())
            symbols = json.loads((root / 'first_ohlcv_timestamps_unified_exchange_specific_symbols.json').read_text())
            return {
                exchange: {
                    coin: {
                        'timestamp': specific.get(coin, {}).get('binanceusdm' if exchange == 'binance' else exchange),
                        'symbol': symbols.get(coin, {}).get('binanceusdm' if exchange == 'binance' else exchange),
                    }
                    for coin in coins
                }
                for exchange, coins in markets.items()
            }
        finally:
            os.chdir(previous)


def main():
    """Return only bounded machine-readable metadata to the PBGui parent."""
    request = json.load(sys.stdin)
    print(json.dumps(asyncio.run(resolve(request)), allow_nan=False))


if __name__ == '__main__':
    main()
