"""Execute unchanged pinned PB8 decisions with only infrastructure replaced."""

import ast
import asyncio
import datetime
import hashlib
import logging
import os
from pathlib import Path
from types import SimpleNamespace

import dateutil.parser
import numpy as np
import pandas as pd

from vast_jobs import REVISION

ROOT = Path(__file__).parent / 'reference/pb8_ohlcv'
PIN = 'b9d6692ea41266ecb4ab81d40bb5e5baef29b50a'
HASHES = {
    'source_loader.py.txt': '4637a47a6547815d0128bddd97ea892b1390bf28964d4c2d7fc85f5b25a819f7',
    'ohlcv_utils.py.txt': 'fac1bb529c6bf88a3447c2c30fb8ef8a83c2dc175de21f5736d11bced6430bb6',
    'dates.py.txt': '65d07e0b8608fbc4e04d23c65a1ca8c7bb6baa1b0164135f5d5913508e081b15',
    'days.py.txt': 'a3ff2295a2a246c5eb84c1c4281ce80191db79fc8a631032ea0d3a6fe04980e9',
    'dataset_windows.py.txt': '0cd84ae4e2732e9dc3e77c44bfd26272fd9de15e6c205b3ba9f72e4f83633642',
    'preparation.py.txt': 'ce1079d42369779a478b7e9d9c57051bebdc82198c4cf57fe5da2f2ba616738a',
    'coin_window.py.txt': '76da7f43a298c268441c20937db86564cfe6fab64ae796b9671d7f2f94ce250d',
    'positive_timestamp.py.txt': 'cf24cdda2f31af0ad3df290651d306882bcb91f887cc7c81e008e01e8db4b3d7',
    'direct_coin_window.py.txt': '54709226f9a9ad10b20fd0d8159ea11e12077d437ae5a9da87105a0606c42558',
}


class OfflineDataError(ValueError):
    """The original offline exception identity, without runtime imports."""


def namespace():
    """Load source-verified original functions; omit external module imports only."""
    assert REVISION == PIN, 'Update the parity reference when the worker pin changes'
    env = dict(np=np, pd=pd, os=os, Path=Path, logging=logging, datetime=datetime,
               dateutil=dateutil, SimpleNamespace=SimpleNamespace, OfflineDataError=OfflineDataError,
               simulation_data_scope=simulation_data_scope)
    for filename in HASHES:
        raw = (ROOT / filename).read_bytes()
        assert hashlib.sha256(raw).hexdigest() == HASHES[filename]
        source = raw.decode()
        if filename == 'source_loader.py.txt':
            source = 'class HLCVManager:\n' + source
        tree = ast.parse(source)
        tree.body = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))]
        exec(compile('from __future__ import annotations\n' + ast.unparse(tree), filename, 'exec'), env)
    return env


def source_manager(root, start, end, tolerance, listing, coin='BTC'):
    """Run the original reader and all original offline prefix/end/gap decisions.

    The real reader may rewrite duplicate NPY rows. Call only on a disposable
    copy, never the production validator's source. Gap filling happens after
    admission and is replaced by identity; no acceptance decision is mocked.
    """
    env = namespace()
    env.update(is_offline=lambda: True, to_standard_exchange_name=lambda ex: ex,
               record_range=lambda *a: None)

    async def first_timestamps(coins, exchange=None):
        """Supply the identical synthetic authoritative listing evidence."""
        return {coin: listing}

    env['get_first_timestamps_unified'] = first_timestamps
    manager = env['HLCVManager'].__new__(env['HLCVManager'])
    manager.__dict__.update(ohlcv_source_dir=str(root), exchange='bybit', markets={'present': True},
                            start_ts=start, end_ts=end, gap_tolerance_ohlcvs_minutes=tolerance,
                            tradfi_for_stock_perps=True, verbose=False)
    manager.has_coin = lambda c: True
    manager.get_symbol = lambda c: c + '/USDT:USDT'
    manager.cm = SimpleNamespace(standardize_gaps=lambda src, **kw: src)
    manager.load_cc = lambda: None
    return manager


def accepts(root, start, end, tolerance, listing, coin='BTC'):
    """Run all unchanged source-loader admission decisions on disposable files."""
    manager = source_manager(root, start, end, tolerance, listing, coin)
    try:
        return not asyncio.run(manager.get_ohlcvs(coin, source_dir_only=True)).empty
    except OfflineDataError:
        return False


# The decorator has ownership effects, but does not decide data admissibility.
def simulation_data_scope(function):
    """Keep the original preparation function executable without tracing I/O."""
    return function
