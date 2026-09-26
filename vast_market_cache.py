"""Provide fresh public market metadata to historical cloud optimizers."""

import asyncio
import gzip
import hashlib
import json
from pathlib import Path
import shlex
import tempfile
import time

from logging_helpers import human_log
from secure_files import read_regular_file_nofollow
from vast_provider import VastError

from vast_exchanges import SUPPORTED_EXCHANGES, quote_currency

SERVICE = "VastRunner"
MARKET_ROOT = Path(__file__).resolve().parent / "data/coindata"


def _load_local_markets(exchanges):
    """Read fresh PBGui market snapshots without contacting an exchange."""
    result = {}
    for exchange in exchanges:
        if exchange not in SUPPORTED_EXCHANGES:
            raise VastError("Unsupported exchange for local market metadata", 422)
        source = MARKET_ROOT / exchange / "ccxt_markets.json"
        try:
            before = source.stat()
            if before.st_size > 128 * 1024**2:
                raise VastError(f"Local market metadata is too large for {exchange}", 422)
            raw = read_regular_file_nofollow(source, MARKET_ROOT)
            after = source.stat()
        except VastError:
            raise
        except FileNotFoundError:
            raise VastError(
                f"Local market metadata is missing for {exchange}; wait for the Market Data update before renting",
                422,
            ) from None
        except (OSError, RuntimeError):
            raise VastError(f"Local market metadata cannot be read for {exchange}; check the Market Data cache", 422) from None
        if (before.st_ino, before.st_size, before.st_mtime_ns) != (after.st_ino, after.st_size, after.st_mtime_ns):
            raise VastError(f"Local market metadata changed while reading {exchange}; retry preparation", 409)
        if not 0 <= time.time() - before.st_mtime < 86400:
            raise VastError(
                f"Local market metadata for {exchange} is older than 24 hours; wait for the Market Data update",
                422,
            )
        try:
            source_markets = json.loads(raw)
        except (ValueError, TypeError):
            raise VastError(f"Local market metadata is invalid for {exchange}", 422) from None
        if not isinstance(source_markets, dict):
            raise VastError(f"Local market metadata is invalid for {exchange}", 422)
        markets = {
            symbol: market for symbol, market in source_markets.items()
            if isinstance(symbol, str) and isinstance(market, dict)
            and market.get("symbol") == symbol and market.get("swap") is True
            and market.get("linear") is True and market.get("quote") == quote_currency(exchange)
        }
        if not markets:
            raise VastError(f"Local market metadata has no linear perpetuals for {exchange}", 422)
        result[exchange] = {"markets": markets, "fetched_at": before.st_mtime}
    return result


async def fetch_markets(exchanges):
    """Load bounded local snapshots while retaining the worker's async timeout."""
    return await asyncio.to_thread(_load_local_markets, exchanges)


def stage_public_markets(connection):
    """Seed PB8's working-directory cache without modifying immutable input."""
    exchanges = connection.store.read(connection.identifier).get("exchanges")
    if (not isinstance(exchanges, list) or not exchanges
            or any(not isinstance(name, str) or name not in SUPPORTED_EXCHANGES for name in exchanges)):
        raise VastError("Missing or unsupported exchanges for public market metadata", 422)
    try:
        async def bounded_fetch():
            """Bound parsing of local market snapshots."""
            return await asyncio.wait_for(fetch_markets(list(dict.fromkeys(exchanges))), timeout=180)
        snapshots = asyncio.run(bounded_fetch())
        if not isinstance(snapshots, dict) or set(snapshots) != set(exchanges):
            raise VastError('Public market metadata is incomplete', 422)
        for item in snapshots.values():
            if (not isinstance(item, dict) or not isinstance(item.get('markets'), dict) or not item['markets']
                    or type(item.get('fetched_at')) not in (int, float)
                    or not 0 <= time.time() - item['fetched_at'] < 86400):
                raise VastError('Public market metadata is empty or expired', 422)
    except Exception as exc:
        exchange_names = ", ".join(dict.fromkeys(exchanges))
        error_type = type(exc).__name__
        human_log(
            SERVICE,
            "Could not prepare local public market metadata before optimizer start",
            level="ERROR",
            meta={"exchanges": list(dict.fromkeys(exchanges)), "error_type": error_type},
        )
        if isinstance(exc, VastError):
            raise
        raise VastError(
            "Local market metadata could not be prepared on PBGui for "
            f"{exchange_names} ({error_type}); optimizer was not started"
        ) from None
    payload = json.dumps(snapshots).encode()
    if len(payload) > 64 * 1024**2:
        raise VastError("Public market metadata exceeds the transfer limit", 422)
    checksum = hashlib.sha256(payload).hexdigest()
    script = (
        "import sys,gzip,json,hashlib,pathlib,os,time; "
        "raw=gzip.decompress(sys.stdin.buffer.read()); "
        "assert hashlib.sha256(raw).hexdigest()==" + repr(checksum) + "; "
        "data=json.loads(raw); root=pathlib.Path(" + repr(connection.remote_root + "/output/caches") + "); "
        "assert set(data).issubset(" + repr(SUPPORTED_EXCHANGES) + ")\n"
        "for exchange,item in data.items():\n"
        " stamp=item['fetched_at']; assert 0 <= time.time()-stamp < 86400\n"
        " target=root/exchange/'markets.json'; target.parent.mkdir(parents=True,exist_ok=True)\n"
        " temporary=target.with_suffix('.tmp'); temporary.write_text(json.dumps(item['markets']))\n"
        " os.chmod(temporary,0o600); os.utime(temporary,(stamp,stamp)); os.replace(temporary,target)\n"
    )
    with tempfile.TemporaryFile() as stream:
        stream.write(gzip.compress(payload))
        stream.seek(0)
        connection.command("/usr/local/bin/python -c " + shlex.quote(script), stdin=stream, timeout=120)
    human_log(SERVICE, "Local public market metadata installed for cloud optimizer", level="INFO")
