"""Provide fresh public market metadata to historical cloud optimizers."""

import asyncio
import gzip
import hashlib
import json
import shlex
import tempfile
import time

from logging_helpers import human_log
from vast_provider import VastError

from vast_exchanges import CCXT_EXCHANGES, SUPPORTED_EXCHANGES

SERVICE = "VastRunner"


async def fetch_markets(exchanges):
    """Fetch only public metadata and close every owned exchange client."""
    import ccxt.async_support as ccxt

    result = {}
    for exchange in exchanges:
        client = getattr(ccxt, CCXT_EXCHANGES[exchange])(
            {"enableRateLimit": True, "timeout": 30000})
        try:
            markets = await client.load_markets(True)
            if not isinstance(markets, dict) or not markets:
                raise VastError("The exchange returned empty public market metadata", 422)
            result[exchange] = {"markets": markets, "fetched_at": time.time()}
        finally:
            await client.close()
    return result


def stage_public_markets(connection):
    """Seed PB8's working-directory cache without modifying immutable input."""
    exchanges = connection.store.read(connection.identifier).get("exchanges")
    if (not isinstance(exchanges, list) or not exchanges
            or any(not isinstance(name, str) or name not in SUPPORTED_EXCHANGES for name in exchanges)):
        raise VastError("Missing or unsupported exchanges for public market metadata", 422)
    try:
        async def bounded_fetch():
            """Cancel and close exchange clients if local metadata preparation stalls."""
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
        human_log(SERVICE, "Could not refresh public market metadata before optimizer start", level="ERROR")
        if isinstance(exc, VastError):
            raise
        raise VastError("Public market metadata could not be refreshed on PBGui; optimizer was not started") from None
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
    human_log(SERVICE, "Fresh public market metadata installed for cloud optimizer", level="INFO")
