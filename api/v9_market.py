"""PB9 market routes reusing the common offline editor data services."""
import json

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from api.auth import SessionToken, require_auth
from api import editor_market_data as markets
from api.pb9 import _execute, _request_envelope
from pbgui_purefunc import PBGDIR

SERVICE = 'V9Instances'
router = APIRouter()


def _operation(operation, cancel_event, payload):
    """Read published mappings in the existing bounded PB9 request owner."""
    if cancel_event.is_set():
        from pb9_config import PB9ConfigError
        raise PB9ConfigError('helper_cancelled')
    keys = {'symbols': {'exchange'}, 'tags': {'exchange'},
            'status': {'exchanges', 'coins'},
            'filter': {'exchange', 'market_cap', 'vol_mcap', 'only_cpt', 'notices_ignore', 'tags'}}
    body = _request_envelope(payload, keys[operation])
    if operation == 'status':
        return markets.exact_market_catalog(PBGDIR, body['exchanges'], body['coins'])
    exchange = body['exchange']
    # Validate before any CoinData lookup or path construction.
    catalog = markets.exact_market_catalog(PBGDIR, [exchange])
    if operation == 'symbols':
        return catalog
    if operation == 'tags':
        return {'tags': markets.tags(exchange)}
    approved, ignored = markets.filter_symbols(exchange, body['market_cap'], body['vol_mcap'],
                                               body['only_cpt'], body['notices_ignore'], body['tags'])
    approved, ignored = set(approved), set(ignored)
    return {'approved': [r['config_id'] for r in catalog['catalog'] if r['coin'] in approved],
            'ignored': [r['config_id'] for r in catalog['catalog'] if r['coin'] in ignored],
            'unresolved': sorted((approved | ignored) - {r['coin'] for r in catalog['catalog']})}


async def _get(request, operation, body):
    """Serialize server-validated query values for the request-owned worker."""
    return await _execute(request, 'market_' + operation, json.dumps(body, allow_nan=False).encode())


@router.get('/symbols')
async def symbols(request: Request, exchange: str = Query(...), session: SessionToken = Depends(require_auth)):
    """Return exact venue-qualified linear swap identifiers from local mappings."""
    return await _get(request, 'symbols', {'exchange': exchange})


@router.get('/tags')
async def tags(request: Request, exchange: str = Query(...), session: SessionToken = Depends(require_auth)):
    """Return shared published Coin Data tags."""
    return await _get(request, 'tags', {'exchange': exchange})


@router.get('/coins/filter')
async def filter_coins(request: Request, exchange: str = Query(...), market_cap: int = Query(0),
                       vol_mcap: float = Query(10.0), only_cpt: bool = Query(False),
                       notices_ignore: bool = Query(False), tags: str = Query(''),
                       session: SessionToken = Depends(require_auth)):
    """Project shared coin filters back onto exact native market IDs."""
    return await _get(request, 'filter', dict(exchange=exchange, market_cap=market_cap,
                      vol_mcap=vol_mcap, only_cpt=only_cpt, notices_ignore=notices_ignore, tags=tags))


@router.post('/coins/status')
async def statuses(request: Request, session: SessionToken = Depends(require_auth)):
    """Classify bounded selections without external market requests."""
    body = bytearray()
    async for chunk in request.stream():
        if len(body) + len(chunk) > 384 * 1024:
            raise HTTPException(413, 'Market selection is too large')
        body.extend(chunk)
    return await _execute(request, 'market_status', bytes(body))
