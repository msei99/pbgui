"""PB9 market routes reusing the common offline editor data services."""
import json

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from api.auth import SessionToken, require_auth
from api import editor_market_data as markets
from api.pb9 import _execute, _request_envelope
from pb9_market import get_market_identifiers, validate_selection

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
    exchanges = body['exchanges'] if operation == 'status' else [body['exchange']]
    identifiers = body['coins'] if operation == 'status' else []
    try:
        validate_selection(exchanges, identifiers)
    except (TypeError, ValueError):
        raise HTTPException(422, 'Invalid exchange or market selection') from None
    if operation == 'tags':
        return {'tags': markets.tags(body['exchange'])}
    from api.pb9 import _read_paths
    catalog = get_market_identifiers(_read_paths(), exchanges, identifiers, cancel_event)
    if operation == 'status':
        return catalog
    exchange = body['exchange']
    if operation == 'symbols':
        return catalog
    approved, ignored = markets.filter_symbols(exchange, body['market_cap'], body['vol_mcap'],
                                               body['only_cpt'], body['notices_ignore'], body['tags'])
    return markets.project_filtered_catalog(catalog, approved, ignored)


async def _get(request, operation, body):
    """Serialize server-validated query values for the request-owned worker."""
    return await _execute(request, 'market_' + operation, json.dumps(body, allow_nan=False).encode())


@router.get('/symbols')
async def symbols(request: Request, exchange: str = Query(...), session: SessionToken = Depends(require_auth)):
    """Use native coin names and exact identifiers only where collisions require them."""
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
