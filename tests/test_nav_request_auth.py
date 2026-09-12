"""Navigation bridge authentication without starting API services or reading secrets."""

import ast
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from fastapi import Depends, FastAPI, HTTPException, Request, WebSocket
from fastapi.testclient import TestClient

from api import auth


@pytest.fixture
def bridge(monkeypatch):
    """Compile the actual route with the production authentication dependencies."""
    app = FastAPI()
    socket = AsyncMock()
    session = auth.SessionToken(token='valid', user_id='test', created_at=1, expires_at=9999999999)
    monkeypatch.setattr(auth, 'validate_token', lambda token: session if token == 'valid' else None)
    path = Path(__file__).resolve().parents[1] / 'PBApiServer.py'
    tree = ast.parse(path.read_text())
    node = next(n for n in tree.body if isinstance(n, ast.AsyncFunctionDef) and n.name == 'nav_request')
    namespace = dict(app=app, Request=Request, WebSocket=WebSocket, Depends=Depends,
                     SessionToken=auth.SessionToken, require_auth=auth.require_auth,
                     HTTPException=HTTPException, dashboard_ws_clients={socket})
    exec(compile(ast.Module(body=[node], type_ignores=[]),str(path),'exec'),namespace)
    with TestClient(app) as client:
        yield client, socket


@pytest.mark.parametrize('transport', ['cookie','bearer'])
@pytest.mark.parametrize('payload', [{'page':'dashboard'}, {'action':'select_dashboard','params':{'name':'test'}}])
def test_cookie_and_bearer_navigation(bridge, transport, payload):
    """Both supported credential transports deliver only the requested navigation data."""
    client, socket = bridge
    headers = {'Cookie':'pbgui_session=valid'} if transport == 'cookie' else {'Authorization':'Bearer valid'}
    response = client.post('/api/nav/request',headers=headers,json=payload)
    assert response.status_code == 200
    assert response.json() == {'ok':True,'notified':1}
    message = socket.send_json.call_args.args[0]
    assert message['type'] == ('dashboard_action' if 'action' in payload else 'nav_request')
    assert 'token' not in message


@pytest.mark.parametrize('headers,payload', [({}, {'page':'dashboard'}),
    ({}, {'page':'dashboard','token':'valid'}),
    ({'Cookie':'pbgui_session=expired'}, {'page':'dashboard'}),
    ({'Cookie':'pbgui_session=valid','Authorization':'Bearer expired'}, {'page':'dashboard'}),
    ({'Authorization':'Basic valid'}, {'page':'dashboard'})])
def test_invalid_auth_never_broadcasts(bridge, headers, payload):
    """Missing/expired credentials and legacy body tokens cannot navigate clients."""
    client, socket = bridge
    assert client.post('/api/nav/request',headers=headers,json=payload).status_code == 401
    socket.send_json.assert_not_called()


@pytest.mark.parametrize('payload', [[], None, {}, {'action':'invalid'}])
def test_invalid_payload_never_broadcasts(bridge, payload):
    """Authenticated malformed navigation requests remain ordinary client errors."""
    client, socket = bridge
    assert client.post('/api/nav/request',headers={'Cookie':'pbgui_session=valid'},json=payload).status_code == 400
    socket.send_json.assert_not_called()
