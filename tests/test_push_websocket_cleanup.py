"""Push WebSockets drain their tasks on transport disconnect and cancellation."""

import asyncio
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from starlette.websockets import WebSocketState
from api import auth


@pytest.mark.parametrize('ending', ['disconnect','sender_done','sender_error','cancel','logout'])
def test_push_socket_lifetime(monkeypatch, ending):
    """No auth registry or child task survives any endpoint exit path."""
    async def exercise():
        """Use real authentication registration with isolated cookies and transport."""
        queue = asyncio.Queue()
        entered = asyncio.Event()
        closed_sender = asyncio.Event()
        class Socket:
            """Minimal transport for exercising owned async task lifetimes."""
            cookies = {auth.SESSION_COOKIE_NAME:'test-token'}
            client_state = WebSocketState.CONNECTED
            async def accept(self):
                """Accept the test transport."""
            async def receive(self):
                """Wait for a mocked client event."""
                return await queue.get()
            async def close(self, **kwargs):
                """Record server closure without injecting a receive event."""
                self.client_state = WebSocketState.DISCONNECTED
        socket = Socket()
        monkeypatch.setattr(auth, 'require_same_origin', Mock())
        monkeypatch.setattr(auth, '_request_session_cookie_name', lambda ws: auth.SESSION_COOKIE_NAME)
        monkeypatch.setattr(auth, 'validate_token', lambda token: SimpleNamespace(token=token))
        monkeypatch.setattr(auth, '_websocket_sessions', {})
        monkeypatch.setattr(auth, '_websocket_watchdogs', {})
        @auth.authenticated_push_websocket
        async def endpoint(ws):
            """Simulate an idle push-only endpoint, including explicit failures."""
            entered.set()
            try:
                if ending == 'sender_done':
                    return
                if ending == 'sender_error':
                    raise ValueError('sender failed')
                await asyncio.Event().wait()
            finally:
                closed_sender.set()
        owner = asyncio.create_task(endpoint(socket))
        await entered.wait()
        if ending == 'disconnect':
            await queue.put({'type':'websocket.disconnect'})
        elif ending == 'cancel':
            owner.cancel()
        elif ending == 'logout':
            await auth.close_websocket_sessions('test-token')
        if ending == 'sender_error':
            with pytest.raises(ValueError, match='sender failed'):
                await asyncio.wait_for(owner, 1)
        elif ending == 'cancel':
            with pytest.raises(asyncio.CancelledError):
                await owner
        else:
            await asyncio.wait_for(owner, 1)
        assert closed_sender.is_set()
        assert not auth._websocket_sessions
        assert not auth._websocket_watchdogs
        assert not [t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()]
    asyncio.run(exercise())


@pytest.mark.parametrize('route', ['/ws/jobs','/ws/market-data','/ws/heatmap-watch','/ws/dashboard'])
def test_real_push_routes_disconnect_without_registry_leaks(monkeypatch, route):
    """Exercise the actual registered handlers without starting production services."""
    import ast
    from pathlib import Path
    from fastapi import FastAPI, WebSocket, WebSocketDisconnect
    from fastapi.testclient import TestClient
    import task_queue
    from api import heatmap

    monkeypatch.setattr(task_queue, 'list_jobs', lambda **kwargs: [])
    monkeypatch.setattr(heatmap, '_latest_mtime', lambda *args: 0)
    monkeypatch.setattr(auth, '_websocket_sessions', {})
    monkeypatch.setattr(auth, '_websocket_watchdogs', {})
    monkeypatch.setattr(auth, 'validate_token', lambda token: SimpleNamespace(token=token) if token == 'test' else None)
    app = FastAPI()
    namespace = dict(app=app, WebSocket=WebSocket, WebSocketDisconnect=WebSocketDisconnect,
                     authenticated_push_websocket=auth.authenticated_push_websocket,
                     asyncio=asyncio, active_connections=[], dashboard_ws_clients=set(),
                     _log=Mock(), SERVICE='Test')
    path = Path(__file__).resolve().parents[1] / 'PBApiServer.py'
    names = {'websocket_jobs','websocket_market_data','websocket_heatmap_watch','websocket_dashboard'}
    nodes = [n for n in ast.parse(path.read_text()).body if isinstance(n, ast.AsyncFunctionDef) and n.name in names]
    exec(compile(ast.Module(body=nodes,type_ignores=[]),str(path),'exec'),namespace)
    with TestClient(app) as client:
        for _ in range(3):
            with client.websocket_connect(route,headers={'Cookie':'pbgui_session=test','Origin':'http://testserver'}):
                pass
    assert namespace['active_connections'] == []
    assert namespace['dashboard_ws_clients'] == set()
    assert auth._websocket_sessions == {}
    assert auth._websocket_watchdogs == {}
