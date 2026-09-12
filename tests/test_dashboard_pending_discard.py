"""Discarding a dashboard draft is scoped, authenticated and idempotent."""

import asyncio

from fastapi import FastAPI
import httpx

from api import dashboard


def test_discard_pending_route(monkeypatch):
    """Delete only the selected draft and preserve unrelated pending layouts."""
    monkeypatch.setattr(dashboard, '_pending_full_configs', {'alice': {'rows':2}, 'bob':{'rows':1}})
    app=FastAPI()
    app.include_router(dashboard.router, prefix='/api/dashboard')
    app.dependency_overrides[dashboard.require_auth]=lambda: object()
    async def exercise():
        """Invoke the ASGI route without production startup or network sockets."""
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as client:
            for _ in range(2):
                response=await client.delete('/api/dashboard/pending_full',params={'name':'alice'})
                assert response.status_code==200
            response=await client.get('/api/dashboard/pending_full',params={'name':'alice'})
            assert response.json()['found'] is False
    asyncio.run(exercise())
    assert dashboard._pending_full_configs=={'bob':{'rows':1}}
