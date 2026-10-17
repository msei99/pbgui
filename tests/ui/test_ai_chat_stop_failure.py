"""The replacement setup model probe cleans up when its browser or API owner cancels."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import ai_setup


@pytest.mark.parametrize('disconnect',[True,False])
def test_setup_probe_cancellation_releases_runtime_registry(monkeypatch,disconnect):
    """Browser loss and shutdown cancellation leave no detached test or watcher running."""
    async def scenario():
        """Hold a synthetic provider until the owning request is cancelled."""
        started=asyncio.Event();finished=asyncio.Event()
        async def provider(*args,**kwargs):
            """Represent a provider transport with deterministic cancellation cleanup."""
            started.set()
            try:await asyncio.Event().wait()
            finally:finished.set()
        monkeypatch.setattr(ai_setup,'provider_reply',provider)
        chat=SimpleNamespace(accepting_turns=True,active_tasks={})
        request=SimpleNamespace(is_disconnected=AsyncMock(return_value=False))
        task=asyncio.create_task(ai_setup.test_model(chat,'a'*32,ai_setup.ModelTestRequest(provider='chatgpt',model='fixture-model'),request))
        await started.wait()
        if disconnect:request.is_disconnected.return_value=True
        else:next(iter(chat.active_tasks.values())).cancel()
        with pytest.raises(asyncio.CancelledError):await task
        assert finished.is_set() and not chat.active_tasks
        assert not [item for item in asyncio.all_tasks() if item is not asyncio.current_task() and not item.done()]
    asyncio.run(scenario())
