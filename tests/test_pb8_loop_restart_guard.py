"""Restart admission protects in-flight Loop decisions without touching real jobs."""
import asyncio
from types import SimpleNamespace

import pytest

import PBApiServer
from api import loop_optimizer_v8
from pb8_loop_controller import LoopController, LoopRestartPending


@pytest.fixture
def restart_owner(monkeypatch):
    """Isolate queued restart ownership and forbid real service scheduling."""
    for name, value in (("_server_restart_task", None), ("_api_restart_handoff_task", None),
                        ("_server_restart_request", {}), ("_api_restart_lease", None)):
        monkeypatch.setattr(PBApiServer, name, value)
    monkeypatch.setattr(PBApiServer, "_restart_current_api_systemd_unit",
                        lambda *_args: pytest.fail("Cannot restart production services"))


def test_inflight_request_blocks_restart_and_clears_after_response(tmp_path, monkeypatch, restart_owner):
    """A restart cannot cancel the request; completed and failed requests release the guard."""
    async def scenario():
        started, finish = asyncio.Event(), asyncio.Event()

        async def decide(*_args):
            started.set()
            await finish.wait()
            return {'decision': 'continue'}

        controller = LoopController(tmp_path, store=SimpleNamespace(knowledge=lambda _owner: {}), backend=SimpleNamespace(),
                                    ai=SimpleNamespace(decide=decide))
        monkeypatch.setattr(loop_optimizer_v8, '_controller', controller)
        monkeypatch.setattr(PBApiServer, '_api_restart_lease', None)
        loop_optimizer_v8.configure_restart_gate(PBApiServer._api_restart_pending)
        monkeypatch.setattr(PBApiServer, '_acquire_api_restart_leases', lambda: [object()])
        released = []
        monkeypatch.setattr(PBApiServer, '_release_api_restart_leases', lambda leases: released.extend(leases))
        task = asyncio.create_task(controller.decision({'round': 0, 'owner': 'a' * 32}, 'evaluate', {}))
        await started.wait()
        inspected = asyncio.Event()
        original = PBApiServer._restart_block_state

        async def inspect():
            result = await original()
            inspected.set()
            return result

        monkeypatch.setattr(PBApiServer, '_restart_block_state', inspect)
        assert (await PBApiServer.server_restart(SimpleNamespace()))['ok']
        await asyncio.wait_for(inspected.wait(), 2)
        assert 'AI Loop request' in PBApiServer._server_restart_request['reason']
        assert not task.done()
        assert not released and PBApiServer._api_restart_lease is not None
        finish.set()
        assert await task == {'decision': 'continue'}
        assert not controller.restart_block_reason()
        await PBApiServer._shutdown_requested_restart()
        assert released and PBApiServer._api_restart_lease is None
    asyncio.run(scenario())


def test_restart_reservation_closes_ai_admission_before_awaited_inspection(tmp_path, monkeypatch, restart_owner):
    """A decision cannot slip between blocker inspection and the actual restart handoff."""
    async def scenario():
        calls = []

        async def decide(*_args):
            calls.append('sent')
            return {}

        controller = LoopController(tmp_path, store=SimpleNamespace(knowledge=lambda _owner: {}), backend=SimpleNamespace(),
                                    ai=SimpleNamespace(decide=decide))
        monkeypatch.setattr(loop_optimizer_v8, '_controller', controller)
        monkeypatch.setattr(PBApiServer, '_api_restart_lease', None)
        loop_optimizer_v8.configure_restart_gate(PBApiServer._api_restart_pending)
        monkeypatch.setattr(PBApiServer, '_acquire_api_restart_leases', lambda: [object()])
        monkeypatch.setattr(PBApiServer, '_release_api_restart_leases', lambda _leases: None)

        inspected = asyncio.Event()

        async def inspecting():
            with pytest.raises(LoopRestartPending):
                await controller.decision({'round': 0, 'owner': 'a' * 32}, 'evaluate', {})
            inspected.set()
            return True, 'Other operation is busy'

        monkeypatch.setattr(PBApiServer, '_restart_block_state', inspecting)
        assert (await PBApiServer.server_restart(SimpleNamespace()))['ok']
        await asyncio.wait_for(inspected.wait(), 2)
        assert calls == []
        await PBApiServer._shutdown_requested_restart()
        assert PBApiServer._api_restart_lease is None
        assert await controller.decision({'round': 0, 'owner': 'a' * 32}, 'evaluate', {}) == {}
        assert calls == ['sent']
    asyncio.run(scenario())


@pytest.mark.parametrize('error', [RuntimeError('request failed'), asyncio.CancelledError()])
def test_failed_or_cancelled_decision_releases_restart_guard(tmp_path, error):
    """No leaked blocker remains after transport failure or forced process cancellation."""
    async def decide(*_args):
        raise error

    controller = LoopController(tmp_path, store=SimpleNamespace(knowledge=lambda _owner: {}), backend=SimpleNamespace(),
                                ai=SimpleNamespace(decide=decide))
    with pytest.raises(type(error)):
        asyncio.run(controller.decision({'round': 0, 'owner': 'a' * 32}, 'evaluate', {}))
    assert not controller.restart_block_reason()
