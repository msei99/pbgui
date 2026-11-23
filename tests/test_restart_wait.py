"""Direct restart acceptance and handoff tests with isolated service actions."""

import asyncio
import threading

import pytest
from fastapi import HTTPException

import PBApiServer as server


class Lease:
    """Record lifecycle ownership without touching actual service locks."""

    def __init__(self):
        self.releases = 0
        self.detaches = 0

    def release(self):
        """Release a fake lease idempotently like the real owner."""
        self.releases = 1

    def detach(self):
        """Record handoff to asynchronous shutdown."""
        self.detaches += 1


@pytest.fixture
def restart_state(monkeypatch):
    """Isolate every real service, runtime scan and handoff side effect."""
    leases = (Lease(), Lease(), Lease())
    for name, value in (("_api_restart_handoff_task", None), ("_api_restart_lease", None),
                        ("_sse_subscribers", [])):
        monkeypatch.setattr(server, name, value)
    monkeypatch.setattr(server, '_log', lambda *args, **kwargs: None)
    monkeypatch.setattr(server, '_restart_memory_block_reason', lambda: '')
    monkeypatch.setattr(server, '_acquire_api_restart_leases', lambda: leases)
    monkeypatch.setattr(server, '_restart_status_payload', lambda: {'restart_services': []})
    monkeypatch.setattr(server, '_SYSTEMD_RESTART_WATCHDOG_SECONDS', 3600)
    monkeypatch.setattr(server, '_prepare_direct_api_replacement',
                        lambda **kwargs: pytest.fail('Must not spawn an actual replacement'))
    return leases


def test_acceptance_waits_for_preflight_and_schedules_all_stale_services(monkeypatch, restart_state):
    """HTTP success follows validation and scheduling, never a deferred queue entry."""
    units = [item['unit'] for item in server._RUNTIME_SYSTEMD_SERVICES if item['service'] != 'VPSMonitor']
    scheduled = []
    monkeypatch.setattr(server, '_restart_status_payload', lambda: {
        'restart_services': [{'unit': unit, 'label': unit} for unit in units]})
    monkeypatch.setattr(server, '_restart_current_api_systemd_unit',
                        lambda selected: scheduled.append(selected) or True)

    async def scenario():
        entered, release = asyncio.Event(), asyncio.Event()

        async def blockers():
            """Hold only synthetic preflight while the event loop remains usable."""
            entered.set()
            await release.wait()
            return False, ''

        monkeypatch.setattr(server, '_restart_block_state', blockers)
        request = asyncio.create_task(server.server_restart(session=object()))
        await asyncio.wait_for(entered.wait(), 2)
        assert not request.done()
        assert not scheduled
        assert server._api_restart_pending()
        release.set()
        result = await asyncio.wait_for(request, 2)
        assert result['ok'] is True
        assert 'restart_request' not in result
        assert scheduled == [units]
        assert server._api_restart_handoff_task is not None
        await server._shutdown_requested_restart()
        assert not server._api_restart_pending()
        assert [lease.releases for lease in restart_state] == [1, 1, 1]

    asyncio.run(scenario())


def test_real_blocker_rejects_without_starting_services(monkeypatch, restart_state):
    """A protected operation returns its reason and releases every reservation."""
    monkeypatch.setattr(server, '_restart_block_state',
                        lambda: asyncio.sleep(0, result=(True, 'PB9 inspection in progress')))
    monkeypatch.setattr(server, '_restart_current_api_systemd_unit',
                        lambda *args: pytest.fail('Blocked restart must not schedule services'))
    with pytest.raises(HTTPException) as caught:
        asyncio.run(server.server_restart(session=object()))
    assert caught.value.status_code == 409
    assert 'PB9 inspection' in caught.value.detail
    assert server._api_restart_lease is None
    assert [lease.releases for lease in restart_state] == [1, 1, 1]


def test_cancelled_lock_acquisition_releases_late_leases(monkeypatch, restart_state):
    """An interrupted request cannot leave a late worker holding lifecycle locks."""
    entered, release = threading.Event(), threading.Event()

    def acquire():
        """Delay an isolated acquisition until the request has been cancelled."""
        entered.set()
        assert release.wait(3)
        return restart_state

    monkeypatch.setattr(server, '_acquire_api_restart_leases', acquire)

    async def scenario():
        task = asyncio.create_task(server.server_restart(session=object()))
        assert await asyncio.to_thread(entered.wait, 2)
        task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert [lease.releases for lease in restart_state] == [1, 1, 1]

    asyncio.run(scenario())


def test_failed_scheduling_is_not_acknowledged_as_success(monkeypatch, restart_state):
    """A systemd scheduling failure is returned to the original GUI request."""
    monkeypatch.setattr(server, '_restart_block_state', lambda: asyncio.sleep(0, result=(False, '')))

    def fail(*args):
        """Model a failed schedule without controlling any actual unit."""
        raise RuntimeError('scheduling failed')

    monkeypatch.setattr(server, '_restart_current_api_systemd_unit', fail)
    with pytest.raises(HTTPException) as caught:
        asyncio.run(server.server_restart(session=object()))
    assert caught.value.status_code == 500
    assert 'scheduling failed' in caught.value.detail
    assert server._api_restart_lease is None


@pytest.mark.parametrize('step', ['status', 'schedule', 'replacement'])
def test_cancellation_during_preparation_drains_worker(monkeypatch, restart_state, step):
    """Late worker results cannot leak a reservation or acknowledged child."""
    entered, release = threading.Event(), threading.Event()
    replacement = object()
    cleaned = []

    def delay(value):
        """Hold an isolated worker until the request has been cancelled twice."""
        entered.set()
        assert release.wait(3)
        return value

    monkeypatch.setattr(server, '_restart_block_state', lambda: asyncio.sleep(0, result=(False, '')))
    monkeypatch.setattr(server, '_restart_current_api_systemd_unit',
                        lambda units: delay(True) if step == 'schedule' else False)
    monkeypatch.setattr(server, '_prepare_direct_api_replacement', lambda **kwargs: delay(replacement))
    monkeypatch.setattr(server, 'cancel_api_replacement', lambda child: cleaned.append(child))
    if step == 'status':
        monkeypatch.setattr(server, '_restart_status_payload', lambda: delay({'restart_services': []}))

    async def scenario():
        request = asyncio.create_task(server.server_restart(session=object()))
        assert await asyncio.to_thread(entered.wait, 2)
        request.cancel()
        await asyncio.sleep(0)
        request.cancel()
        release.set()
        if step == 'schedule':
            assert (await request)['ok']
            assert server._api_restart_pending()
            await server._shutdown_requested_restart()
        else:
            with pytest.raises(asyncio.CancelledError):
                await request
        assert not server._api_restart_pending()
        assert [lease.releases for lease in restart_state] == [1, 1, 1]
        assert cleaned == ([replacement] if step == 'replacement' else [])

    asyncio.run(scenario())


def test_cancelled_direct_handoff_cleans_acknowledged_child(monkeypatch, restart_state):
    """Shutdown before SIGTERM removes the prepared replacement and reservations."""
    child = object()
    cleaned = []
    monkeypatch.setattr(server, '_restart_block_state', lambda: asyncio.sleep(0, result=(False, '')))
    monkeypatch.setattr(server, '_restart_current_api_systemd_unit', lambda units: False)
    monkeypatch.setattr(server, '_prepare_direct_api_replacement', lambda **kwargs: child)
    monkeypatch.setattr(server, 'cancel_api_replacement', lambda item: cleaned.append(item))

    async def scenario():
        assert (await server.server_restart(session=object()))['ok']
        await server._shutdown_requested_restart()
        assert cleaned == [child]
        assert not server._api_restart_pending()
        assert [lease.releases for lease in restart_state] == [1, 1, 1]

    asyncio.run(scenario())



def test_new_memory_owner_before_scheduling_blocks_restart(monkeypatch, restart_state):
    """A new operation appearing during status inspection must protect its work."""
    monkeypatch.setattr(server, '_restart_block_state', lambda: asyncio.sleep(0, result=(False, '')))
    monkeypatch.setattr(server, '_restart_memory_block_reason', lambda: 'new protected operation')
    monkeypatch.setattr(server, '_restart_current_api_systemd_unit',
                        lambda units: pytest.fail('Late protected work must block scheduling'))
    with pytest.raises(HTTPException) as caught:
        asyncio.run(server.server_restart(session=object()))
    assert caught.value.status_code == 409
    assert [lease.releases for lease in restart_state] == [1, 1, 1]


@pytest.mark.parametrize('payload', [None, [], {}, {'active': 'false'}])
def test_malformed_deploy_status_cannot_authorize_restart(monkeypatch, payload):
    """Invalid deployment observations fail closed instead of permitting restart."""
    from types import SimpleNamespace
    from api import vps_manager

    monkeypatch.setattr(server, '_restart_memory_block_reason', lambda: '')
    for name in ('profit_sweep_restart_block_reason', 'ai_restart_block_reason',
                 'credential_migration_restart_block_reason'):
        monkeypatch.setattr(server, name, lambda *args: '')
    monkeypatch.setattr(vps_manager, 'get_service_instance', lambda: SimpleNamespace(
        active_vps_deploy_summary=lambda: payload))
    monkeypatch.setattr(server, '_log', lambda *args, **kwargs: None)
    blocked, reason = asyncio.run(server._restart_block_state())
    assert blocked and 'Could not verify' in reason


@pytest.mark.parametrize('mode', ['signal_failure', 'watchdog'])
def test_failed_handoff_reopens_read_admission(monkeypatch, restart_state, mode):
    """A live API remains usable when accepted restart does not actually stop it."""
    from types import SimpleNamespace
    from api.read_stream_lifecycle import ReadStreamOwner
    owner = ReadStreamOwner()
    monkeypatch.setattr(server, '_read_stream_owner', owner)
    monkeypatch.setattr(server, '_restart_block_state', lambda: asyncio.sleep(0, result=(False, '')))
    monkeypatch.setattr(server, '_restart_current_api_systemd_unit', lambda units: mode == 'watchdog')
    monkeypatch.setattr(server, '_SYSTEMD_RESTART_WATCHDOG_SECONDS', 0)
    monkeypatch.setattr(server, '_prepare_direct_api_replacement',
                        lambda **kwargs: SimpleNamespace(old_identity=object()))
    cleaned = []
    monkeypatch.setattr(server, 'cancel_api_replacement', lambda child: cleaned.append(child))

    def fail_signal(*args):
        """Reject a synthetic signal without controlling a real process."""
        raise RuntimeError('synthetic signal failure')

    monkeypatch.setattr(server, 'signal_exact_process', fail_signal)

    async def scenario():
        owner.start()
        await server.server_restart(session=object())
        await asyncio.wait_for(server._api_restart_handoff_task, 2)
        assert not owner.closing
        assert not server._api_restart_pending()
        assert bool(cleaned) is (mode == 'signal_failure')
    asyncio.run(scenario())
