"""Server-owned deferred restart tests without touching services or runtime data."""

import asyncio
import threading

import pytest

import PBApiServer as server


class Lease:
    """Record reservation ownership without acquiring production locks."""

    def __init__(self):
        self.releases = 0
        self.detaches = 0

    def release(self):
        """Record one release."""
        self.releases += 1

    def detach(self):
        """Record handoff to shutdown."""
        self.detaches += 1


@pytest.fixture
def restart_state(monkeypatch):
    """Keep all restart state and process handoffs isolated from real services."""
    leases = (Lease(), Lease(), Lease())
    for name, value in (("_server_restart_task", None), ("_api_restart_handoff_task", None),
                        ("_api_restart_lease", None), ("_server_restart_request", {}),
                        ("_sse_subscribers", [])):
        monkeypatch.setattr(server, name, value)
    monkeypatch.setattr(server, "_log", lambda *args, **kwargs: None)
    monkeypatch.setattr(server, "_acquire_api_restart_leases", lambda: leases)
    monkeypatch.setattr(server, "_restart_status_payload", lambda: {"restart_services": []})
    monkeypatch.setattr(server, "_SYSTEMD_RESTART_WATCHDOG_SECONDS", 3600)
    monkeypatch.setattr(server, "_prepare_direct_api_replacement",
                        lambda **kwargs: pytest.fail("Must not spawn a replacement"))
    return leases


def test_one_request_waits_then_restarts_without_resubmission(monkeypatch, restart_state):
    """A completed HTTP request leaves one owner waiting, closing new admission."""
    scheduled = []
    monkeypatch.setattr(server, "_restart_current_api_systemd_unit",
                        lambda units: scheduled.append(units) or True)

    async def scenario():
        clear = False
        inspected = asyncio.Event()

        async def blockers():
            inspected.set()
            return not clear, "PB9 inspection is in progress" if not clear else ""

        monkeypatch.setattr(server, "_restart_block_state", blockers)
        first = await server.server_restart(session=object())
        owner = server._server_restart_task
        assert first["ok"] and first["restart_request"]["state"] == "waiting"
        assert server._api_restart_pending()
        await inspected.wait()
        duplicate = await server.server_restart(session=object())
        assert duplicate["ok"] and server._server_restart_task is owner
        assert server._server_restart_request["reason"] == "PB9 inspection is in progress"
        assert not scheduled
        assert server._api_restart_lease is restart_state[-1]
        clear = True
        await asyncio.wait_for(asyncio.shield(owner), 2)
        assert scheduled == [[]]
        assert server._server_restart_request["state"] == "restarting"
        assert [lease.detaches for lease in restart_state] == [1, 1, 0]
        await server._shutdown_requested_restart()

    asyncio.run(scenario())


def test_shutdown_releases_waiting_request_and_does_not_restart(monkeypatch, restart_state):
    """Shutdown deterministically drains preparation and releases every lease."""
    async def scenario():
        inspected = asyncio.Event()

        async def blockers():
            inspected.set()
            return True, "busy"

        monkeypatch.setattr(server, "_restart_block_state", blockers)
        await server.server_restart(session=object())
        await inspected.wait()
        await server._shutdown_requested_restart()
        assert not server._api_restart_pending()
        assert [lease.releases for lease in restart_state] == [1, 1, 1]

    asyncio.run(scenario())


def test_shutdown_during_lock_acquisition_releases_late_leases(monkeypatch, restart_state):
    """A cancelled worker acquisition cannot leak locks after shutdown."""
    started, finish = threading.Event(), threading.Event()

    def acquire():
        started.set()
        assert finish.wait(3)
        return restart_state

    monkeypatch.setattr(server, "_acquire_api_restart_leases", acquire)

    async def scenario():
        await server.server_restart(session=object())
        assert await asyncio.to_thread(started.wait, 2)
        shutdown = asyncio.create_task(server._shutdown_requested_restart())
        await asyncio.sleep(0)
        finish.set()
        await shutdown
        assert [lease.releases for lease in restart_state] == [1, 1, 1]

    asyncio.run(scenario())


def test_failed_handoff_is_reported_and_can_be_requested_again(monkeypatch, restart_state):
    """Actual scheduling errors stay visible instead of claiming completion."""
    monkeypatch.setattr(server, "_restart_block_state", lambda: asyncio.sleep(0, result=(False, "")))

    def fail(_units):
        raise RuntimeError("schedule failed")

    monkeypatch.setattr(server, "_restart_current_api_systemd_unit", fail)

    async def scenario():
        await server.server_restart(session=object())
        previous = server._server_restart_task
        await previous
        assert server._server_restart_request["state"] == "failed"
        assert "schedule failed" in server._server_restart_request["reason"]
        assert not server._api_restart_pending()
        assert [lease.releases for lease in restart_state] == [1, 1, 1]
        await server.server_restart(session=object())
        assert server._server_restart_task is not previous
        await server._server_restart_task
        await server._shutdown_requested_restart()

    asyncio.run(scenario())


def test_busy_lifecycle_reservation_is_retried_without_another_request(monkeypatch, restart_state):
    """An active update finishes before a reserved restart is allowed to proceed."""
    acquisitions = []

    def acquire():
        acquisitions.append(True)
        if len(acquisitions) == 1:
            raise server.MasterUpdateBusyError("Update is still running")
        return restart_state

    monkeypatch.setattr(server, "_acquire_api_restart_leases", acquire)
    monkeypatch.setattr(server, "_restart_block_state", lambda: asyncio.sleep(0, result=(False, "")))
    monkeypatch.setattr(server, "_restart_current_api_systemd_unit", lambda units: True)

    async def scenario():
        await server.server_restart(session=object())
        owner = server._server_restart_task
        await asyncio.wait_for(asyncio.shield(owner), 2)
        assert len(acquisitions) == 2
        assert server._server_restart_request["state"] == "restarting"
        await server._shutdown_requested_restart()

    asyncio.run(scenario())
