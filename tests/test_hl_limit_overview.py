"""Offline checks for VPS-owned Hyperliquid limit sampling and key reads."""

import asyncio
import json
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

import monitor_agent
from api import vps, vps_manager


ADDRESS = "0x" + "a" * 40


class FakeUsers:
    """Resolve saved accounts without touching runtime credentials."""

    def find_user(self, name):
        """Return one saved Hyperliquid user for both running bot names."""
        if name not in {"hl_one", "hl_two"}:
            return None
        return SimpleNamespace(exchange="hyperliquid", wallet_address=ADDRESS)


def test_vps_agent_samples_only_running_wallet_once(monkeypatch):
    """Two local bots sharing a wallet generate one Hyperliquid info request."""
    import User
    import httpx

    snapshot = {"generated_at": monitor_agent.time.time(),
                "v7": [{"name": "bot_a", "user": "hl_one", "running": True}],
                "v8": [{"name": "bot_b", "user": "hl_two", "running": True},
                       {"name": "stopped", "user": "hl_one", "running": False}]}
    monkeypatch.setattr(User, "Users", FakeUsers)
    monkeypatch.setattr(monitor_agent, "_read_json", lambda *args: snapshot)
    monkeypatch.setattr(monitor_agent, "_HL_RATE_LIMITS_BY_USER", {})
    calls = []

    class Response:
        """Provide a valid exchange info response."""

        def raise_for_status(self):
            """Accept the fake response."""

        def json(self):
            """Return counters in the official field names."""
            return {"nRequestsUsed": 80, "nRequestsCap": 100}

    def post(url, *, json, timeout):
        """Capture the one address read."""
        calls.append((url, json, timeout))
        return Response()

    monkeypatch.setattr(httpx, "post", post)
    monitor_agent._run_hl_rate_limits()
    assert len(calls) == 1
    assert calls[0][1] == {"type": "userRateLimit", "user": ADDRESS}
    assert monitor_agent._HL_RATE_LIMITS_BY_USER["hl_one"]["used"] == 80
    assert monitor_agent._HL_RATE_LIMITS_BY_USER["hl_two"]["cap"] == 100


def test_vps_agent_does_not_poll_without_running_bot(monkeypatch):
    """An unused account generates no background Hyperliquid request."""
    import httpx

    monkeypatch.setattr(monitor_agent, "_read_json", lambda *args: {"generated_at": monitor_agent.time.time(), "v7": [], "v8": []})
    monkeypatch.setattr(monitor_agent, "_HL_RATE_LIMITS_BY_USER", {"old": {"used": 1}})
    monkeypatch.setattr(httpx, "post", lambda *args, **kwargs: pytest.fail("Unexpected exchange request"))
    monitor_agent._run_hl_rate_limits()
    assert monitor_agent._HL_RATE_LIMITS_BY_USER == {}


def test_vps_agent_ignores_stale_bot_snapshot(monkeypatch):
    """A stopped collector cannot keep generating exchange reads from old bot data."""
    import httpx

    snapshot = {"generated_at": monitor_agent.time.time() - 200,
                "v8": [{"name": "old", "user": "hl_one", "running": True}]}
    monkeypatch.setattr(monitor_agent, "_read_json", lambda *args: snapshot)
    monkeypatch.setattr(monitor_agent, "_HL_RATE_LIMITS_BY_USER", {})
    monkeypatch.setattr(httpx, "post", lambda *args, **kwargs: pytest.fail("Unexpected exchange request"))
    monitor_agent._run_hl_rate_limits()
    assert monitor_agent._HL_RATE_LIMITS_BY_USER == {}


def test_master_imports_vps_sample_without_exchange_request(monkeypatch):
    """The master records one VPS sample and leaves an idle wallet unsampled."""
    idle = "0x" + "b" * 40
    monkeypatch.setattr(vps_manager, "_configured_hl_wallets", lambda: {ADDRESS: ["hl_one"], idle: ["unused"]})
    monkeypatch.setattr(vps_manager, "_hl_rate_limit_accounts", {})
    now = int(vps_manager.time.time())
    monkeypatch.setattr(vps, "get_monitor_state_snapshot", lambda: {
        "v7_instances": {},
        "v8_instances": {"vps-a": [{"name": "bot_a", "user": "hl_one", "running": True,
                                  "hl_rate_limit": {"used": 80, "cap": 100, "sampled_at": now}}]},
    })
    saved = []
    monkeypatch.setattr(vps_manager, "_append_hl_rate_limit_sample", lambda *args: saved.append(args))

    class StopCycle(Exception):
        """Stop the perpetual importer after its first pass."""

    async def stop_sleep(seconds):
        """End the importer without waiting."""
        raise StopCycle

    monkeypatch.setattr(vps_manager.asyncio, "sleep", stop_sleep)
    with pytest.raises(StopCycle):
        asyncio.run(vps_manager._poll_hl_rate_limits())
    assert saved == [(ADDRESS, now, 80, 100)]
    assert vps_manager._hl_rate_limit_accounts[ADDRESS]["bots"] == [
        {"name": "bot_a", "host": "vps-a", "pb_version": "8"}]
    assert vps_manager._hl_rate_limit_accounts[idle]["state"] == "idle"
    assert "used" not in vps_manager._hl_rate_limit_accounts[idle]


def test_newer_key_read_survives_older_vps_sample(monkeypatch):
    """A delayed VPS poll cannot replace a newer one-time account reading."""
    monkeypatch.setattr(vps_manager, "_configured_hl_wallets", lambda: {ADDRESS: ["hl_one"]})
    now = int(vps_manager.time.time())
    monkeypatch.setattr(vps_manager, "_hl_rate_limit_accounts", {
        ADDRESS: {"users": ["hl_one"], "used": 120, "cap": 100,
                  "sampled_at": now, "source": "on_demand", "state": "on_demand"}})
    monkeypatch.setattr(vps, "get_monitor_state_snapshot", lambda: {
        "v7_instances": {}, "v8_instances": {"vps-a": [
            {"name": "bot_a", "user": "hl_one", "running": True,
             "hl_rate_limit": {"used": 80, "cap": 100, "sampled_at": now - 120}}]}})
    monkeypatch.setattr(vps_manager, "_append_hl_rate_limit_sample", lambda *args: None)

    class StopCycle(Exception):
        """Stop after one import cycle."""

    async def stop_sleep(seconds):
        """End the importer without waiting."""
        raise StopCycle

    monkeypatch.setattr(vps_manager.asyncio, "sleep", stop_sleep)
    with pytest.raises(StopCycle):
        asyncio.run(vps_manager._poll_hl_rate_limits())
    assert vps_manager._hl_rate_limit_accounts[ADDRESS]["used"] == 120
    assert vps_manager._hl_rate_limit_accounts[ADDRESS]["bots"][0]["host"] == "vps-a"


def test_open_key_reads_once_without_creating_history(monkeypatch):
    """The saved wallet uses one info read and no background history for idle accounts."""
    monkeypatch.setattr(vps_manager, "_configured_hl_wallets", lambda: {ADDRESS: ["hl_one"]})
    monkeypatch.setattr(vps_manager, "_hl_rate_limit_accounts", {})
    reads = []
    monkeypatch.setattr(vps_manager, "_read_hl_rate_limit_now", lambda address: (reads.append(address) or (120, 100)))
    monkeypatch.setattr(vps_manager, "_append_hl_rate_limit_sample", lambda *args: pytest.fail("Idle account history written"))
    payload = json.loads(vps_manager.get_hl_user_rate_limit_live("hl_one", session=object()).body)
    assert reads == [ADDRESS]
    assert payload["remaining"] == 0
    assert vps_manager._hl_rate_limit_accounts[ADDRESS]["state"] == "on_demand"
    with pytest.raises(HTTPException) as exc:
        vps_manager.get_hl_user_rate_limit_live("missing", session=object())
    assert exc.value.status_code == 404


def test_vps_snapshot_carries_only_public_counters(monkeypatch):
    """The existing telemetry channel carries counters only for running bots."""
    payload = {"v7": [{"name": "bot_a", "user": "hl_one", "running": True}],
               "v8": [{"name": "idle", "user": "hl_one", "running": False}], "cache": {}}
    monkeypatch.setattr(monitor_agent, "_read_json", lambda *args: {})
    monkeypatch.setattr(monitor_agent, "_embedded_monitor_script", lambda name: "unused")
    monkeypatch.setattr(monitor_agent, "_run_shell_script", lambda *args, **kwargs: payload)
    monkeypatch.setattr(monitor_agent, "_HL_RATE_LIMITS_BY_USER", {
        "hl_one": {"used": 80, "cap": 100, "sampled_at": 12345}})
    written = []
    monkeypatch.setattr(monitor_agent, "_atomic_write_json", lambda path, data: written.append((path.name, data)))
    monitor_agent._run_instance_snapshot()
    snapshot = next(data for name, data in written if name == "instance_snapshot.json")
    assert snapshot["v7"][0]["hl_rate_limit"]["used"] == 80
    assert "hl_rate_limit" not in snapshot["v8"][0]
    assert ADDRESS not in json.dumps(snapshot)


def test_overview_page_is_registered_and_renders(monkeypatch):
    """The authenticated Information page renders a complete API URL."""
    from starlette.requests import Request

    scope = {"type": "http", "scheme": "https", "server": ("pbgui.test", 443),
             "path": "/api/vps-manager/hyperliquid-limits/main_page", "root_path": "/team/pbgui", "headers": []}
    request = Request(scope)
    result = vps_manager.get_hl_limits_page(request, session=object())
    html = result.body.decode()
    assert 'current:\'info_hl_limits\'' in html
    assert '"/team/pbgui/api/vps-manager"' in html
    assert '/team/pbgui/app/pbgui_nav.js' in html
    assert '%%API_BASE%%' not in html
