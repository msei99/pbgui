"""Offline load reduction, retry, pacing and shutdown tests for overview balances."""

from types import SimpleNamespace

import pytest
import requests

import profit_sweep_overview_balances as balances
from profit_sweep_overview import TargetReadCache

ADDRESS = "0x" + "1" * 40
LEADER = "0x" + "2" * 40


def test_normal_overview_requests_only_three_balance_inputs():
    """Display refreshes must not fetch history, orders, role or global spot metadata."""
    calls = []

    def read(kind, **kwargs):
        """Supply only the three permitted synthetic balance inputs."""
        calls.append(kind)
        return {"userAbstraction": "standard", "clearinghouseState": {"marginSummary": {"accountValue": "108.55"}, "withdrawable": "100"},
                "spotClearinghouseState": {"balances": [{"coin": "USDC", "total": "31.49", "hold": "0"}]}}[kind]

    snapshot = balances.collect_hyperliquid_balances(SimpleNamespace(wallet_address=ADDRESS, is_vault=False), {}, SimpleNamespace(read=read), TargetReadCache())
    assert calls == ["userAbstraction", "clearinghouseState", "spotClearinghouseState"]
    assert not snapshot["errors"]
    assert snapshot["account_balances"]["source"]["balance"] == "108.55"
    assert snapshot["account_balances"]["destination"]["balance"] == "31.49"


def test_vault_refresh_reuses_shared_leader_reads():
    """Two vaults sharing a leader need seven reads, not two full preflights."""
    calls = []

    def read(kind, **kwargs):
        """Return leader equity rather than total vault assets."""
        calls.append(kind)
        return {"vaultDetails": {"leader": LEADER, "followerState": {"vaultEquity": "456.70"}, "maxWithdrawable": "400"},
                "userAbstraction": "standard", "clearinghouseState": {"marginSummary": {"accountValue": "5132.83"}},
                "spotClearinghouseState": {"balances": []}}[kind]

    cache = TargetReadCache()
    keys = []
    for address in (ADDRESS, "0x" + "3" * 40):
        snapshot = balances.collect_hyperliquid_balances(SimpleNamespace(wallet_address=address, is_vault=True), {}, SimpleNamespace(read=read), cache)
        assert snapshot["account_balances"]["source"]["balance"] == "456.70"
        assert snapshot["account_balances"]["destination"]["main_perps"]["balance"] == "5132.83"
        assert not snapshot["errors"]
        key = snapshot["account_balances"]["destination"]["main_perps"]["overview_key"]
        assert len(key) == 64 and LEADER not in key
        keys.append(key)
    assert len(calls) == 7
    assert keys[0] == keys[1]


@pytest.mark.parametrize("status,retries,code", [(429, 3, "rate_limited"), (503, 3, "exchange_unavailable"), (401, 1, "read_failed")])
def test_http_retries_are_bounded_and_only_transient(monkeypatch, status, retries, code):
    """Retry transient HTTP failures with waits, but never retry authentication failures."""
    calls, waits = [], []
    reader = balances.BalanceReader()
    monkeypatch.setattr(reader, "_pace", lambda: None)
    monkeypatch.setattr(reader, "_wait", waits.append)

    def fail(*args, **kwargs):
        """Raise an HTTP error with a response body that must not escape."""
        calls.append(1)
        response = requests.Response()
        response.status_code = status
        response.headers["Retry-After"] = "5"
        raise requests.HTTPError("SECRET URL", response=response)

    monkeypatch.setattr(balances, "hyperliquid_readonly_info", fail)
    with pytest.raises(balances.BalanceReadError) as error:
        reader.read("spotMeta")
    assert str(error.value) == code
    assert len(calls) == retries
    assert len(waits) == retries - 1
    if status == 429:
        assert all(wait >= 5 for wait in waits)
        assert reader.cooldown > 0


def test_timeout_recovery_and_shutdown_interrupt_waits(monkeypatch):
    """A transient timeout recovers; shutdown prevents any subsequent request."""
    reader = balances.BalanceReader()
    calls = []

    def fetch(*args, **kwargs):
        """Fail once and return a valid synthetic balance on retry."""
        calls.append(1)
        if len(calls) == 1:
            raise requests.Timeout("SECRET")
        return {"balance": "12"}

    monkeypatch.setattr(balances, "hyperliquid_readonly_info", fetch)
    monkeypatch.setattr(reader, "_wait", lambda _: None)
    assert reader.read("spotMeta") == {"balance": "12"}
    reader.stop()
    with pytest.raises(balances.BalanceReadError, match="read_cancelled"):
        reader.read("spotMeta")
    assert len(calls) == 2


def test_pacing_uses_shared_clock_and_cooldown(monkeypatch):
    """No subsequent account request can skip the shared rate-limit pause."""
    reader = balances.BalanceReader()
    clock, waits = [100.0], []

    def wait(seconds):
        """Advance synthetic time without sleeping."""
        waits.append(seconds)
        clock[0] += seconds

    monkeypatch.setattr(balances.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(reader, "_wait", wait)
    reader._pace()
    reader._pace()
    assert waits == pytest.approx([0.4])
    reader.cooldown = clock[0] + 10
    reader._pace()
    assert waits[-1] == pytest.approx(10)
