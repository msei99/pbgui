"""Offline checks for the one-time Hyperliquid request-credit purchase."""

import json
from types import SimpleNamespace

import ccxt
import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from api import vps_manager


ADDRESS = "0x" + "a" * 40


class FakeUsers:
    """Return one test account without reading the saved credential store."""

    def find_user(self, name):
        """Resolve the test account by name."""
        if name != "hl_test":
            return None
        return SimpleNamespace(
            name=name, exchange="hyperliquid", is_vault=False,
            wallet_address=ADDRESS, private_key="test-private-key",
        )


def _install_fakes(monkeypatch, response=None, failure=None):
    """Replace every credential, exchange, and persistence dependency."""
    import User

    calls = []
    monkeypatch.setattr(User, "Users", FakeUsers)
    monkeypatch.setattr(vps_manager, "_log", lambda *args, **kwargs: None)
    monkeypatch.setattr(vps_manager, "_append_hl_rate_limit_sample", lambda *args: calls.append(("sample", args)))
    monkeypatch.setattr(vps_manager, "_read_hl_rate_limit_now", lambda address: (100, 10000))
    monkeypatch.setattr(vps_manager, "_hl_rate_limit_accounts", {})

    class FakeExchange:
        """Capture the requested credits without exchange traffic."""

        def reserve_request_weight(self, credits):
            """Return the configured fake exchange result."""
            calls.append(("reserve", credits))
            assert vps_manager.restart_block_reason()
            if failure:
                raise failure
            return response if response is not None else {"status": "ok", "response": {"type": "default"}}

    monkeypatch.setattr(ccxt, "hyperliquid", lambda config: FakeExchange())
    return calls


@pytest.mark.parametrize("credits", [0, -1, 100001, 1.5, "6000", True])
def test_purchase_rejects_invalid_credit_counts(credits):
    """Credit amounts must be whole integers within the purchase limit."""
    with pytest.raises(ValidationError):
        vps_manager.HlCreditPurchaseRequest(user_name="hl_test", credits=credits)


def test_purchase_reserves_exact_requested_credits(monkeypatch):
    """One confirmed purchase uses the exact count and refreshes cached counters."""
    calls = _install_fakes(monkeypatch)
    result = vps_manager.purchase_hl_request_credits(
        vps_manager.HlCreditPurchaseRequest(user_name="hl_test", credits=6000),
        session=object(),
    )
    assert calls[0] == ("reserve", 6000)
    payload = json.loads(result.body)
    assert payload["credits"] == 6000
    assert payload["cost_usdc"] == "3.0000"
    assert payload["verified"] is True
    assert vps_manager._hl_rate_limit_accounts[ADDRESS]["used"] == 100
    assert vps_manager.restart_block_reason() == ""


def test_purchase_does_not_retry_uncertain_exchange_result(monkeypatch):
    """A lost exchange response returns an explicit unknown-state error."""
    calls = _install_fakes(monkeypatch, failure=ccxt.RequestTimeout("timeout"))
    with pytest.raises(HTTPException) as exc:
        vps_manager.purchase_hl_request_credits(
            vps_manager.HlCreditPurchaseRequest(user_name="hl_test", credits=6000),
            session=object(),
        )
    assert exc.value.status_code == 502
    assert "Check the account limit" in exc.value.detail
    assert calls == [("reserve", 6000)]
    assert vps_manager.restart_block_reason() == ""


def test_purchase_rejects_parallel_action(monkeypatch):
    """A second request cannot submit while a purchase is active."""
    _install_fakes(monkeypatch)
    assert vps_manager._hl_credit_purchase_lock.acquire(blocking=False)
    try:
        with pytest.raises(HTTPException) as exc:
            vps_manager.purchase_hl_request_credits(
                vps_manager.HlCreditPurchaseRequest(user_name="hl_test", credits=1),
                session=object(),
            )
        assert exc.value.status_code == 409
        assert vps_manager.restart_block_reason()
    finally:
        vps_manager._hl_credit_purchase_lock.release()


def test_purchase_keeps_success_when_followup_read_fails(monkeypatch):
    """A successful action remains successful if counter verification is unavailable."""
    calls = _install_fakes(monkeypatch)
    monkeypatch.setattr(vps_manager, "_read_hl_rate_limit_now", lambda address: (_ for _ in ()).throw(ValueError("bad info response")))
    result = vps_manager.purchase_hl_request_credits(
        vps_manager.HlCreditPurchaseRequest(user_name="hl_test", credits=1),
        session=object(),
    )
    assert calls == [("reserve", 1)]
    assert json.loads(result.body)["verified"] is False
    assert vps_manager.restart_block_reason() == ""


def test_purchase_requires_saved_main_account(monkeypatch):
    """A missing account never reaches the exchange action."""
    calls = _install_fakes(monkeypatch)
    with pytest.raises(HTTPException) as exc:
        vps_manager.purchase_hl_request_credits(
            vps_manager.HlCreditPurchaseRequest(user_name="missing", credits=1),
            session=object(),
        )
    assert exc.value.status_code == 404
    assert calls == []
