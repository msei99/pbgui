"""Offline regression tests for shared Bitget mode resolution in snapshots."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import profit_sweep_exchanges as exchanges
from bitget_uta import BitgetUTAError


@pytest.mark.parametrize("failure", [
    TimeoutError("request timed out"),
    RuntimeError("not a UTA account"),
    RuntimeError('bitget {"code":"40009","msg":"signature error"}'),
])
def test_unknown_mode_never_reads_classic_accounts(monkeypatch, failure):
    """No speculative Classic reads or transferable snapshot after probe errors."""
    calls = []

    def read(client, method, params):
        """Record only the failing settings read."""
        calls.append(method)
        raise failure

    monkeypatch.setattr(exchanges, "_client_read", read)
    with pytest.raises(exchanges.ReadOnlyRequestError, match="could not be verified"):
        exchanges._collect_bitget({}, SimpleNamespace(), "USDT", 1, 2)
    assert calls == ["privateUtaGetV3AccountSettings"]


@pytest.mark.parametrize("mode", ["upgrading", "switching", "unknown", "classic"])
def test_snapshot_rejects_unverified_success_modes(monkeypatch, mode):
    """Settings success alone must not imply a usable UTA account."""
    calls = []

    def read(client, method, params):
        """Return a settings envelope with an unsupported native mode."""
        calls.append(method)
        return {"code": "00000", "data": {"accountMode": mode}}

    monkeypatch.setattr(exchanges, "_client_read", read)
    with pytest.raises(BitgetUTAError, match="unknown or transitioning"):
        exchanges._collect_bitget({}, SimpleNamespace(), "USDT", 1, 2)
    assert calls == ["privateUtaGetV3AccountSettings"]


def test_funding_assets_without_implicit_ccxt_method():
    """Pinned CCXT must use the fixed v3 GET, never the unrelated v2 route."""
    response = {"code": "00000", "data": [{"coin": "USDT", "balance": "2", "available": "2", "frozen": "0"}]}
    request = Mock(return_value=response)
    assert exchanges._bitget_funding_assets(SimpleNamespace(request=request), "USDT") == response
    request.assert_called_once_with("v3/account/funding-assets", ["private", "uta"], "GET", {"coin": "USDT"}, {"cost": 1})


def test_funding_assets_prefers_available_implicit_method():
    """Newer CCXT keeps the same allowlisted native method contract."""
    implicit = Mock(return_value={"code": "00000", "data": []})
    request = Mock()
    client = SimpleNamespace(privateUtaGetV3AccountFundingAssets=implicit, request=request)
    assert exchanges._bitget_funding_assets(client, "USDT") == {"code": "00000", "data": []}
    implicit.assert_called_once_with({"coin": "USDT"})
    request.assert_not_called()


def test_funding_assets_does_not_retry_permission_denial_via_fallback():
    """A native permission error is not evidence of a missing method."""
    implicit = Mock(side_effect=PermissionError("denied"))
    request = Mock()
    client = SimpleNamespace(privateUtaGetV3AccountFundingAssets=implicit, request=request)
    with pytest.raises(PermissionError):
        exchanges._bitget_funding_assets(client, "USDT")
    request.assert_not_called()


@pytest.mark.parametrize("cursor", [None, ""])
def test_funded_uta_snapshot_keeps_wallet_and_funding_separate(cursor):
    """Replay funded UTA/Funding wallets with an explicitly empty futures ledger."""
    maximum = {"code": "00000", "data": {"coin": "USDT", "maxTransfer": "3"}}
    funding = {"code": "00000", "data": [{"coin": "USDT", "balance": "2", "available": "2", "frozen": "0"}]}
    request = Mock(side_effect=[maximum, funding])
    client = SimpleNamespace(
        privateUtaGetV3AccountSettings=Mock(return_value={"code": "00000", "data": {"accountMode": "unified"}}),
        privateUtaGetV3AccountAssets=Mock(return_value={"code": "00000", "data": {
            "accountEquity": "2.9989", "assets": [{"coin": "USDT", "balance": "3", "equity": "3", "available": "3"}],
        }}),
        privateUtaGetV3AccountFinancialRecords=Mock(return_value={"code": "00000", "data": {"list": None, "cursor": cursor}}),
        request=request,
    )
    snapshot = {"errors": []}
    exchanges._collect_bitget(snapshot, client, "USDT", 1000, 2000)
    assert snapshot["errors"] == []
    assert snapshot["account_balances"]["source"]["balance"] == "3"
    assert snapshot["account_balances"]["destination"]["balance"] == "2"
    assert snapshot["account_balances"]["destination"]["withdrawable"] == "2"
    assert snapshot["account_balances"]["max_transferable"] == "3"
    assert snapshot["realized_net_pnl"] == "0"
    assert [call.args[0] for call in request.call_args_list] == ["v3/account/max-transferable", "v3/account/funding-assets"]
    assert all(call.args[2] == "GET" for call in request.call_args_list)


@pytest.mark.parametrize("payload", [
    {"code": "00000", "data": {"cursor": None}},
    {"code": "00000", "data": {"list": None}},
    {"code": "00000", "data": {"list": None, "cursor": "next"}},
    {"code": "00000", "data": {"list": None, "cursor": False}},
    {"code": "40014", "data": {"list": None, "cursor": None}},
])
def test_null_snapshot_ledger_does_not_hide_invalid_responses(payload):
    """Missing fields, errors and continuing null pages still invalidate snapshots."""
    client = SimpleNamespace(privateUtaGetV3AccountFinancialRecords=Mock(return_value=payload))
    with pytest.raises(exchanges.ReadOnlyRequestError):
        exchanges._bitget_history(client, "uta", 1000, 2000, "USDT")
