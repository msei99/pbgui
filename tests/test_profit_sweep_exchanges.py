"""Offline contract tests for profit-sweep exchange snapshots."""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

import pytest

import profit_sweep_exchanges as exchanges


WALLET = "0x" + "a" * 40
VAULT = "0x" + "b" * 40
LEADER = "0x" + "c" * 40
AGENT = "0x" + "d" * 40
NOW_MS = 2_000_000


def _user(*, is_vault: bool = False, exchange: str = "hyperliquid") -> SimpleNamespace:
    """Build a synthetic exchange user with a private-key sentinel."""

    return SimpleNamespace(
        name="fixture-user",
        exchange=exchange,
        key="API-KEY-MUST-NOT-ESCAPE",
        secret="API-SECRET-MUST-NOT-ESCAPE",
        passphrase="PASSPHRASE-MUST-NOT-ESCAPE",
        quote="USDT",
        wallet_address=VAULT if is_vault else WALLET,
        private_key="PRIVATE-KEY-MUST-NOT-ESCAPE",
        is_vault=is_vault,
        agent_address=AGENT if is_vault else None,
        extra={},
    )


def test_hyperliquid_default_abstraction_is_standard_manual() -> None:
    """Map Hyperliquid's current default label to its separated balance mode."""

    assert exchanges._account_mode("default") == "standard_manual"


def test_empty_hyperliquid_spot_balance_is_confirmed_zero() -> None:
    """Treat a successful empty spot-clearing list as zero rather than unavailable."""

    assert exchanges._spot_usdc_balance({"balances": []}) == {
        "coin": "USDC",
        "total": "0",
        "hold": "0",
    }
    assert exchanges._spot_usdc_balance({}) is None


def test_vault_destination_balance_uses_unified_spot_state() -> None:
    """Use the shared spot-clearing balance when separate perp state is meaningless."""

    destinations = exchanges._vault_destination_balances(
        "unified",
        {"accountValue": "0"},
        {"total": "4942.027581", "hold": "0"},
    )

    assert destinations == {
        "main_perps": {
            "label": "Main Unified",
            "balance": "4942.027581",
            "available": True,
            "withdrawable": "4942.027581",
            "asset": "USDC",
        },
        "main_spot": {
            "label": "Main Unified",
            "balance": "4942.027581",
            "available": True,
            "withdrawable": "4942.027581",
            "asset": "USDC",
        },
    }


class FakeCcxtClient:
    """Capture exact private GET calls and return configured offline responses."""

    def __init__(self, responses: dict[str, Any]) -> None:
        """Initialize response fixtures, call history, and timeout state."""

        self.responses = responses
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.timeout: int | None = None

    def _call(self, method: str, params: dict[str, Any]) -> Any:
        """Record one method call and resolve its static or callable response."""

        self.calls.append((method, dict(params)))
        response = self.responses[method]
        if isinstance(response, Exception):
            raise response
        if callable(response):
            return response(params)
        return response

    def privateGetV5AssetTransferQueryTransferCoinList(self, params: dict[str, Any]) -> Any:
        """Return Bybit transferable coins."""

        return self._call("privateGetV5AssetTransferQueryTransferCoinList", params)

    def privateGetV5AssetTransferQueryAccountCoinBalance(self, params: dict[str, Any]) -> Any:
        """Return Bybit transfer balance."""

        return self._call("privateGetV5AssetTransferQueryAccountCoinBalance", params)

    def privateGetV5AssetTransferQueryAccountCoinsBalance(self, params: dict[str, Any]) -> Any:
        """Return Bybit Funding wallet balances."""

        return self._call("privateGetV5AssetTransferQueryAccountCoinsBalance", params)

    def privateGetV5AccountWalletBalance(self, params: dict[str, Any]) -> Any:
        """Return Bybit Unified wallet balance."""

        return self._call("privateGetV5AccountWalletBalance", params)

    def privateGetV5AccountTransactionLog(self, params: dict[str, Any]) -> Any:
        """Return Bybit Unified transaction history."""

        return self._call("privateGetV5AccountTransactionLog", params)

    def fapiPrivateV3GetBalance(self, params: dict[str, Any]) -> Any:
        """Return Binance USD-M balances."""

        return self._call("fapiPrivateV3GetBalance", params)

    def fapiPrivateGetIncome(self, params: dict[str, Any]) -> Any:
        """Return one Binance income class."""

        return self._call("fapiPrivateGetIncome", params)

    def sapiPostAssetGetFundingAsset(self, params: dict[str, Any]) -> Any:
        """Return Binance Funding wallet assets from its read-only SAPI POST."""

        return self._call("sapiPostAssetGetFundingAsset", params)

    def privateUtaGetV3AccountSettings(self, params: dict[str, Any]) -> Any:
        """Return Bitget UTA settings or a Classic-mode error."""

        return self._call("privateUtaGetV3AccountSettings", params)

    def privateUtaGetV3AccountAssets(self, params: dict[str, Any]) -> Any:
        """Return Bitget UTA assets."""

        return self._call("privateUtaGetV3AccountAssets", params)

    def privateUtaGetV3AccountFundingAssets(self, params: dict[str, Any]) -> Any:
        """Return Bitget UTA Funding assets."""

        return self._call("privateUtaGetV3AccountFundingAssets", params)

    def privateUtaGetV3AccountMaxTransferable(self, params: dict[str, Any]) -> Any:
        """Return Bitget UTA max transferable amount."""

        return self._call("privateUtaGetV3AccountMaxTransferable", params)

    def privateUtaGetV3AccountFinancialRecords(self, params: dict[str, Any]) -> Any:
        """Return Bitget UTA financial records."""

        return self._call("privateUtaGetV3AccountFinancialRecords", params)

    def privateMixGetV2MixAccountAccounts(self, params: dict[str, Any]) -> Any:
        """Return Bitget Classic futures accounts."""

        return self._call("privateMixGetV2MixAccountAccounts", params)

    def privateMixGetV2MixAccountBill(self, params: dict[str, Any]) -> Any:
        """Return Bitget Classic futures bills."""

        return self._call("privateMixGetV2MixAccountBill", params)

    def privateSpotGetV2AccountFundingAssets(self, params: dict[str, Any]) -> Any:
        """Return Bitget Classic Funding/P2P assets."""

        return self._call("privateSpotGetV2AccountFundingAssets", params)

    def privateSpotGetV2SpotAccountAssets(self, params: dict[str, Any]) -> Any:
        """Return Bitget Classic Spot assets."""

        return self._call("privateSpotGetV2SpotAccountAssets", params)


def _install_ccxt_client(
    monkeypatch: pytest.MonkeyPatch,
    client: FakeCcxtClient,
) -> list[Any]:
    """Install one fake Exchange owner and return every created owner."""

    owners: list[Any] = []

    class FakeExchange:
        """Expose deterministic connect and close behavior for one fake client."""

        def __init__(self, exchange: str, user: Any) -> None:
            """Record owner construction without reading credential values."""

            self.exchange = exchange
            self.user = user
            self.instance: FakeCcxtClient | None = None
            self.closed = False
            owners.append(self)

        def connect(self) -> None:
            """Attach the supplied offline client."""

            self.instance = client

        def close(self) -> None:
            """Record deterministic cleanup."""

            self.closed = True

    monkeypatch.setattr(exchanges, "Exchange", FakeExchange)
    return owners


def _common_responses(payload: dict[str, Any]) -> Any:
    """Return shared synthetic responses for fixed Hyperliquid reads."""

    request_type = payload["type"]
    if request_type == "spotMeta":
        return {"tokens": [{"name": "USDC", "tokenId": "0xusdc", "szDecimals": 6}]}
    if request_type == "openOrders":
        return []
    if request_type == "userNonFundingLedgerUpdates":
        return []
    raise AssertionError(f"Unexpected shared request: {payload}")


@pytest.mark.parametrize(
    "request_type",
    ["agentSendAsset", "vaultTransfer", "usdClassTransfer", "withdraw", "exchange"],
)
def test_readonly_allowlist_rejects_action_like_types(
    monkeypatch: pytest.MonkeyPatch,
    request_type: str,
) -> None:
    """Reject action-like request types before any HTTP helper is called."""

    calls: list[dict[str, Any]] = []

    def capture(payload: dict[str, Any], *, timeout_s: float) -> Any:
        """Capture an unexpected low-level call."""

        calls.append(payload)
        return {}

    monkeypatch.setattr(exchanges, "hyperliquid_info_post", capture)
    with pytest.raises(exchanges.ReadOnlyRequestError):
        exchanges.hyperliquid_readonly_info(request_type, user=WALLET)
    assert calls == []


def test_readonly_wrapper_builds_only_fixed_payload_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    """Build a timed read from explicit scalar arguments without passthrough."""

    calls: list[tuple[dict[str, Any], float]] = []

    def capture(payload: dict[str, Any], *, timeout_s: float) -> list[Any]:
        """Capture one fixed info request."""

        calls.append((payload, timeout_s))
        return []

    monkeypatch.setattr(exchanges, "hyperliquid_info_post", capture)
    exchanges.hyperliquid_readonly_info(
        "userFillsByTime",
        user=WALLET.upper().replace("0X", "0x"),
        start_ms=1,
        end_ms=2,
        timeout_s=4,
    )
    assert calls == [(
        {"type": "userFillsByTime", "user": WALLET, "startTime": 1, "endTime": 2},
        4.0,
    )]
    with pytest.raises(exchanges.ReadOnlyRequestError):
        exchanges.hyperliquid_readonly_info({"type": "spotMeta"})  # type: ignore[arg-type]


def test_ccxt_read_allowlist_rejects_generic_or_write_methods() -> None:
    """Reject non-contract CCXT methods before invoking the client."""

    client = FakeCcxtClient({})

    for method in ("fetchBalance", "privatePostV5AssetTransferInterTransfer", "withdraw"):
        with pytest.raises(exchanges.ReadOnlyRequestError):
            exchanges._client_read(client, method, {})
    assert client.calls == []


def test_normal_snapshot_preserves_decimal_pnl_and_stable_events(monkeypatch: pytest.MonkeyPatch) -> None:
    """Collect Standard/Manual balances, activity, and exact realized net PnL."""

    calls: list[dict[str, Any]] = []

    def fake_info(payload: dict[str, Any], *, timeout_s: float) -> Any:
        """Return an offline normal-account fixture by read type."""

        calls.append(payload)
        request_type = payload["type"]
        if request_type == "userAbstraction":
            return "disabled"
        if request_type == "userRole":
            return {"role": "user"}
        if request_type == "clearinghouseState":
            return {
                "marginSummary": {"accountValue": "1000.1200"},
                "withdrawable": "750.3400",
                "assetPositions": [
                    {"position": {"coin": "BTC", "szi": "0.010", "entryPx": "60000.0", "unrealizedPnl": "2.50"}},
                    {"position": {"coin": "ETH", "szi": "0"}},
                ],
            }
        if request_type == "spotClearinghouseState":
            return {"balances": [{"coin": "USDC", "total": "25.0000", "hold": "1.00"}]}
        if request_type == "openOrders":
            return [{"oid": 7, "coin": "BTC", "side": "A", "limitPx": "65000.00", "sz": "0.01"}]
        if request_type == "userFillsByTime":
            return [{
                "time": 1_900_000,
                "hash": "0xfill",
                "tid": 11,
                "oid": 7,
                "coin": "BTC",
                "side": "A",
                "px": "61000.00",
                "sz": "0.01",
                "closedPnl": "10.2500",
                "fee": "0.1250",
            }]
        if request_type == "userFunding":
            return [{
                "time": 1_910_000,
                "hash": "0xfunding",
                "delta": {"type": "funding", "coin": "BTC", "usdc": "-0.5000", "fundingRate": "0.0001", "szi": "0.01"},
            }]
        if request_type == "userNonFundingLedgerUpdates":
            return [{"time": 1_920_000, "hash": "0xdeposit", "delta": {"type": "deposit", "usdc": "50.000"}}]
        return _common_responses(payload)

    monkeypatch.setattr(exchanges, "hyperliquid_info_post", fake_info)
    monkeypatch.setattr(exchanges, "_now_ms", lambda: NOW_MS)
    first = exchanges.collect_readonly_snapshot(_user(), 1_800_000, NOW_MS)
    second = exchanges.collect_readonly_snapshot(_user(), 1_800_000, NOW_MS)

    assert first["complete"] is True
    assert first["account"]["mode"] == "standard_manual"
    assert first["account"]["account_value"] == "1000.1200"
    assert first["account"]["withdrawable"] == "750.3400"
    assert first["account"]["spot_usdc"]["total"] == "25.0000"
    assert first["account_balances"] == {
        "source": {
            "label": "Perps",
            "balance": "1000.1200",
            "available": True,
            "withdrawable": "750.3400",
            "asset": "USDC",
        },
        "destination": {
            "label": "Spot",
            "balance": "25.0000",
            "available": True,
            "asset": "USDC",
        },
        "max_transferable": "750.3400",
    }
    assert [position["coin"] for position in first["positions"]] == ["BTC"]
    assert first["fills"]["closed_pnl_less_fees"] == "10.1250"
    assert first["funding"]["total"] == "-0.5000"
    assert first["realized_net_pnl"] == "9.6250"
    assert [event["id"] for event in first["events"]] == [event["id"] for event in second["events"]]
    assert all(payload["type"] in exchanges.HYPERLIQUID_READ_TYPES for payload in calls)
    encoded = json.dumps(first)
    assert "PRIVATE-KEY-MUST-NOT-ESCAPE" not in encoded
    assert "private_key" not in encoded


def test_vault_snapshot_uses_leader_context_and_exact_commission_hash(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Resolve a vault leader, validate its agent, and attribute exact commission events."""

    calls: list[dict[str, Any]] = []

    def fake_info(payload: dict[str, Any], *, timeout_s: float) -> Any:
        """Return an offline legacy-vault fixture by read type and account."""

        calls.append(payload)
        request_type = payload["type"]
        if request_type == "vaultDetails" and "user" not in payload:
            return {"leader": LEADER}
        if request_type == "vaultDetails":
            assert payload["user"] == LEADER
            return {
                "leader": LEADER,
                "leaderFraction": "0.2500",
                "maxWithdrawable": "120.5000",
                "alwaysCloseOnWithdraw": False,
                "isClosed": False,
                "followerState": {
                    "allTimePnl": "45.6700",
                    "pnl": "5.25",
                    "vaultEquity": "321.0000",
                    "lockupUntil": NOW_MS - 1,
                },
            }
        if request_type == "userRole":
            return {"role": "vault"}
        if request_type == "userAbstraction":
            return "disabled"
        if request_type == "extraAgents":
            assert payload["user"] == LEADER
            return [{"address": AGENT, "name": "PBGui", "validUntil": NOW_MS + 100_000}]
        if request_type == "clearinghouseState" and payload["user"] == VAULT:
            return {
                "marginSummary": {"accountValue": "900.0000"},
                "withdrawable": "200.0000",
                "assetPositions": [{"position": {"coin": "ETH", "szi": "1.00", "unrealizedPnl": "3.00"}}],
            }
        if request_type == "clearinghouseState" and payload["user"] == LEADER:
            return {
                "marginSummary": {"accountValue": "55.5000"},
                "withdrawable": "50.0000",
                "assetPositions": [],
            }
        if request_type == "spotClearinghouseState" and payload["user"] == LEADER:
            return {"balances": [{"coin": "USDC", "total": "12.3400", "hold": "0"}]}
        if request_type == "openOrders":
            return [{"oid": 8, "coin": "ETH", "side": "B", "limitPx": "3000", "sz": "1"}]
        if request_type == "userVaultEquities":
            assert payload["user"] == LEADER
            return [{"vaultAddress": VAULT, "equity": "321.0000", "lockedUntilTimestamp": NOW_MS - 1}]
        if request_type == "userNonFundingLedgerUpdates" and payload["user"] == VAULT:
            return [{
                "time": 1_900_000,
                "hash": "0xshared",
                "delta": {
                    "type": "vaultWithdraw",
                    "basis": "10.00",
                    "netWithdrawnUsd": "14.00",
                    "commission": "1.2500",
                },
            }]
        if request_type == "userNonFundingLedgerUpdates" and payload["user"] == LEADER:
            return [
                {
                    "time": 1_900_000,
                    "hash": "0xshared",
                    "delta": {"type": "vaultLeaderCommission", "usdc": "1.2500"},
                },
                {
                    "time": 1_900_001,
                    "hash": "0xother",
                    "delta": {"type": "vaultLeaderCommission", "usdc": "9.99"},
                },
            ]
        return _common_responses(payload)

    monkeypatch.setattr(exchanges, "hyperliquid_info_post", fake_info)
    monkeypatch.setattr(exchanges, "_now_ms", lambda: NOW_MS)
    snapshot = exchanges.collect_readonly_snapshot(_user(is_vault=True), 1_800_000, NOW_MS)

    assert snapshot["complete"] is True
    assert snapshot["account_kind"] == "vault"
    assert snapshot["leader"]["address"] == LEADER
    assert snapshot["leader"]["agent"] == {
        "configured": True,
        "relationship_valid": True,
        "matched": True,
        "valid_until_ms": NOW_MS + 100_000,
        "expired": False,
        "name": "PBGui",
    }
    assert AGENT not in json.dumps(snapshot)
    assert snapshot["vault"]["all_time_pnl"] == "45.6700"
    assert snapshot["vault"]["vault_equity"] == "321.0000"
    assert snapshot["vault"]["leader_fraction"] == "0.2500"
    assert snapshot["vault"]["max_withdrawable"] == "120.5000"
    assert snapshot["vault"]["always_close_on_withdraw"] is False
    assert snapshot["vault"]["user_vault_equity"]["equity"] == "321.0000"
    assert snapshot["account_balances"] == {
        "source": {
            "label": "Vault",
            "balance": "321.0000",
            "available": True,
            "withdrawable": "200.0000",
            "asset": "USDC",
        },
        "destination": {
                "main_perps": {
                    "label": "Main Perps",
                    "balance": "55.5000",
                    "available": True,
                    "withdrawable": "50.0000",
                    "asset": "USDC",
            },
                "main_spot": {
                    "label": "Main Spot",
                    "balance": "12.3400",
                    "available": True,
                    "withdrawable": "12.3400",
                    "asset": "USDC",
            },
        },
        "max_transferable": "120.5000",
    }
    attributions = snapshot["vault_leader_commissions"]["attributions"]
    assert [item["status"] for item in attributions] == ["exact", "unmatched"]
    assert snapshot["vault_leader_commissions"]["exact_attributed_total"] == "1.2500"
    scoped_details = [payload for payload in calls if payload["type"] == "vaultDetails" and "user" in payload]
    assert scoped_details == [{"type": "vaultDetails", "vaultAddress": VAULT, "user": LEADER}]
    assert [
        payload
        for payload in calls
        if payload["type"] in {"clearinghouseState", "spotClearinghouseState"}
    ] == [
        {"type": "clearinghouseState", "user": VAULT},
        {"type": "clearinghouseState", "user": LEADER},
        {"type": "spotClearinghouseState", "user": LEADER},
    ]
    encoded = json.dumps(snapshot)
    assert "PRIVATE-KEY-MUST-NOT-ESCAPE" not in encoded
    assert "private_key" not in encoded


def test_snapshot_fails_closed_on_stale_or_incomplete_history(monkeypatch: pytest.MonkeyPatch) -> None:
    """Expose freshness and transport failures rather than presenting usable data."""

    def fake_info(payload: dict[str, Any], *, timeout_s: float) -> Any:
        """Fail one required history source and satisfy the other reads."""

        request_type = payload["type"]
        if request_type == "userAbstraction":
            return "disabled"
        if request_type == "userRole":
            return "user"
        if request_type == "clearinghouseState":
            return {"marginSummary": {"accountValue": "1"}, "withdrawable": "1", "assetPositions": []}
        if request_type == "spotClearinghouseState":
            return {"balances": []}
        if request_type == "userFillsByTime":
            raise TimeoutError("offline timeout fixture")
        if request_type == "userFunding":
            return []
        return _common_responses(payload)

    monkeypatch.setattr(exchanges, "hyperliquid_info_post", fake_info)
    monkeypatch.setattr(exchanges, "_now_ms", lambda: 10_000_000)
    snapshot = exchanges.collect_readonly_snapshot(_user(), 1, 2)

    assert snapshot["complete"] is False
    assert snapshot["history"]["fresh"] is False
    assert {error["code"] for error in snapshot["errors"]} == {"stale_snapshot", "read_failed"}
    assert snapshot["fills"]["complete"] is False


def test_bybit_snapshot_uses_only_fixed_unified_reads_and_closes(monkeypatch: pytest.MonkeyPatch) -> None:
    """Collect Bybit wallet, transfer cap, and fee-preserving UTA history."""

    client = FakeCcxtClient({
        "privateGetV5AssetTransferQueryTransferCoinList": {
            "retCode": 0,
            "result": {"list": ["USDT", "USDC"]},
        },
        "privateGetV5AssetTransferQueryAccountCoinBalance": {
            "retCode": 0,
            "result": {"balance": {"coin": "USDT", "walletBalance": "100", "transferBalance": "70.5"}},
        },
        "privateGetV5AccountWalletBalance": {
            "retCode": 0,
            "result": {
                "list": [{
                    "accountType": "UNIFIED",
                    "coin": [{"coin": "USDT", "walletBalance": "100", "equity": "104.25"}],
                }],
            },
        },
        "privateGetV5AssetTransferQueryAccountCoinsBalance": {
            "retCode": 0,
            "result": {
                "accountType": "FUND",
                "balance": [{"coin": "USDT", "walletBalance": "23.75", "transferBalance": "23.75"}],
            },
        },
        "privateGetV5AccountTransactionLog": {
            "retCode": 0,
            "result": {
                "nextPageCursor": "",
                "list": [
                    {
                        "id": "trade-1",
                        "transactionTime": "1900000",
                        "type": "TRADE",
                        "symbol": "BTCUSDT",
                        "cashFlow": "10",
                        "fee": "0.25",
                        "funding": "",
                        "tradeId": "trade-1",
                    },
                    {
                        "id": "funding-1",
                        "transactionTime": "1910000",
                        "type": "SETTLEMENT",
                        "symbol": "BTCUSDT",
                        "cashFlow": "0",
                        "fee": "0",
                        "funding": "-0.5",
                    },
                    {
                        "id": "deposit-1",
                        "transactionTime": "1920000",
                        "type": "DEPOSIT",
                        "cashFlow": "100",
                        "fee": "0",
                        "funding": "",
                    },
                    {
                        "id": "transfer-1",
                        "transactionTime": "1930000",
                        "type": "TRANSFER_IN",
                        "cashFlow": "50",
                        "fee": "0",
                        "funding": "",
                    },
                ],
            },
        },
    })
    owners = _install_ccxt_client(monkeypatch, client)
    monkeypatch.setattr(exchanges, "_now_ms", lambda: NOW_MS)

    snapshot = exchanges.collect_readonly_snapshot(_user(exchange="bybit"), 1_800_000, NOW_MS)

    assert snapshot["complete"] is True
    assert snapshot["account_kind"] == "normal"
    assert snapshot["account"] == {
        "mode": "unified",
        "balance": "100",
        "account_value": "104.25",
        "withdrawable": "70.5",
    }
    assert snapshot["fills"]["events"][0]["fee"] == "0.25"
    assert [event["trade_id"] for event in snapshot["fills"]["events"]] == ["trade-1"]
    assert len(snapshot["funding"]["events"]) == 1
    assert snapshot["realized_net_pnl"] == "9.25"
    assert snapshot["asset"] == {"symbol": "USDT", "amount_precision": 8}
    assert snapshot["account_balances"] == {
        "source": {
            "label": "Unified",
            "balance": "100",
            "available": True,
            "withdrawable": "70.5",
            "asset": "USDT",
        },
        "destination": {
            "label": "Funding",
            "balance": "23.75",
            "available": True,
            "withdrawable": "23.75",
            "asset": "USDT",
        },
        "max_transferable": "70.5",
    }
    assert client.calls == [
        ("privateGetV5AssetTransferQueryTransferCoinList", {"fromAccountType": "UNIFIED", "toAccountType": "FUND"}),
        (
            "privateGetV5AssetTransferQueryAccountCoinBalance",
            {"accountType": "UNIFIED", "toAccountType": "FUND", "coin": "USDT"},
        ),
        ("privateGetV5AccountWalletBalance", {"accountType": "UNIFIED", "coin": "USDT"}),
        ("privateGetV5AssetTransferQueryAccountCoinsBalance", {"accountType": "FUND", "coin": "USDT"}),
        (
            "privateGetV5AccountTransactionLog",
            {
                "accountType": "UNIFIED",
                "category": "linear",
                "currency": "USDT",
                "startTime": 1_800_000,
                "endTime": NOW_MS,
                "limit": 50,
            },
        ),
    ]
    assert client.timeout == 30_000
    assert len(owners) == 1 and owners[0].closed is True


def test_snapshot_authoritative_asset_binds_bybit_reads_and_rejects_unsupported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use the server-selected asset for every read and fail before I/O for unsupported assets."""

    client = FakeCcxtClient({
        "privateGetV5AssetTransferQueryTransferCoinList": {
            "retCode": 0,
            "result": {"list": ["USDC"]},
        },
        "privateGetV5AssetTransferQueryAccountCoinBalance": {
            "retCode": 0,
            "result": {"balance": {"coin": "USDC", "walletBalance": "4", "transferBalance": "3"}},
        },
        "privateGetV5AccountWalletBalance": {
            "retCode": 0,
            "result": {
                "list": [{
                    "accountType": "UNIFIED",
                    "coin": [{"coin": "USDC", "walletBalance": "4", "equity": "4"}],
                }],
            },
        },
        "privateGetV5AssetTransferQueryAccountCoinsBalance": {
            "retCode": 0,
            "result": {"balance": [{"coin": "USDC", "walletBalance": "1"}]},
        },
        "privateGetV5AccountTransactionLog": {
            "retCode": 0,
            "result": {"list": [], "nextPageCursor": ""},
        },
    })
    owners = _install_ccxt_client(monkeypatch, client)
    monkeypatch.setattr(exchanges, "_now_ms", lambda: NOW_MS)

    snapshot = exchanges.collect_readonly_snapshot(
        _user(exchange="bybit"),
        1_800_000,
        NOW_MS,
        settlement_asset="USDC",
    )
    unsupported = exchanges.collect_readonly_snapshot(
        _user(exchange="bybit"),
        1_800_000,
        NOW_MS,
        settlement_asset="BTC",
    )

    assert snapshot["complete"] is True
    assert snapshot["asset"]["symbol"] == "USDC"
    assert all(
        params.get("coin", params.get("currency", "USDC")) == "USDC"
        for _method, params in client.calls
    )
    assert client.calls[-1][1]["category"] == "linear"
    assert unsupported["complete"] is False
    assert unsupported["errors"][0]["code"] == "unsupported_asset"
    assert len(owners) == 1 and owners[0].closed is True


def test_binance_snapshot_separates_income_types_and_preserves_commission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Collect Binance maxWithdrawAmount and three explicit income classes."""

    def income(params: dict[str, Any]) -> list[dict[str, Any]]:
        """Return one fixture selected by the required incomeType."""

        rows = {
            "REALIZED_PNL": [{"incomeType": "REALIZED_PNL", "time": 1_900_000, "income": "4", "symbol": "BTCUSDT", "tradeId": "1"}],
            "COMMISSION": [{"incomeType": "COMMISSION", "time": 1_900_001, "income": "-0.2", "symbol": "BTCUSDT", "tranId": "2"}],
            "FUNDING_FEE": [{"incomeType": "FUNDING_FEE", "time": 1_900_002, "income": "0.1", "symbol": "BTCUSDT", "tranId": "3"}],
        }
        return rows[params["incomeType"]]

    client = FakeCcxtClient({
        "fapiPrivateV3GetBalance": [{
            "asset": "USDT",
            "balance": "500.00",
            "availableBalance": "450",
            "maxWithdrawAmount": "420.50",
        }],
        "sapiPostAssetGetFundingAsset": [{
            "asset": "USDT",
            "free": "20.25",
            "locked": "1.00",
            "freeze": "0.50",
            "withdrawing": "0.25",
        }],
        "fapiPrivateGetIncome": income,
    })
    owners = _install_ccxt_client(monkeypatch, client)
    monkeypatch.setattr(exchanges, "_now_ms", lambda: NOW_MS)

    snapshot = exchanges.collect_readonly_snapshot(_user(exchange="binance"), 1_800_000, NOW_MS)

    assert snapshot["complete"] is True
    assert snapshot["account"]["mode"] == "usd_m_futures"
    assert snapshot["account"]["withdrawable"] == "420.50"
    assert [event["fee"] for event in snapshot["fills"]["events"]] == ["0", "0.2"]
    assert snapshot["realized_net_pnl"] == "3.9"
    assert snapshot["account_balances"] == {
        "source": {
            "label": "USD-M Futures",
            "balance": "500.00",
            "available": True,
            "withdrawable": "420.50",
            "asset": "USDT",
        },
        "destination": {
            "label": "Funding Wallet",
            "balance": "22.00",
            "available": True,
            "withdrawable": "20.25",
            "asset": "USDT",
        },
        "max_transferable": "420.50",
    }
    assert client.calls == [
        ("fapiPrivateV3GetBalance", {}),
        ("sapiPostAssetGetFundingAsset", {"asset": "USDT"}),
        ("fapiPrivateGetIncome", {"incomeType": "REALIZED_PNL", "startTime": 1_800_000, "endTime": NOW_MS, "limit": 1000}),
        ("fapiPrivateGetIncome", {"incomeType": "COMMISSION", "startTime": 1_800_000, "endTime": NOW_MS, "limit": 1000}),
        ("fapiPrivateGetIncome", {"incomeType": "FUNDING_FEE", "startTime": 1_800_000, "endTime": NOW_MS, "limit": 1000}),
    ]
    assert len(owners) == 1 and owners[0].closed is True


def test_binance_empty_funding_wallet_is_zero_not_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Treat Binance's successful empty funding-asset list as a zero balance."""

    client = FakeCcxtClient({
        "fapiPrivateV3GetBalance": [{
            "asset": "USDT",
            "balance": "500",
            "maxWithdrawAmount": "420",
        }],
        "sapiPostAssetGetFundingAsset": [],
        "fapiPrivateGetIncome": [],
    })
    owners = _install_ccxt_client(monkeypatch, client)
    monkeypatch.setattr(exchanges, "_now_ms", lambda: NOW_MS)

    snapshot = exchanges.collect_readonly_snapshot(_user(exchange="binance"), 1_800_000, NOW_MS)

    assert snapshot["complete"] is True
    assert snapshot["account_balances"]["destination"] == {
        "label": "Funding Wallet",
        "balance": "0",
        "available": True,
        "withdrawable": "0",
        "asset": "USDT",
    }
    assert len(owners) == 1 and owners[0].closed is True


def test_binance_snapshot_exposes_only_safe_transfer_permissions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Expose Binance transfer flags without returning unrelated API-key metadata."""

    class PermissionClient(FakeCcxtClient):
        """Add the Binance API-restrictions read to the standard fixture client."""

        def sapiGetAccountApiRestrictions(self, params: dict[str, Any]) -> Any:
            """Return configured API permission flags."""

            return self._call("sapiGetAccountApiRestrictions", params)

    client = PermissionClient({
        "fapiPrivateV3GetBalance": [{
            "asset": "USDT",
            "balance": "500",
            "maxWithdrawAmount": "420",
        }],
        "sapiPostAssetGetFundingAsset": [],
        "sapiGetAccountApiRestrictions": {
            "enableReading": True,
            "enableFutures": True,
            "enableInternalTransfer": False,
            "permitsUniversalTransfer": False,
            "enableWithdrawals": False,
            "secretMetadata": "must-not-escape",
        },
        "fapiPrivateGetIncome": [],
    })
    _install_ccxt_client(monkeypatch, client)
    monkeypatch.setattr(exchanges, "_now_ms", lambda: NOW_MS)

    snapshot = exchanges.collect_readonly_snapshot(_user(exchange="binance"), 1_800_000, NOW_MS)

    assert snapshot["transfer_permissions"] == {
        "internal_transfer": False,
        "universal_transfer": False,
    }
    assert "secretMetadata" not in json.dumps(snapshot)


def test_bitget_classic_snapshot_detects_fallback_and_uses_max_transfer_out(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Detect Classic mode and normalize account bills without UTA reads."""

    client = FakeCcxtClient({
        "privateUtaGetV3AccountSettings": RuntimeError("not a UTA account"),
        "privateMixGetV2MixAccountAccounts": {
            "code": "00000",
            "data": [{
                "marginCoin": "USDT",
                "available": "80",
                "accountEquity": "95",
                "maxTransferOut": "75.5",
            }],
        },
        "privateSpotGetV2SpotAccountAssets": {
            "code": "00000",
            "data": [{"coin": "USDT", "available": "14", "frozen": "2", "locked": "1"}],
        },
        "privateMixGetV2MixAccountBill": {
            "code": "00000",
            "data": {
                "bills": [
                    {"billId": "1", "businessType": "close_long", "amount": "6", "fee": "-0.1", "symbol": "BTCUSDT", "cTime": "1900000"},
                    {"billId": "2", "businessType": "contract_settle_fee", "amount": "-0.25", "fee": "0", "symbol": "BTCUSDT", "cTime": "1900001"},
                ],
                "endId": "2",
            },
        },
    })
    owners = _install_ccxt_client(monkeypatch, client)
    monkeypatch.setattr(exchanges, "_now_ms", lambda: NOW_MS)

    snapshot = exchanges.collect_readonly_snapshot(_user(exchange="bitget"), 1_800_000, NOW_MS)

    assert snapshot["complete"] is True
    assert snapshot["account_mode"] == "classic"
    assert snapshot["account"]["withdrawable"] == "75.5"
    assert snapshot["realized_net_pnl"] == "5.65"
    assert snapshot["account_balances"] == {
        "source": {
            "label": "Classic Futures",
            "balance": "95",
            "available": True,
            "withdrawable": "75.5",
            "asset": "USDT",
        },
        "destination": {
            "label": "Spot",
            "balance": "17",
            "available": True,
            "withdrawable": "14",
            "asset": "USDT",
        },
        "max_transferable": "75.5",
    }
    assert client.calls == [
        ("privateUtaGetV3AccountSettings", {}),
        ("privateMixGetV2MixAccountAccounts", {"productType": "USDT-FUTURES"}),
        ("privateSpotGetV2SpotAccountAssets", {"coin": "USDT"}),
        (
            "privateMixGetV2MixAccountBill",
            {"productType": "USDT-FUTURES", "startTime": 1_800_000, "endTime": NOW_MS, "limit": "100"},
        ),
    ]
    assert len(owners) == 1 and owners[0].closed is True


def test_bitget_uta_snapshot_uses_max_transferable_and_financial_records(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Detect UTA mode and use only UTA balance, maximum, and ledger reads."""

    client = FakeCcxtClient({
        "privateUtaGetV3AccountSettings": {"code": "00000", "data": {"accountMode": "unified"}},
        "privateUtaGetV3AccountAssets": {
            "code": "00000",
            "data": {
                "accountEquity": "120",
                "assets": [{"coin": "USDT", "balance": "100", "equity": "110", "available": "90"}],
            },
        },
        "privateUtaGetV3AccountMaxTransferable": {
            "code": "00000",
            "data": {"coin": "USDT", "maxTransfer": "85", "borrowMaxTransfer": "100"},
        },
        "privateUtaGetV3AccountFundingAssets": {
            "code": "00000",
            "data": [{"coin": "USDT", "available": "30", "frozen": "2", "balance": "32"}],
        },
        "privateUtaGetV3AccountFinancialRecords": {
            "code": "00000",
            "data": {
                "list": [
                    {"id": "1", "type": "CLOSE_LONG", "amount": "5", "fee": "-0.1", "symbol": "BTCUSDT", "ts": "1900000"},
                    {"id": "2", "type": "CONTRACT_MAIN_SETTLE_FEE_USER_OUT", "amount": "0.2", "fee": "0", "symbol": "BTCUSDT", "ts": "1900001"},
                ],
                "cursor": "",
            },
        },
    })
    owners = _install_ccxt_client(monkeypatch, client)
    monkeypatch.setattr(exchanges, "_now_ms", lambda: NOW_MS)

    snapshot = exchanges.collect_readonly_snapshot(_user(exchange="bitget"), 1_800_000, NOW_MS)

    assert snapshot["complete"] is True
    assert snapshot["account_mode"] == "uta"
    assert snapshot["account"]["withdrawable"] == "85"
    assert snapshot["funding"]["events"][0]["amount"] == "-0.2"
    assert snapshot["realized_net_pnl"] == "4.7"
    assert snapshot["account_balances"] == {
        "source": {
            "label": "UTA",
            "balance": "100",
            "available": True,
            "withdrawable": "85",
            "asset": "USDT",
        },
        "destination": {
            "label": "Funding",
            "balance": "32",
            "available": True,
            "withdrawable": "30",
            "asset": "USDT",
        },
        "max_transferable": "85",
    }
    assert client.calls == [
        ("privateUtaGetV3AccountSettings", {}),
        ("privateUtaGetV3AccountAssets", {}),
        ("privateUtaGetV3AccountMaxTransferable", {"coin": "USDT"}),
        ("privateUtaGetV3AccountFundingAssets", {"coin": "USDT"}),
        (
            "privateUtaGetV3AccountFinancialRecords",
            {
                "category": "USDT-FUTURES",
                "coin": "USDT",
                "startTime": 1_800_000,
                "endTime": NOW_MS,
                "limit": "100",
            },
        ),
    ]
    assert len(owners) == 1 and owners[0].closed is True


def test_bitget_max_transferable_ccxt_fallback_is_one_fixed_get() -> None:
    """Use a fixed GET request when pinned CCXT lacks the new implicit method."""

    calls: list[tuple[Any, ...]] = []

    class RequestOnlyClient:
        """Expose only CCXT's low-level request method for the new endpoint."""

        def request(self, *args: Any) -> dict[str, Any]:
            """Capture the complete fixed request tuple."""

            calls.append(args)
            return {"code": "00000", "data": {"coin": "USDT", "maxTransfer": "1"}}

    result = exchanges._bitget_max_transferable(RequestOnlyClient(), "USDT")

    assert result["code"] == "00000"
    assert calls == [(
        "v3/account/max-transferable",
        ["private", "uta"],
        "GET",
        {"coin": "USDT"},
        {"cost": 1},
    )]


def test_optional_target_balance_failure_does_not_fail_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep a valid source snapshot usable when Bitget cannot expose Spot."""

    client = FakeCcxtClient({
        "privateUtaGetV3AccountSettings": RuntimeError("not a UTA account"),
        "privateMixGetV2MixAccountAccounts": {
            "code": "00000",
            "data": [{
                "marginCoin": "USDT",
                "available": "8",
                "accountEquity": "9",
                "maxTransferOut": "7",
            }],
        },
        "privateSpotGetV2SpotAccountAssets": PermissionError("endpoint unavailable"),
        "privateMixGetV2MixAccountBill": {
            "code": "00000",
            "data": {"bills": [], "endId": ""},
        },
    })
    owners = _install_ccxt_client(monkeypatch, client)
    monkeypatch.setattr(exchanges, "_now_ms", lambda: NOW_MS)

    snapshot = exchanges.collect_readonly_snapshot(_user(exchange="bitget"), 1_800_000, NOW_MS)

    assert snapshot["complete"] is True
    assert snapshot["account_balances"]["destination"] == {
        "label": "Spot",
        "balance": None,
        "available": False,
        "withdrawable": None,
        "asset": "USDT",
    }
    assert client.calls == [
        ("privateUtaGetV3AccountSettings", {}),
        ("privateMixGetV2MixAccountAccounts", {"productType": "USDT-FUTURES"}),
        ("privateSpotGetV2SpotAccountAssets", {"coin": "USDT"}),
        (
            "privateMixGetV2MixAccountBill",
            {"productType": "USDT-FUTURES", "startTime": 1_800_000, "endTime": NOW_MS, "limit": "100"},
        ),
    ]
    assert len(owners) == 1 and owners[0].closed is True


def test_bybit_transfer_permission_failure_keeps_preview_snapshot_complete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep Wallet/History evaluation usable when Bybit transfer reads are forbidden."""

    client = FakeCcxtClient({
        "privateGetV5AssetTransferQueryTransferCoinList": PermissionError("transfer denied"),
        "privateGetV5AssetTransferQueryAccountCoinBalance": PermissionError("transfer denied"),
        "privateGetV5AccountWalletBalance": {
            "retCode": 0,
            "result": {
                "list": [{
                    "accountType": "UNIFIED",
                    "coin": [{"coin": "USDT", "walletBalance": "100", "equity": "100"}],
                }],
            },
        },
        "privateGetV5AssetTransferQueryAccountCoinsBalance": PermissionError("funding denied"),
        "privateGetV5AccountTransactionLog": {
            "retCode": 0,
            "result": {"list": [], "nextPageCursor": ""},
        },
    })
    _install_ccxt_client(monkeypatch, client)
    monkeypatch.setattr(exchanges, "_now_ms", lambda: NOW_MS)

    snapshot = exchanges.collect_readonly_snapshot(_user(exchange="bybit"), 1_800_000, NOW_MS)

    assert snapshot["complete"] is True
    assert snapshot["account"]["balance"] == "100"
    assert snapshot["account"]["withdrawable"] == "0"
    assert snapshot["account_balances"]["max_transferable"] is None
    assert snapshot["transfer_permissions"] == {
        "internal_transfer": False,
        "reason": "Bybit API key does not permit internal account transfers. Enable Account Transfer permission; withdrawals are not required.",
    }


def test_native_snapshot_fails_closed_on_unknown_shape_and_still_closes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject an unknown exchange envelope without leaking its owned client."""

    client = FakeCcxtClient({
        "privateGetV5AssetTransferQueryTransferCoinList": PermissionError("transfer denied"),
        "privateGetV5AssetTransferQueryAccountCoinBalance": PermissionError("transfer denied"),
        "privateGetV5AccountWalletBalance": {"retCode": 0, "result": {"unexpected": []}},
    })
    owners = _install_ccxt_client(monkeypatch, client)
    monkeypatch.setattr(exchanges, "_now_ms", lambda: NOW_MS)

    snapshot = exchanges.collect_readonly_snapshot(_user(exchange="bybit"), 1_800_000, NOW_MS)

    assert snapshot["complete"] is False
    assert snapshot["errors"][0]["code"] == "invalid_response"
    assert len(owners) == 1 and owners[0].closed is True


def test_unsupported_exchange_returns_capability_without_call(monkeypatch: pytest.MonkeyPatch) -> None:
    """Return a clear read-only unsupported result instead of raising."""

    def unexpected_call(payload: dict[str, Any], *, timeout_s: float) -> Any:
        """Fail if an unsupported exchange reaches Hyperliquid."""

        raise AssertionError("Hyperliquid helper must not be called")

    monkeypatch.setattr(exchanges, "hyperliquid_info_post", unexpected_call)
    snapshot = exchanges.collect_readonly_snapshot(_user(exchange="kraken"), 1, 2)
    assert snapshot["capability"] == {
        "exchange": "kraken",
        "supported": False,
        "read_only": True,
        "writes_available": False,
        "reason": "Exchange has no read-only Profit Sweep snapshot adapter",
    }
    assert snapshot["account_balances"] == {
        "source": {
            "label": None,
            "balance": None,
            "available": False,
            "asset": "USDT",
            "withdrawable": None,
        },
        "destination": {
            "label": None,
            "balance": None,
            "available": False,
            "asset": "USDT",
        },
        "max_transferable": None,
    }
    assert snapshot["complete"] is False
    assert snapshot["errors"][0]["code"] == "unsupported_exchange"
