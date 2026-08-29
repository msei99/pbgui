"""Offline contract tests for profit-sweep exchange write adapters."""

from __future__ import annotations

import json
from copy import deepcopy
from types import SimpleNamespace
from typing import Any

import ccxt
import pytest

import profit_sweep_transfers as transfers


WALLET = "0x" + "a" * 40
VAULT = "0x" + "b" * 40
LEADER = "0x" + "c" * 40
PRIVATE_KEY = "MASTER-OR-AGENT-PRIVATE-KEY-MUST-NOT-ESCAPE"
NOW_MS = 1_800_000_000_000
BYBIT_ID = "123e4567-e89b-12d3-a456-426614174000"


def _user(exchange: str, *, vault: bool = False) -> SimpleNamespace:
    """Build a synthetic server-side exchange user."""

    return SimpleNamespace(
        name="fixture-user",
        exchange=exchange,
        key="API-KEY-MUST-NOT-ESCAPE",
        secret="API-SECRET-MUST-NOT-ESCAPE",
        passphrase="PASSPHRASE-MUST-NOT-ESCAPE",
        wallet_address=VAULT if vault else WALLET,
        private_key=PRIVATE_KEY if exchange == "hyperliquid" else None,
        is_vault=vault,
    )


def _snapshot(exchange: str, *, mode: str | None = None, vault: bool = False) -> dict[str, Any]:
    """Build a complete deterministic adapter snapshot."""

    snapshot: dict[str, Any] = {
        "complete": True,
        "exchange": exchange,
        "collected_at_ms": NOW_MS,
        "asset": {"symbol": "USDC" if exchange == "hyperliquid" else "USDT"},
    }
    if exchange == "hyperliquid":
        snapshot.update({
            "account_kind": "vault" if vault else "normal",
            "account": {"mode": "standard_manual"},
            "asset": {"symbol": "USDC", "token_id": "0xcanonical-usdc", "size_decimals": 6},
        })
        if vault:
            snapshot.update({
                "vault": {"address": VAULT},
                "leader": {
                    "address": LEADER,
                    "account_mode": "standard_manual",
                    "agent": {"relationship_valid": True},
                },
            })
    elif exchange == "bitget":
        snapshot["account_mode"] = mode
    return snapshot


class FakeClient:
    """Capture allowlisted implicit-method calls without network access."""

    def __init__(self, responses: dict[str, Any] | None = None) -> None:
        """Initialize response fixtures and call history."""

        self.responses = responses or {}
        self.calls: list[tuple[str, Any]] = []

    def _call(self, method: str, payload: Any) -> Any:
        """Capture one method and return or raise its configured fixture."""

        self.calls.append((method, deepcopy(payload)))
        response = self.responses.get(method, {})
        if isinstance(response, Exception):
            raise response
        return deepcopy(response)

    def sign_l1_action(
        self,
        action: dict[str, Any],
        nonce: int,
        vault_address: str | None = None,
    ) -> dict[str, str]:
        """Return a fake signature after capturing exact action and nonce."""

        args = (deepcopy(action), nonce, vault_address) if vault_address is not None else (deepcopy(action), nonce)
        self.calls.append(("sign_l1_action", args))
        return {"r": "0x1", "s": "0x2", "v": "0x1b", "secret_source": PRIVATE_KEY}

    def privatePostExchange(self, payload: dict[str, Any]) -> Any:
        """Capture a Hyperliquid exchange submission."""

        return self._call("privatePostExchange", payload)

    def publicPostInfo(self, payload: dict[str, Any]) -> Any:
        """Capture a Hyperliquid fixed reconciliation query."""

        return self._call("publicPostInfo", payload)

    def privatePostV5AssetTransferInterTransfer(self, payload: dict[str, Any]) -> Any:
        """Capture a Bybit V5 internal transfer."""

        return self._call("privatePostV5AssetTransferInterTransfer", payload)

    def privateGetV5AssetTransferQueryInterTransferList(self, payload: dict[str, Any]) -> Any:
        """Capture a Bybit V5 transfer-ID query."""

        return self._call("privateGetV5AssetTransferQueryInterTransferList", payload)

    def sapiPostAssetTransfer(self, payload: dict[str, Any]) -> Any:
        """Capture a Binance universal transfer."""

        return self._call("sapiPostAssetTransfer", payload)

    def sapiGetAssetTransfer(self, payload: dict[str, Any]) -> Any:
        """Capture a Binance transfer-history query."""

        return self._call("sapiGetAssetTransfer", payload)

    def privateSpotPostV2SpotWalletTransfer(self, payload: dict[str, Any]) -> Any:
        """Capture a Bitget Classic wallet transfer."""

        return self._call("privateSpotPostV2SpotWalletTransfer", payload)

    def privateSpotGetV2SpotAccountTransferRecords(self, payload: dict[str, Any]) -> Any:
        """Capture a Bitget Classic transfer-record query."""

        return self._call("privateSpotGetV2SpotAccountTransferRecords", payload)

    def privateUtaPostV3AccountTransfer(self, payload: dict[str, Any]) -> Any:
        """Capture a Bitget UTA account transfer."""

        return self._call("privateUtaPostV3AccountTransfer", payload)

    def privateUtaGetV3AccountFinancialRecords(self, payload: dict[str, Any]) -> Any:
        """Capture a Bitget UTA financial-record query."""

        return self._call("privateUtaGetV3AccountFinancialRecords", payload)


def _install_client(
    monkeypatch: pytest.MonkeyPatch,
    client: FakeClient,
) -> list[SimpleNamespace]:
    """Install an Exchange-compatible owner factory and return created owners."""

    owners: list[SimpleNamespace] = []

    class FakeExchange:
        """Own one supplied fake client and expose close state."""

        def __init__(self, exchange: str, user: Any) -> None:
            """Record constructor inputs without inspecting credentials."""

            self.exchange = exchange
            self.user = user
            self.instance: FakeClient | None = None
            self.closed = False
            owners.append(self)  # type: ignore[arg-type]

        def connect(self) -> None:
            """Attach the supplied offline client."""

            self.instance = client

        def close(self) -> None:
            """Record deterministic owner cleanup."""

            self.closed = True

    monkeypatch.setattr(transfers, "Exchange", FakeExchange)
    return owners


def test_transfer_capability_distinguishes_bitget_modes_and_hyperliquid_gate() -> None:
    """Expose only routes allowed by the snapshotted account mode."""

    classic = transfers.transfer_capability(_user("bitget"), _snapshot("bitget", mode="classic"))
    uta = transfers.transfer_capability(_user("bitget"), _snapshot("bitget", mode="uta"))
    unified_hyperliquid = _snapshot("hyperliquid")
    unified_hyperliquid["account"]["mode"] = "unified"

    assert classic["adapter"] == "bitget_classic"
    assert classic["routes"] == ["usdt_futures_to_spot"]
    assert uta["adapter"] == "bitget_uta"
    assert uta["routes"] == ["uta_to_spot"]
    assert transfers.transfer_capability(_user("hyperliquid"), unified_hyperliquid)["supported"] is False


def test_bybit_capability_explains_missing_account_transfer_permission() -> None:
    """Keep read-only support while blocking writes without Bybit Account Transfer permission."""

    snapshot = _snapshot("bybit")
    snapshot["transfer_permissions"] = {"internal_transfer": False}

    capability = transfers.transfer_capability(_user("bybit"), snapshot)

    assert capability["supported"] is True
    assert capability["writes_available"] is False
    assert "Account Transfer permission" in capability["reason"]

    with pytest.raises(transfers.TransferRequestError, match="Account Transfer permission"):
        transfers.prepare_transfer(
            _user("bybit"),
            operation_id=BYBIT_ID,
            amount="1",
            asset="USDT",
            route="unified_to_fund",
            snapshot=snapshot,
        )


def test_prepare_transfer_requires_snapshotted_asset() -> None:
    """Reject a supported adapter asset when it differs from the authoritative snapshot."""

    snapshot = _snapshot("bybit")
    snapshot["asset"]["symbol"] = "USDC"

    with pytest.raises(transfers.TransferRequestError, match="snapshotted settlement asset"):
        transfers.prepare_transfer(
            _user("bybit"),
            operation_id=BYBIT_ID,
            amount="1",
            asset="USDT",
            route="unified_to_fund",
            snapshot=snapshot,
        )


def test_vault_capability_allows_main_perps_route_for_unified_leader() -> None:
    """Allow Vault withdrawals while withholding only the incompatible Spot leg."""

    user = _user("hyperliquid", vault=True)
    snapshot = _snapshot("hyperliquid", vault=True)
    snapshot["leader"]["account_mode"] = "unified"

    capability = transfers.transfer_capability(user, snapshot)

    assert capability["supported"] is True
    assert capability["writes_available"] is True
    assert capability["routes"] == ["vault_to_main_perps"]
    descriptor = transfers.prepare_transfer(
        user,
        operation_id="unified-vault-test",
        amount="1",
        asset="USDC",
        route="vault_to_main_perps",
        snapshot=snapshot,
        nonce=1000,
    )
    assert descriptor["request"]["action"]["isDeposit"] is False
    with pytest.raises(transfers.TransferRequestError, match="Standard/Manual"):
        transfers.prepare_transfer(
            user,
            operation_id="unified-vault-spot",
            amount="1",
            asset="USDC",
            route="main_perps_to_spot",
            snapshot=snapshot,
            nonce=1001,
        )


def test_binance_capability_explains_missing_universal_transfer_permission() -> None:
    """Block Binance writes with actionable API-key permission guidance."""

    snapshot = _snapshot("binance")
    snapshot["transfer_permissions"] = {
        "internal_transfer": False,
        "universal_transfer": False,
    }

    capability = transfers.transfer_capability(_user("binance"), snapshot)

    assert capability["supported"] is True
    assert capability["writes_available"] is False
    assert capability["reason"] == transfers.BINANCE_TRANSFER_PERMISSION_REASON
    assert "Withdrawals are not required" in capability["reason"]


def test_prepare_hyperliquid_agent_uses_canonical_token_exact_nonce_and_own_wallet() -> None:
    """Build the fixed agentSendAsset action and keep credentials out of JSON."""

    descriptor = transfers.prepare_transfer(
        _user("hyperliquid"),
        operation_id="hl-operation-1",
        amount="5.1234569",
        asset="USDC",
        route="perp_to_spot",
        snapshot=_snapshot("hyperliquid"),
        nonce=1787932800123,
    )

    assert descriptor["request"] == {
        "method": "privatePostExchange",
        "action": {
            "type": "agentSendAsset",
            "destination": WALLET,
            "sourceDex": "",
            "destinationDex": "spot",
            "token": "USDC:0xcanonical-usdc",
            "amount": "5.123456",
            "fromSubAccount": "",
            "nonce": 1787932800123,
        },
        "nonce": 1787932800123,
    }
    encoded = json.dumps(descriptor)
    assert PRIVATE_KEY not in encoded
    assert "private_key" not in encoded
    assert descriptor["destination"] == WALLET


def test_submit_hyperliquid_signs_exact_action_closes_client_and_redacts_signature(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sign only at submission and never return signature or private material."""

    user = _user("hyperliquid")
    descriptor = transfers.prepare_transfer(
        user,
        operation_id="hl-operation-2",
        amount="5",
        asset="USDC",
        route="perp_to_spot",
        snapshot=_snapshot("hyperliquid"),
        nonce=1001,
    )
    client = FakeClient()
    owners = _install_client(monkeypatch, client)
    posted: list[dict[str, Any]] = []
    monkeypatch.setattr(
        transfers,
        "_post_hyperliquid_exchange",
        lambda payload: posted.append(deepcopy(payload)) or {"status": "ok", "response": {"type": "default"}},
    )

    result = transfers.submit_transfer(user, descriptor)

    action = descriptor["request"]["action"]
    assert client.calls == [("sign_l1_action", (action, 1001))]
    assert posted == [{
        "action": action,
        "nonce": 1001,
        "signature": {"r": "0x1", "s": "0x2", "v": "0x1b", "secret_source": PRIVATE_KEY},
    }]
    assert result["status"] == "submitted"
    assert PRIVATE_KEY not in json.dumps(result)
    assert owners[0].closed is True


def test_persisted_hyperliquid_action_restores_schema_key_order(monkeypatch: pytest.MonkeyPatch) -> None:
    """Rebuild the official MessagePack key order after sorted JSON persistence."""

    user = _user("hyperliquid")
    descriptor = transfers.prepare_transfer(
        user,
        operation_id="persisted-agent-order",
        amount="1",
        asset="USDC",
        route="perp_to_spot",
        snapshot=_snapshot("hyperliquid"),
        nonce=1002,
    )
    persisted = json.loads(json.dumps(descriptor, sort_keys=True))
    client = FakeClient()
    _install_client(monkeypatch, client)
    posted: list[dict[str, Any]] = []
    monkeypatch.setattr(
        transfers,
        "_post_hyperliquid_exchange",
        lambda payload: posted.append(deepcopy(payload)) or {"status": "ok", "response": {"type": "default"}},
    )

    transfers.submit_transfer(user, persisted)

    expected_order = [
        "type",
        "destination",
        "sourceDex",
        "destinationDex",
        "token",
        "amount",
        "fromSubAccount",
        "nonce",
    ]
    assert list(client.calls[0][1][0]) == expected_order
    assert list(posted[0]["action"]) == expected_order


def test_hyperliquid_vault_uses_separate_exact_leg_descriptors() -> None:
    """Build micro-USDC vault leg 1 and optional leader-to-spot leg 2 separately."""

    user = _user("hyperliquid", vault=True)
    snapshot = _snapshot("hyperliquid", vault=True)
    leg1 = transfers.prepare_transfer(
        user,
        operation_id="vault-leg-1",
        amount="5.0000009",
        asset="USDC",
        route="vault_to_main_perps",
        snapshot=snapshot,
        nonce=2001,
    )
    leg2 = transfers.prepare_transfer(
        user,
        operation_id="vault-leg-2",
        amount="4.750001",
        asset="USDC",
        route="main_perps_to_spot",
        snapshot=snapshot,
        nonce=2002,
    )
    return_leg = transfers.prepare_transfer(
        user,
        operation_id="vault-test-return",
        amount="4.750001",
        asset="USDC",
        route="main_perps_to_vault",
        snapshot=snapshot,
        nonce=2003,
    )

    assert leg1["request"] == {
        "method": "privatePostExchange",
        "action": {"type": "vaultTransfer", "vaultAddress": VAULT, "isDeposit": False, "usd": 5_000_000},
        "nonce": 2001,
    }
    assert leg2["request"]["action"] == {
        "type": "agentSendAsset",
        "destination": LEADER,
        "sourceDex": "",
        "destinationDex": "spot",
        "token": "USDC:0xcanonical-usdc",
        "amount": "4.750001",
        "fromSubAccount": "",
        "nonce": 2002,
    }
    assert return_leg["request"] == {
        "method": "privatePostExchange",
        "action": {"type": "vaultTransfer", "vaultAddress": VAULT, "isDeposit": True, "usd": 4_750_001},
        "nonce": 2003,
    }
    assert (return_leg["source"], return_leg["destination"]) == (LEADER, VAULT)
    assert transfers.reverse_transfer_route(user, snapshot, "vault_to_main_perps") == "main_perps_to_vault"
    assert leg1["idempotency"]["value"] != leg2["idempotency"]["value"]


def test_submit_vault_transfer_uses_official_leader_signing_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use the official null outer Vault context for withdrawal and return deposit."""

    user = _user("hyperliquid", vault=True)
    descriptor = transfers.prepare_transfer(
        user,
        operation_id="vault-leg-submit",
        amount="2.25",
        asset="USDC",
        route="vault_to_main_perps",
        snapshot=_snapshot("hyperliquid", vault=True),
        nonce=3001,
    )
    return_descriptor = transfers.prepare_transfer(
        user,
        operation_id="vault-leg-return-submit",
        amount="2.25",
        asset="USDC",
        route="main_perps_to_vault",
        snapshot=_snapshot("hyperliquid", vault=True),
        nonce=3002,
    )
    client = FakeClient()
    _install_client(monkeypatch, client)
    posted: list[dict[str, Any]] = []
    monkeypatch.setattr(
        transfers,
        "_post_hyperliquid_exchange",
        lambda payload: posted.append(deepcopy(payload)) or {"status": "ok"},
    )

    transfers.submit_transfer(user, descriptor)
    transfers.submit_transfer(user, return_descriptor)

    assert all(set(item) == {"action", "nonce", "signature"} for item in posted)
    assert client.calls == [
        ("sign_l1_action", (descriptor["request"]["action"], 3001)),
        ("sign_l1_action", (return_descriptor["request"]["action"], 3002)),
    ]


def test_hyperliquid_rejection_preserves_safe_actionable_reason(monkeypatch: pytest.MonkeyPatch) -> None:
    """Persist provider guidance without exposing the rejected signed request body."""

    user = _user("hyperliquid", vault=True)
    descriptor = transfers.prepare_transfer(
        user,
        operation_id="vault-provider-error",
        amount="1",
        asset="USDC",
        route="vault_to_main_perps",
        snapshot=_snapshot("hyperliquid", vault=True),
        nonce=3004,
    )
    _install_client(monkeypatch, FakeClient())
    monkeypatch.setattr(
        transfers,
        "_post_hyperliquid_exchange",
        lambda _payload: {
            "status": "err",
            "response": "User or API Wallet 0x1234567890abcdef1234567890abcdef12345678 does not exist.",
        },
    )

    result = transfers.submit_transfer(user, descriptor)

    assert result["status"] == "failed"
    assert result["error"]["type"] == "HyperliquidError"
    assert "Regenerate or reauthorize" in result["error"]["reason"]
    assert "0x123456" not in json.dumps(result)


def test_agent_send_asset_reconciles_send_ledger_event(monkeypatch: pytest.MonkeyPatch) -> None:
    """Confirm the provider's current `send` delta for one exact own-wallet route."""

    user = _user("hyperliquid")
    descriptor = transfers.prepare_transfer(
        user,
        operation_id="agent-send-reconcile",
        amount="1",
        asset="USDC",
        route="perp_to_spot",
        snapshot=_snapshot("hyperliquid"),
        nonce=NOW_MS + 20,
    )
    action = descriptor["request"]["action"]
    client = FakeClient({
        "publicPostInfo": [{
            "time": NOW_MS + 20,
            "hash": "0xsend",
            "delta": {
                "type": "send",
                "destination": WALLET,
                "sourceDex": "",
                "destinationDex": "spot",
                "token": "USDC",
                "amount": "1.0",
                "nonce": NOW_MS + 20,
                "fee": "0",
            },
        }],
    })
    owners = _install_client(monkeypatch, client)

    result = transfers.reconcile_transfer(
        user,
        descriptor,
        {"status": "submitted", "submitted_at_ms": NOW_MS + 20},
    )

    assert result["status"] == "confirmed"
    assert result["matched_records"] == 1
    assert result["received_amount"] == "1.0"
    assert owners[0].closed is True


def test_reverse_agent_send_reconciles_action_destination_not_logical_account(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Match Spot-to-Perps by the signed own-wallet destination, not `default_perps`."""

    user = _user("hyperliquid")
    descriptor = transfers.prepare_transfer(
        user,
        operation_id="agent-send-return-reconcile",
        amount="1",
        asset="USDC",
        route="spot_to_perp",
        snapshot=_snapshot("hyperliquid"),
        nonce=NOW_MS + 21,
    )
    action = descriptor["request"]["action"]
    assert descriptor["destination"] == "default_perps"
    assert action["destination"] == WALLET
    client = FakeClient({
        "publicPostInfo": [{
            "time": NOW_MS + 21,
            "hash": "0xreturn",
            "delta": {
                "type": "send",
                "destination": WALLET,
                "sourceDex": "spot",
                "destinationDex": "",
                "token": "USDC",
                "amount": "1.0",
                "nonce": NOW_MS + 21,
            },
        }],
    })
    _install_client(monkeypatch, client)

    result = transfers.reconcile_transfer(
        user,
        descriptor,
        {"status": "submitted", "submitted_at_ms": NOW_MS + 21},
    )

    assert result["status"] == "confirmed"
    assert result["received_amount"] == "1.0"


def test_agent_send_missing_identifier_stays_pending(monkeypatch: pytest.MonkeyPatch) -> None:
    """Do not confirm an agent send when the provider omits an expected nonce."""

    user = _user("hyperliquid")
    descriptor = transfers.prepare_transfer(
        user,
        operation_id="agent-send-missing-field",
        amount="1",
        asset="USDC",
        route="perp_to_spot",
        snapshot=_snapshot("hyperliquid"),
        nonce=NOW_MS + 22,
    )
    client = FakeClient({
        "publicPostInfo": [{
            "time": NOW_MS + 22,
            "delta": {
                "type": "send",
                "destination": WALLET,
                "sourceDex": "",
                "destinationDex": "spot",
                "token": "USDC",
                "amount": "1",
            },
        }],
    })
    _install_client(monkeypatch, client)

    result = transfers.reconcile_transfer(
        user,
        descriptor,
        {"status": "unknown", "submitted_at_ms": NOW_MS + 22},
    )

    assert result["status"] == "pending"
    assert result["matched_records"] == 0


def test_vault_spot_leg_reconciles_against_leader_ledger(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Query the Leader rather than the Vault for Main-Perps-to-Spot forwarding."""

    user = _user("hyperliquid", vault=True)
    descriptor = transfers.prepare_transfer(
        user,
        operation_id="vault-spot-reconcile",
        amount="4.75",
        asset="USDC",
        route="main_perps_to_spot",
        snapshot=_snapshot("hyperliquid", vault=True),
        nonce=NOW_MS + 23,
    )
    client = FakeClient({
        "publicPostInfo": [{
            "time": NOW_MS + 23,
            "delta": {
                "type": "send",
                "destination": LEADER,
                "sourceDex": "",
                "destinationDex": "spot",
                "token": "USDC",
                "amount": "4.75",
                "nonce": NOW_MS + 23,
            },
        }],
    })
    _install_client(monkeypatch, client)

    result = transfers.reconcile_transfer(
        user,
        descriptor,
        {"status": "submitted", "submitted_at_ms": NOW_MS + 23},
    )

    assert result["status"] == "confirmed"
    assert client.calls[0][1]["user"] == LEADER


def test_browser_wallet_signature_preserves_hyperliquid_wire_fields() -> None:
    """Generate typed data and normalize the wallet signature without persisting it."""

    leader_key = "1" * 64
    client = ccxt.hyperliquid()
    leader = client.privateKeyToAddress(leader_key).lower()
    user = SimpleNamespace(
        name="vault-browser-test",
        exchange="hyperliquid",
        key="",
        secret="",
        passphrase=None,
        wallet_address=VAULT,
        private_key="2" * 64,
        is_vault=True,
    )
    snapshot = _snapshot("hyperliquid", vault=True)
    snapshot["leader"]["address"] = leader
    descriptor = transfers.prepare_transfer(
        user,
        operation_id="vault-browser-signature",
        amount="5",
        asset="USDC",
        route="vault_to_main_perps",
        snapshot=snapshot,
        nonce=NOW_MS + 10,
    )
    signing = transfers.browser_signing_request(user, descriptor, leader)
    typed_data = signing["typed_data"]
    encoded = client.eth_encode_structured_data(
        typed_data["domain"],
        {"Agent": typed_data["types"]["Agent"]},
        typed_data["message"],
    )
    signed = client.sign_message(encoded, leader_key)
    signature = (
        "0x"
        + signed["r"][2:].zfill(64)
        + signed["s"][2:].zfill(64)
        + format(signed["v"], "02x")
    )

    verified = transfers.verify_browser_signature(user, descriptor, signature, leader)

    assert typed_data["types"]["EIP712Domain"] == [
        {"name": "name", "type": "string"},
        {"name": "version", "type": "string"},
        {"name": "chainId", "type": "uint256"},
        {"name": "verifyingContract", "type": "address"},
    ]
    assert verified == {"r": signed["r"], "s": signed["s"], "v": signed["v"]}


def test_vault_reconciliation_uses_vault_ledger_and_actual_net_amount(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Confirm unique vault leg 1 by its event and return actual net USDC."""

    user = _user("hyperliquid", vault=True)
    descriptor = transfers.prepare_transfer(
        user,
        operation_id="vault-reconcile",
        amount="5",
        asset="USDC",
        route="vault_to_main_perps",
        snapshot=_snapshot("hyperliquid", vault=True),
        nonce=3002,
    )
    client = FakeClient({
        "publicPostInfo": [{
            "time": NOW_MS,
            "hash": "0xevent",
            "delta": {
                "type": "vaultWithdraw",
                "vaultAddress": VAULT,
                "requestedUsd": "5.0",
                "netWithdrawnUsd": "4.750001",
                "closingCost": "0",
            },
        }],
    })
    owners = _install_client(monkeypatch, client)

    result = transfers.reconcile_transfer(
        user,
        descriptor,
        {"status": "submitted", "submitted_at_ms": NOW_MS},
    )

    assert client.calls == [(
        "publicPostInfo",
        {
            "type": "userNonFundingLedgerUpdates",
            "user": VAULT,
            "startTime": NOW_MS - 300_000,
            "endTime": NOW_MS + 300_000,
        },
    )]
    assert result["status"] == "confirmed"
    assert result["received_amount"] == "4.750001"
    assert owners[0].closed is True


def test_vault_withdrawal_wrong_vault_or_amount_stays_pending(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reject same-window Vault withdrawals that do not identify this exact request."""

    user = _user("hyperliquid", vault=True)
    descriptor = transfers.prepare_transfer(
        user,
        operation_id="vault-wrong-withdrawal",
        amount="5",
        asset="USDC",
        route="vault_to_main_perps",
        snapshot=_snapshot("hyperliquid", vault=True),
        nonce=3005,
    )
    client = FakeClient({
        "publicPostInfo": [
            {
                "time": NOW_MS,
                "delta": {
                    "type": "vaultWithdraw",
                    "vaultAddress": "0x" + "d" * 40,
                    "requestedUsd": "5",
                    "netWithdrawnUsd": "5",
                },
            },
            {
                "time": NOW_MS,
                "delta": {
                    "type": "vaultWithdraw",
                    "vaultAddress": VAULT,
                    "requestedUsd": "6",
                    "netWithdrawnUsd": "6",
                },
            },
        ],
    })
    _install_client(monkeypatch, client)

    result = transfers.reconcile_transfer(
        user,
        descriptor,
        {"status": "unknown", "submitted_at_ms": NOW_MS},
    )

    assert result["status"] == "pending"
    assert result["matched_records"] == 0


def test_vault_return_reconciles_main_ledger_deposit_to_same_vault(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Confirm the return leg only from one exact Main-to-same-Vault deposit event."""

    user = _user("hyperliquid", vault=True)
    descriptor = transfers.prepare_transfer(
        user,
        operation_id="vault-test-return",
        amount="4.750001",
        asset="USDC",
        route="main_perps_to_vault",
        snapshot=_snapshot("hyperliquid", vault=True),
        nonce=3003,
    )
    client = FakeClient({
        "publicPostInfo": [{
            "time": NOW_MS,
            "hash": "0xdeposit",
            "delta": {"type": "vaultDeposit", "vaultAddress": VAULT, "usdc": "4.750001"},
        }],
    })
    owners = _install_client(monkeypatch, client)

    result = transfers.reconcile_transfer(
        user,
        descriptor,
        {"status": "submitted", "submitted_at_ms": NOW_MS},
    )

    assert client.calls == [(
        "publicPostInfo",
        {
            "type": "userNonFundingLedgerUpdates",
            "user": LEADER,
            "startTime": NOW_MS - 300_000,
            "endTime": NOW_MS + 300_000,
        },
    )]
    assert result["status"] == "confirmed"
    assert result["received_amount"] == "4.750001"
    assert owners[0].closed is True


def test_bybit_v5_payload_uses_persisted_uuid_and_reconciles_by_transfer_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use the operation UUID for both V5 submission and exact status query."""

    user = _user("bybit")
    descriptor = transfers.prepare_transfer(
        user,
        operation_id=BYBIT_ID,
        amount="12.3400",
        asset="USDT",
        route="unified_to_fund",
        snapshot=_snapshot("bybit"),
    )
    expected = {
        "transferId": BYBIT_ID,
        "coin": "USDT",
        "amount": "12.3400",
        "fromAccountType": "UNIFIED",
        "toAccountType": "FUND",
    }
    client = FakeClient({
        "privatePostV5AssetTransferInterTransfer": {"retCode": 0, "result": {"transferId": BYBIT_ID}},
        "privateGetV5AssetTransferQueryInterTransferList": {
            "retCode": 0,
            "result": {"list": [{"transferId": BYBIT_ID, "status": "SUCCESS"}]},
        },
    })
    owners = _install_client(monkeypatch, client)

    submission = transfers.submit_transfer(user, descriptor)
    reconciliation = transfers.reconcile_transfer(user, descriptor, submission)

    assert descriptor["request"]["params"] == expected
    assert descriptor["idempotency"] == {"kind": "transferId", "value": BYBIT_ID, "replay_safe": True}
    assert client.calls == [
        ("privatePostV5AssetTransferInterTransfer", expected),
        ("privateGetV5AssetTransferQueryInterTransferList", {"transferId": BYBIT_ID}),
    ]
    assert submission["status"] == "submitted"
    assert reconciliation["status"] == "confirmed"
    assert all(owner.closed for owner in owners)


def test_bybit_requires_canonical_uuid() -> None:
    """Reject non-UUID transfer identifiers before creating a client."""

    with pytest.raises(transfers.TransferRequestError, match="UUID"):
        transfers.prepare_transfer(
            _user("bybit"),
            operation_id="browser-chosen-id",
            amount="1",
            asset="USDT",
            route="unified_to_fund",
            snapshot=_snapshot("bybit"),
        )


def test_binance_payload_has_no_idempotency_and_timeout_is_unknown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Never claim a Binance timeout is safely retryable."""

    user = _user("binance")
    descriptor = transfers.prepare_transfer(
        user,
        operation_id="binance-operation-1",
        amount="9.876500",
        asset="USDT",
        route="umfuture_to_funding",
        snapshot=_snapshot("binance"),
    )
    client = FakeClient({"sapiPostAssetTransfer": TimeoutError(f"timeout {PRIVATE_KEY}")})
    owners = _install_client(monkeypatch, client)

    result = transfers.submit_transfer(user, descriptor)

    assert descriptor["request"]["params"] == {
        "type": "UMFUTURE_FUNDING",
        "asset": "USDT",
        "amount": "9.876500",
    }
    assert descriptor["idempotency"] == {"kind": "none", "value": None, "replay_safe": False}
    assert result["status"] == "unknown"
    assert result["retry_safe"] is False
    assert PRIVATE_KEY not in json.dumps(result)
    assert owners[0].closed is True


@pytest.mark.parametrize(
    "error",
    [
        ccxt.NetworkError("network unavailable"),
        ConnectionError("connection unavailable"),
        ConnectionResetError("connection reset"),
    ],
)
def test_submission_connection_errors_are_unknown(
    monkeypatch: pytest.MonkeyPatch,
    error: Exception,
) -> None:
    """Never report a transport exception after calling the exchange as a definitive failure."""

    user = _user("binance")
    descriptor = transfers.prepare_transfer(
        user,
        operation_id="binance-connection-error",
        amount="1",
        asset="USDT",
        route="umfuture_to_funding",
        snapshot=_snapshot("binance"),
    )
    client = FakeClient({"sapiPostAssetTransfer": error})
    _install_client(monkeypatch, client)

    result = transfers.submit_transfer(user, descriptor)

    assert result["status"] == "unknown"
    assert result["retry_safe"] is False


def test_unknown_submission_is_reconciled_through_history(monkeypatch: pytest.MonkeyPatch) -> None:
    """Query history after a connection reset because the exchange may have accepted the transfer."""

    user = _user("binance")
    descriptor = transfers.prepare_transfer(
        user,
        operation_id="binance-reset-reconcile",
        amount="2",
        asset="USDT",
        route="umfuture_to_funding",
        snapshot=_snapshot("binance"),
    )
    client = FakeClient({
        "sapiPostAssetTransfer": ConnectionResetError("reset after send"),
        "sapiGetAssetTransfer": {
            "rows": [{
                "tranId": "accepted-after-reset",
                "type": "UMFUTURE_FUNDING",
                "asset": "USDT",
                "amount": "2",
                "timestamp": NOW_MS,
                "status": "CONFIRMED",
            }],
        },
    })
    _install_client(monkeypatch, client)

    submission = transfers.submit_transfer(user, descriptor)
    submission["submitted_at_ms"] = NOW_MS
    result = transfers.reconcile_transfer(user, descriptor, submission)

    assert submission["status"] == "unknown"
    assert result["status"] == "confirmed"
    assert [call[0] for call in client.calls] == ["sapiPostAssetTransfer", "sapiGetAssetTransfer"]


def test_malformed_success_response_is_unknown(monkeypatch: pytest.MonkeyPatch) -> None:
    """Require the provider transfer identifier even when the envelope claims success."""

    user = _user("bybit")
    descriptor = transfers.prepare_transfer(
        user,
        operation_id=BYBIT_ID,
        amount="1",
        asset="USDT",
        route="unified_to_fund",
        snapshot=_snapshot("bybit"),
    )
    client = FakeClient({"privatePostV5AssetTransferInterTransfer": {"retCode": 0, "result": {}}})
    _install_client(monkeypatch, client)

    result = transfers.submit_transfer(user, descriptor)

    assert result["status"] == "unknown"
    assert result["exchange_id"] is None


def test_binance_ambiguous_exact_history_is_unknown(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pause when two Binance records match type, asset, amount, and time."""

    user = _user("binance")
    descriptor = transfers.prepare_transfer(
        user,
        operation_id="binance-operation-2",
        amount="10.5",
        asset="USDT",
        route="umfuture_to_funding",
        snapshot=_snapshot("binance"),
    )
    record = {
        "type": "UMFUTURE_FUNDING",
        "asset": "USDT",
        "amount": "10.500",
        "timestamp": NOW_MS,
        "status": "CONFIRMED",
    }
    client = FakeClient({"sapiGetAssetTransfer": {"rows": [record, {**record, "tranId": 2}]}})
    owners = _install_client(monkeypatch, client)

    result = transfers.reconcile_transfer(
        user,
        descriptor,
        {"status": "unknown", "submitted_at_ms": NOW_MS},
    )

    assert result == {
        "status": "unknown",
        "matched_records": 2,
        "reason": "ambiguous_history",
        "operation_id": "binance-operation-2",
    }
    assert client.calls[0] == (
        "sapiGetAssetTransfer",
        {
            "type": "UMFUTURE_FUNDING",
            "startTime": NOW_MS - 300_000,
            "endTime": NOW_MS + 300_000,
        },
    )
    assert owners[0].closed is True


def test_binance_reconciliation_uses_submission_exchange_id(monkeypatch: pytest.MonkeyPatch) -> None:
    """Select only the Binance history row carrying the submitted transaction ID."""

    user = _user("binance")
    descriptor = transfers.prepare_transfer(
        user,
        operation_id="binance-exchange-id",
        amount="10.5",
        asset="USDT",
        route="umfuture_to_funding",
        snapshot=_snapshot("binance"),
    )
    record = {
        "type": "UMFUTURE_FUNDING",
        "asset": "USDT",
        "amount": "10.5",
        "timestamp": NOW_MS,
        "status": "CONFIRMED",
    }
    client = FakeClient({
        "sapiGetAssetTransfer": {
            "rows": [{**record, "tranId": "wrong"}, {**record, "tranId": "server-42"}],
        },
    })
    _install_client(monkeypatch, client)

    result = transfers.reconcile_transfer(
        user,
        descriptor,
        {"status": "submitted", "exchange_id": "server-42", "submitted_at_ms": NOW_MS},
    )

    assert result["status"] == "confirmed"
    assert result["matched_records"] == 1
    assert result["exchange_id"] == "server-42"


def test_bitget_classic_payload_has_client_oid_and_queries_transfer_records(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use Classic account names and the persistent clientOid."""

    user = _user("bitget")
    descriptor = transfers.prepare_transfer(
        user,
        operation_id="bitget-classic-1",
        amount="3.25",
        asset="USDT",
        route="usdt_futures_to_spot",
        snapshot=_snapshot("bitget", mode="classic"),
    )
    params = {
        "fromType": "usdt_futures",
        "toType": "spot",
        "amount": "3.25",
        "coin": "USDT",
        "clientOid": "bitget-classic-1",
    }
    client = FakeClient({
        "privateSpotPostV2SpotWalletTransfer": {"code": "00000", "data": {"clientOid": "bitget-classic-1"}},
        "privateSpotGetV2SpotAccountTransferRecords": {
            "code": "00000",
            "data": [{
                "clientOid": "bitget-classic-1",
                "status": "success",
                "coin": "USDT",
                "size": "3.25",
                "fromType": "usdt_futures",
                "toType": "spot",
            }],
        },
    })
    _install_client(monkeypatch, client)

    submission = transfers.submit_transfer(user, descriptor)
    result = transfers.reconcile_transfer(user, descriptor, submission)

    assert descriptor["adapter"] == "bitget_classic"
    assert descriptor["request"]["params"] == params
    assert client.calls == [
        ("privateSpotPostV2SpotWalletTransfer", params),
        (
            "privateSpotGetV2SpotAccountTransferRecords",
            {"clientOid": "bitget-classic-1", "coin": "USDT", "fromType": "usdt_futures"},
        ),
    ]
    assert result["status"] == "confirmed"
    assert result["received_amount"] == "3.25"


def test_bitget_exchange_ack_confirms_when_transfer_history_permission_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use Bitget's synchronous transfer ID when optional history read is forbidden."""

    user = _user("bitget")
    descriptor = transfers.prepare_transfer(
        user,
        operation_id="bitget-no-history",
        amount="1",
        asset="USDT",
        route="usdt_futures_to_spot",
        snapshot=_snapshot("bitget", mode="classic"),
    )
    client = FakeClient({
        "privateSpotPostV2SpotWalletTransfer": {
            "code": "00000",
            "data": {"transferId": "bitget-transfer-id"},
        },
        "privateSpotGetV2SpotAccountTransferRecords": PermissionError("history denied"),
    })
    _install_client(monkeypatch, client)

    submission = transfers.submit_transfer(user, descriptor)
    result = transfers.reconcile_transfer(user, descriptor, submission)

    assert submission["status"] == "submitted"
    assert submission["exchange_id"] == "bitget-transfer-id"
    assert result == {
        "status": "confirmed",
        "operation_id": "bitget-no-history",
        "exchange_id": "bitget-transfer-id",
        "matched_records": 0,
        "received_amount": "1",
        "reason": "exchange_acknowledged_history_permission_unavailable",
    }


def test_bitget_uta_forbids_borrow_and_ambiguous_ledger_is_unknown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Force allowBorrow=no and fail closed on duplicate UTA ledger matches."""

    user = _user("bitget")
    descriptor = transfers.prepare_transfer(
        user,
        operation_id="bitget-uta-1",
        amount="8.125",
        asset="USDT",
        route="uta_to_spot",
        snapshot=_snapshot("bitget", mode="uta"),
    )
    record = {
        "transferId": "uta-server-id",
        "businessType": "account_transfer",
        "coin": "USDT",
        "amount": "8.1250",
        "fromType": "uta",
        "toType": "spot",
        "cTime": NOW_MS,
        "status": "success",
    }
    client = FakeClient({
        "privateUtaPostV3AccountTransfer": {"code": "00000", "data": {"transferId": "uta-server-id"}},
        "privateUtaGetV3AccountFinancialRecords": {"data": {"list": [record, {**record, "id": "duplicate"}]}},
    })
    owners = _install_client(monkeypatch, client)

    submission = transfers.submit_transfer(user, descriptor)
    submission["submitted_at_ms"] = NOW_MS
    result = transfers.reconcile_transfer(user, descriptor, submission)

    assert descriptor["adapter"] == "bitget_uta"
    assert descriptor["request"]["params"] == {
        "fromType": "uta",
        "toType": "spot",
        "coin": "USDT",
        "amount": "8.125",
        "allowBorrow": "no",
    }
    assert descriptor["idempotency"]["replay_safe"] is False
    assert result["status"] == "unknown"
    assert result["reason"] == "ambiguous_history"
    assert all(owner.closed for owner in owners)


def test_bitget_uta_reconciliation_uses_submission_exchange_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ignore otherwise identical UTA records with a different provider transfer ID."""

    user = _user("bitget")
    descriptor = transfers.prepare_transfer(
        user,
        operation_id="bitget-uta-exchange-id",
        amount="8.125",
        asset="USDT",
        route="uta_to_spot",
        snapshot=_snapshot("bitget", mode="uta"),
    )
    record = {
        "businessType": "account_transfer",
        "coin": "USDT",
        "amount": "8.125",
        "fromType": "uta",
        "toType": "spot",
        "cTime": NOW_MS,
        "status": "success",
    }
    client = FakeClient({
        "privateUtaGetV3AccountFinancialRecords": {
            "data": {
                "list": [
                    {**record, "transferId": "wrong"},
                    {**record, "transferId": "uta-server-id"},
                ],
            },
        },
    })
    _install_client(monkeypatch, client)

    result = transfers.reconcile_transfer(
        user,
        descriptor,
        {"status": "submitted", "exchange_id": "uta-server-id", "submitted_at_ms": NOW_MS},
    )

    assert result["status"] == "confirmed"
    assert result["matched_records"] == 1
    assert result["exchange_id"] == "uta-server-id"


def test_bitget_route_must_match_snapshotted_account_mode() -> None:
    """Reject a Classic payload when the snapshot identifies a UTA account."""

    with pytest.raises(transfers.TransferRequestError, match="account mode"):
        transfers.prepare_transfer(
            _user("bitget"),
            operation_id="wrong-mode",
            amount="1",
            asset="USDT",
            route="usdt_futures_to_spot",
            snapshot=_snapshot("bitget", mode="uta"),
        )


@pytest.mark.parametrize(
    ("exchange", "mode", "operation_id", "forward_route", "reverse_route", "expected_params"),
    [
        (
            "bybit",
            None,
            BYBIT_ID,
            "unified_to_fund",
            "fund_to_unified",
            {
                "transferId": BYBIT_ID,
                "coin": "USDT",
                "amount": "1",
                "fromAccountType": "FUND",
                "toAccountType": "UNIFIED",
            },
        ),
        (
            "binance",
            None,
            "binance-back",
            "umfuture_to_funding",
            "funding_to_umfuture",
            {"type": "FUNDING_UMFUTURE", "asset": "USDT", "amount": "1"},
        ),
        (
            "bitget",
            "classic",
            "bitget-classic-back",
            "usdt_futures_to_spot",
            "spot_to_usdt_futures",
            {
                "fromType": "spot",
                "toType": "usdt_futures",
                "amount": "1",
                "coin": "USDT",
                "clientOid": "bitget-classic-back",
            },
        ),
        (
            "bitget",
            "uta",
            "bitget-uta-back",
            "uta_to_spot",
            "spot_to_uta",
            {
                "fromType": "spot",
                "toType": "uta",
                "coin": "USDT",
                "amount": "1",
                "allowBorrow": "no",
            },
        ),
    ],
)
def test_reverse_routes_use_exact_exchange_payloads(
    exchange: str,
    mode: str | None,
    operation_id: str,
    forward_route: str,
    reverse_route: str,
    expected_params: dict[str, Any],
) -> None:
    """Derive and seal each non-Hyperliquid reverse account direction."""

    user = _user(exchange)
    snapshot = _snapshot(exchange, mode=mode)

    assert transfers.reverse_transfer_route(user, snapshot, forward_route) == reverse_route
    descriptor = transfers.prepare_transfer(
        user,
        operation_id=operation_id,
        amount="1",
        asset="USDT",
        route=reverse_route,
        snapshot=snapshot,
    )

    assert descriptor["request"]["params"] == expected_params
    assert (descriptor["source"], descriptor["destination"]) == {
        "bybit": ("FUND", "UNIFIED"),
        "binance": ("FUNDING", "UMFUTURE"),
        "classic": ("spot", "usdt_futures"),
        "uta": ("spot", "uta"),
    }[mode or exchange]


def test_hyperliquid_reverse_uses_own_spot_to_default_perps_exact_action() -> None:
    """Reverse only through own-wallet agentSendAsset with spot as the source DEX."""

    user = _user("hyperliquid")
    snapshot = _snapshot("hyperliquid")
    assert transfers.reverse_transfer_route(user, snapshot, "perp_to_spot") == "spot_to_perp"

    descriptor = transfers.prepare_transfer(
        user,
        operation_id="hyperliquid-back",
        amount="1",
        asset="USDC",
        route="spot_to_perp",
        snapshot=snapshot,
        nonce=4001,
    )

    assert descriptor["request"] == {
        "method": "privatePostExchange",
        "action": {
            "type": "agentSendAsset",
            "destination": WALLET,
            "sourceDex": "spot",
            "destinationDex": "",
            "token": "USDC:0xcanonical-usdc",
            "amount": "1",
            "fromSubAccount": "",
            "nonce": 4001,
        },
        "nonce": 4001,
    }
    assert (descriptor["source"], descriptor["destination"]) == ("spot", "default_perps")


def test_reverse_descriptor_direction_tampering_never_reaches_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject a recomputed fingerprint when top-level accounts contradict the payload."""

    user = _user("binance")
    descriptor = transfers.prepare_transfer(
        user,
        operation_id="strict-direction",
        amount="1",
        asset="USDT",
        route="funding_to_umfuture",
        snapshot=_snapshot("binance"),
    )
    descriptor["source"] = "UMFUTURE"
    descriptor["fingerprint"] = transfers._descriptor_fingerprint(descriptor)
    client = FakeClient()
    owners = _install_client(monkeypatch, client)

    with pytest.raises(transfers.TransferRequestError, match="direction"):
        transfers.submit_transfer(user, descriptor)

    assert client.calls == []
    assert owners == []


@pytest.mark.parametrize(
    ("exchange", "route"),
    [
        ("hyperliquid", "withdraw"),
        ("bybit", "external"),
        ("binance", "spot_to_address"),
        ("bitget", "arbitrary_method"),
    ],
)
def test_unsupported_routes_are_rejected(exchange: str, route: str) -> None:
    """Reject every route outside the fixed internal-transfer allowlist."""

    snapshot = _snapshot(exchange, mode="classic" if exchange == "bitget" else None)
    with pytest.raises(transfers.TransferRequestError, match="allowlisted"):
        transfers.prepare_transfer(
            _user(exchange),
            operation_id="unsupported-route",
            amount="1",
            asset="USDC" if exchange == "hyperliquid" else "USDT",
            route=route,
            snapshot=snapshot,
            nonce=1,
        )


def test_tampered_method_and_payload_never_reach_client(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reject a recomputed descriptor that asks for any non-allowlisted method."""

    user = _user("binance")
    descriptor = transfers.prepare_transfer(
        user,
        operation_id="tamper-test",
        amount="1",
        asset="USDT",
        route="umfuture_to_funding",
        snapshot=_snapshot("binance"),
    )
    descriptor["request"] = {"method": "externalValueTransfer", "params": {"address": WALLET}}
    descriptor["fingerprint"] = transfers._descriptor_fingerprint(descriptor)
    client = FakeClient()
    owners = _install_client(monkeypatch, client)

    with pytest.raises(transfers.TransferRequestError, match="method"):
        transfers.submit_transfer(user, descriptor)

    assert client.calls == []
    assert owners == []


def test_amount_is_not_rounded_without_precision_and_rounds_down_with_precision() -> None:
    """Preserve exact decimals unless the snapshot supplies exchange precision."""

    no_precision = transfers.prepare_transfer(
        _user("binance"),
        operation_id="precision-none",
        amount="1.2345678900",
        asset="USDT",
        route="umfuture_to_funding",
        snapshot=_snapshot("binance"),
    )
    with_precision_snapshot = _snapshot("binance")
    with_precision_snapshot["amount_precision"] = 3
    rounded = transfers.prepare_transfer(
        _user("binance"),
        operation_id="precision-three",
        amount="1.2349",
        asset="USDT",
        route="umfuture_to_funding",
        snapshot=with_precision_snapshot,
    )

    assert no_precision["amount"] == "1.2345678900"
    assert rounded["amount"] == "1.234"


def test_reconciliation_timeout_is_unknown_and_client_is_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Treat an unavailable history query as unknown without generic retry."""

    user = _user("bybit")
    descriptor = transfers.prepare_transfer(
        user,
        operation_id=BYBIT_ID,
        amount="1",
        asset="USDT",
        route="unified_to_fund",
        snapshot=_snapshot("bybit"),
    )
    client = FakeClient({"privateGetV5AssetTransferQueryInterTransferList": TimeoutError("secret response")})
    owners = _install_client(monkeypatch, client)

    result = transfers.reconcile_transfer(
        user,
        descriptor,
        {"status": "unknown", "submitted_at_ms": NOW_MS},
    )

    assert result["status"] == "unknown"
    assert result["error"] == {"category": "timeout", "type": "TimeoutError"}
    assert len(client.calls) == 1
    assert owners[0].closed is True


def test_all_submission_methods_are_internal_fixed_allowlist(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure supported submissions never call an external-address method."""

    cases = [
        (_user("bybit"), BYBIT_ID, "USDT", "unified_to_fund", _snapshot("bybit")),
        (_user("binance"), "binance-allowlist", "USDT", "umfuture_to_funding", _snapshot("binance")),
        (
            _user("bitget"),
            "bitget-allowlist",
            "USDT",
            "usdt_futures_to_spot",
            _snapshot("bitget", mode="classic"),
        ),
        (_user("bitget"), "uta-allowlist", "USDT", "uta_to_spot", _snapshot("bitget", mode="uta")),
    ]
    expected_methods = {
        "privatePostV5AssetTransferInterTransfer",
        "sapiPostAssetTransfer",
        "privateSpotPostV2SpotWalletTransfer",
        "privateUtaPostV3AccountTransfer",
    }
    actual_methods: set[str] = set()
    for user, operation_id, asset, route, snapshot in cases:
        descriptor = transfers.prepare_transfer(
            user,
            operation_id=operation_id,
            amount="1",
            asset=asset,
            route=route,
            snapshot=snapshot,
        )
        method = descriptor["request"]["method"]
        client = FakeClient({
            method: {"retCode": 0, "result": {"transferId": BYBIT_ID}}
            if user.exchange == "bybit"
            else {"tranId": 1}
            if user.exchange == "binance"
            else {"code": "00000", "data": {}},
        })
        _install_client(monkeypatch, client)
        transfers.submit_transfer(user, descriptor)
        actual_methods.add(client.calls[0][0])

    assert actual_methods == expected_methods
    assert all("address" not in method.lower() for method in actual_methods)
    assert all("withdraw" not in method.lower() for method in actual_methods)
