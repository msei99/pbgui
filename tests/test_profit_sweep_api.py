"""Offline contract tests for the authenticated Profit Sweep API."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from typing import Any
import uuid

import ccxt
import pytest
from fastapi import HTTPException
from starlette.requests import Request

import api.profit_sweep as profit_sweep_api
from api.auth import require_auth
from profit_sweep import ProfitSweepStore
import profit_sweep_exchanges


ROOT = Path(__file__).resolve().parents[1]
PRIVATE_KEY = "PRIVATE-KEY-MUST-NOT-ESCAPE"
API_SECRET = "API-SECRET-MUST-NOT-ESCAPE"
API_KEY = "API-KEY-MUST-NOT-ESCAPE"
SESSION_TOKEN = "SESSION-TOKEN-MUST-NOT-ESCAPE"
LEADER_PRIVATE_KEY = "1" * 64
LEADER_ADDRESS = ccxt.hyperliquid().privateKeyToAddress(LEADER_PRIVATE_KEY).lower()


class SyntheticUsers:
    """Provide the iteration and lookup contract used by the API."""

    def __init__(self, users: list[SimpleNamespace]) -> None:
        """Store a stable synthetic exchange-user catalog."""

        self.users = users

    def __iter__(self):
        """Iterate over synthetic exchange users."""

        return iter(self.users)

    def find_user(self, name: str) -> SimpleNamespace | None:
        """Return one synthetic user by name."""

        return next((user for user in self.users if user.name == name), None)


def _user(name: str, *, exchange: str = "hyperliquid", is_vault: bool = False) -> SimpleNamespace:
    """Build a synthetic exchange user containing secret sentinels."""

    return SimpleNamespace(
        name=name,
        exchange=exchange,
        is_vault=is_vault,
        wallet_address="0x" + ("b" if is_vault else "a") * 40,
        key=API_KEY,
        private_key=PRIVATE_KEY,
        secret=API_SECRET,
        passphrase="PASSPHRASE-MUST-NOT-ESCAPE",
        extra={},
    )


def _normal_snapshot(user_name: str = "alice") -> dict[str, Any]:
    """Return a complete synthetic Hyperliquid standard-account snapshot."""

    return {
        "schema_version": 1,
        "read_only": True,
        "complete": True,
        "errors": [],
        "exchange": "hyperliquid",
        "collected_at_ms": int(profit_sweep_api.time.time() * 1000),
        "history": {"fresh": True},
        "user_name": user_name,
        "account_kind": "normal",
        "account": {
            "balance": "1000",
            "account_value": "1000",
            "withdrawable": "500",
            "mode": "standard_manual",
        },
        "asset": {"symbol": "USDC", "token_id": "0xcanonical-usdc", "size_decimals": 6},
        "positions": [],
        "orders": [],
        "fills": {
            "events": [{
                "id": f"fill-{user_name}",
                "time_ms": 1_000,
                "coin": "BTC",
                "trade_id": "1",
                "closed_pnl": "100",
                "fee": "0",
            }],
            "complete": True,
        },
        "funding": {"events": [], "complete": True},
        "vault_leader_commissions": {"attributions": []},
    }


def _vault_snapshot(user_name: str = "vault") -> dict[str, Any]:
    """Return a complete synthetic Hyperliquid legacy-vault snapshot."""

    return {
        "schema_version": 1,
        "read_only": True,
        "complete": True,
        "errors": [],
        "exchange": "hyperliquid",
        "collected_at_ms": int(profit_sweep_api.time.time() * 1000),
        "history": {"fresh": True},
        "user_name": user_name,
        "account_kind": "vault",
        "fills": {"events": [], "complete": True},
        "funding": {"events": [], "complete": True},
        "vault_leader_commissions": {
            "attributions": [{
                "status": "exact",
                "time_ms": 2_000,
                "commission_event_id": "commission-vault",
                "amount": "5",
            }],
        },
        "vault": {
            "address": "0x" + "b" * 40,
            "all_time_pnl": "100",
            "vault_equity": "500",
            "leader_fraction": "0.5",
            "max_withdrawable": "200",
            "lockup_until_ms": 0,
            "always_close_on_withdraw": False,
            "closed": False,
            "balances": {"account_value": "1000", "withdrawable": "300"},
            "positions": [],
            "orders": [],
        },
        "leader": {
            "address": LEADER_ADDRESS,
            "account_mode": "standard_manual",
            "agent": {"relationship_valid": True},
        },
        "account_balances": {
            "source": {
                "label": "Vault",
                "balance": "500",
                "available": True,
                "withdrawable": "300",
                "asset": "USDC",
            },
            "destination": {
                "main_perps": {
                    "label": "Main Perps",
                    "balance": "1000",
                    "available": True,
                    "asset": "USDC",
                },
                "main_spot": {
                    "label": "Main Spot",
                    "balance": "0",
                    "available": True,
                    "asset": "USDC",
                },
            },
            "max_transferable": "200",
        },
        "asset": {"symbol": "USDC", "token_id": "0xcanonical-usdc", "size_decimals": 6},
    }


def _request() -> Request:
    """Build a main-page request carrying only the HttpOnly-style cookie."""

    return Request({
        "type": "http",
        "scheme": "http",
        "server": ("testserver", 80),
        "path": "/api/profit-sweep/main_page",
        "headers": [(b"cookie", f"pbgui_session={SESSION_TOKEN}".encode("ascii"))],
        "query_string": b"",
    })


def _browser_signature(prepared: dict[str, Any]) -> str:
    """Sign one synthetic browser-wallet request with the fixture Leader key."""

    typed_data = prepared["signing"]["typed_data"]
    client = ccxt.hyperliquid()
    encoded = client.eth_encode_structured_data(
        typed_data["domain"],
        {"Agent": typed_data["types"]["Agent"]},
        typed_data["message"],
    )
    signature = client.sign_message(encoded, LEADER_PRIVATE_KEY)
    return "0x" + signature["r"][2:].zfill(64) + signature["s"][2:].zfill(64) + format(signature["v"], "02x")


def _test_request(*, amount: str = "1", asset: str = "USDC", operation_id: str | None = None):
    """Build one explicit idempotent manual-transfer request."""

    return profit_sweep_api.TestTransferRequest(
        amount=amount,
        asset=asset,
        operation_id=operation_id or str(uuid.uuid4()),
    )


def _create_dry_policy(store: ProfitSweepStore, user_name: str) -> dict[str, Any]:
    """Create a deterministic lifetime-baseline Dry policy."""

    return store.create_policy(
        user_name,
        "hyperliquid",
        {
            "operating_mode": "dry",
            "asset": "USDC",
            "baseline_mode": "lifetime",
            "reference_capital": "1000",
            "trigger_percent": "0",
            "sweep_percent": "100",
            "minimum_transfer_amount": "1",
            "stabilization_interval": 0,
            "periodic_interval": 60,
        },
    )


def _create_live_policy(
    store: ProfitSweepStore,
    user_name: str,
    exchange: str,
    *,
    changes: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create and activate one deterministic lifetime-baseline Live policy."""

    store.create_policy(
        user_name,
        exchange,
        {
            "operating_mode": "dry",
            "baseline_mode": "lifetime",
            "live_activation_baseline_mode": "include_dry_period",
            "reference_capital": "1000",
            "trigger_percent": "0",
            "sweep_percent": "100",
            "live_minimum_transfer_amount": "1",
            "vault_minimum_transfer_amount": "1",
            "stabilization_interval": 0,
            "periodic_interval": 60,
            **(changes or {}),
        },
    )
    return store.activate_live(user_name, "0", baseline_mode="include_dry_period")


def _binance_snapshot() -> dict[str, Any]:
    """Return a complete current Binance snapshot for sealed-adapter tests."""

    return {
        "complete": True,
        "errors": [],
        "exchange": "binance",
        "collected_at_ms": int(profit_sweep_api.time.time() * 1000),
        "history": {"fresh": True},
        "account": {"balance": "1000", "withdrawable": "500"},
        "asset": {"symbol": "USDT", "amount_precision": 8},
        "fills": {
            "events": [{
                "id": "binance-fill",
                "time_ms": 1_000,
                "closed_pnl": "100",
                "fee": "0",
            }],
        },
        "funding": {"events": []},
    }


def _bybit_snapshot() -> dict[str, Any]:
    """Return a complete current Bybit snapshot for UUID integration tests."""

    snapshot = _binance_snapshot()
    snapshot["exchange"] = "bybit"
    snapshot["fills"]["events"][0]["id"] = "bybit-fill"
    return snapshot


def _bitget_snapshot(mode: str) -> dict[str, Any]:
    """Return a complete current Bitget Classic or UTA snapshot."""

    snapshot = _binance_snapshot()
    snapshot["exchange"] = "bitget"
    snapshot["account_mode"] = mode
    snapshot["fills"]["events"][0]["id"] = f"bitget-{mode}-fill"
    return snapshot


def _assert_secret_free(value: Any) -> None:
    """Assert recursively that a response contains no credential fields or sentinels."""

    forbidden_fields = {
        "api_key",
        "apikey",
        "key",
        "secret",
        "passphrase",
        "password",
        "private_key",
        "privatekey",
        "session",
        "session_token",
        "token",
    }
    if isinstance(value, dict):
        for key, child in value.items():
            normalized = str(key).lower().replace("-", "_")
            assert normalized not in forbidden_fields
            _assert_secret_free(child)
    elif isinstance(value, list):
        for child in value:
            _assert_secret_free(child)
    elif isinstance(value, str):
        assert PRIVATE_KEY not in value
        assert API_KEY not in value
        assert API_SECRET not in value
        assert SESSION_TOKEN not in value
        assert "PASSPHRASE-MUST-NOT-ESCAPE" not in value


@pytest.fixture
def isolated_api(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    """Install an isolated store, synthetic Users, and fail-closed network stubs."""

    store = ProfitSweepStore(tmp_path / "profit-sweep" / "profit-sweep.sqlite3")
    users = SyntheticUsers([
        _user("alice"),
        _user("vault", is_vault=True),
        _user("bybit", exchange="bybit"),
        _user("binance", exchange="binance"),
        _user("bitget-classic", exchange="bitget"),
        _user("bitget-uta", exchange="bitget"),
    ])

    def unexpected_network(*_args: Any, **_kwargs: Any) -> Any:
        """Fail if a test reaches the Hyperliquid HTTP transport."""

        raise AssertionError("Profit Sweep API tests must not use the network")

    def unexpected_snapshot(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        """Require every evaluating test to install an explicit snapshot."""

        raise AssertionError("Evaluating tests must provide a synthetic snapshot")

    monkeypatch.setattr(profit_sweep_api, "_STORE", store)
    monkeypatch.setattr(profit_sweep_api, "_SCHEDULER_TASK", None)
    monkeypatch.setattr(profit_sweep_api, "_SCHEDULER_WAKE", None)
    monkeypatch.setattr(profit_sweep_api, "_STOPPING", False)
    monkeypatch.setattr(profit_sweep_api, "_EVALUATION_LOCKS", {})
    monkeypatch.setattr(profit_sweep_api, "_ACTIVE_OPERATION_TASKS", set())
    monkeypatch.setattr(profit_sweep_api, "_users", lambda: users)
    monkeypatch.setattr(profit_sweep_api, "collect_readonly_snapshot", unexpected_snapshot)
    monkeypatch.setattr(profit_sweep_api, "_log", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(profit_sweep_exchanges, "hyperliquid_info_post", unexpected_network)
    return SimpleNamespace(store=store, users=users)


def test_every_profit_sweep_route_requires_shared_auth() -> None:
    """Every Profit Sweep router route must retain require_auth."""

    assert profit_sweep_api.router.routes
    for route in profit_sweep_api.router.routes:
        dependency_calls = {dependency.call for dependency in route.dependant.dependencies}
        assert require_auth in dependency_calls, f"Missing require_auth on {route.path}"


def test_main_page_is_cookie_only_secret_free_and_non_cacheable() -> None:
    """The page must use cookie auth and emit strict cache/referrer headers."""

    class CookieOnlySession:
        """Fail if page rendering reads any session-token field."""

        def __getattr__(self, name: str) -> Any:
            """Reject session material access during page rendering."""

            raise AssertionError(f"session field accessed: {name}")

    response = profit_sweep_api.get_main_page(_request(), CookieOnlySession())
    html = response.body.decode("utf-8")

    assert response.headers["cache-control"] == "no-store"
    assert response.headers["referrer-policy"] == "no-referrer"
    assert "%%API_BASE%%" not in html
    assert "%%TOKEN%%" not in html
    assert "Authorization" not in html
    assert SESSION_TOKEN not in html
    assert PRIVATE_KEY not in html


def test_users_and_policy_crud_support_adapters_and_block_live_mode_bypass(
    isolated_api: SimpleNamespace,
) -> None:
    """List safe users, support sealed adapters, and require explicit Live activation."""

    users = profit_sweep_api.list_users(object())
    assert [item["name"] for item in users["users"]] == [
        "alice",
        "binance",
        "bitget-classic",
        "bitget-uta",
        "bybit",
        "vault",
    ]
    assert users["users"][0]["capability"]["read_only"] is True

    created = profit_sweep_api.save_policy(
        "alice",
        profit_sweep_api.PolicyRequest(policy={"operating_mode": "disabled"}),
        object(),
    )
    updated = profit_sweep_api.save_policy(
        "alice",
        profit_sweep_api.PolicyRequest(
            policy={"operating_mode": "dry"},
            expected_generation=created["generation"],
            expected_policy_fingerprint=created["policy_fingerprint"],
        ),
        object(),
    )
    assert created["policy"]["asset"] == "USDC"
    assert updated["policy"]["operating_mode"] == "dry"
    assert profit_sweep_api.get_policy("alice", object())["user_name"] == "alice"
    assert len(profit_sweep_api.list_policies(object())["policies"]) == 1

    reset = profit_sweep_api.reset_baseline(
        "alice",
        profit_sweep_api.BaselineRequest(
            cumulative_net_pnl="12.5",
            expected_policy_fingerprint=updated["policy_fingerprint"],
        ),
        object(),
    )
    assert reset["generation"] == 2
    assert profit_sweep_api.get_journal("alice", 100, object()) == {"journal": []}

    bybit = profit_sweep_api.save_policy(
        "bybit", profit_sweep_api.PolicyRequest(policy={"operating_mode": "dry"}), object()
    )
    assert bybit["policy"]["asset"] == "USDT"

    with pytest.raises(HTTPException) as live:
        profit_sweep_api.save_policy(
            "alice",
            profit_sweep_api.PolicyRequest(
                policy={"operating_mode": "live"},
                expected_generation=reset["generation"],
                expected_policy_fingerprint=reset["policy_fingerprint"],
            ),
            object(),
        )
    assert live.value.status_code == 422
    assert "activation endpoint" in str(live.value.detail)

    assert profit_sweep_api.delete_policy(
        "alice",
        profit_sweep_api.DeletePolicyRequest(
            expected_policy_fingerprint=reset["policy_fingerprint"]
        ),
        object(),
    ) == {"ok": True}
    assert [item["user_name"] for item in isolated_api.store.list_policies()] == ["bybit"]


def test_evaluate_now_preview_leaves_simulation_state_and_journal_unchanged(
    isolated_api: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Evaluate now must use commit=false for simulation state and journal."""

    _create_dry_policy(isolated_api.store, "alice")
    monkeypatch.setattr(profit_sweep_api, "collect_readonly_snapshot", lambda *_args: _normal_snapshot())
    before_state = isolated_api.store.get_policy("alice")["simulation_state"]
    before_journal = isolated_api.store.list_simulation_journal("alice")

    result = asyncio.run(profit_sweep_api.evaluate_now("alice", object()))

    assert result["read_only"] is True
    assert result["decision"]["committed"] is False
    assert result["decision"]["would_transfer"] is True
    assert isolated_api.store.get_policy("alice")["simulation_state"] == before_state
    assert isolated_api.store.list_simulation_journal("alice") == before_journal == []


def test_evaluate_now_accepts_unsaved_disabled_form_values(
    isolated_api: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Evaluate now must preview current form values without creating a policy."""

    monkeypatch.setattr(profit_sweep_api, "collect_readonly_snapshot", lambda *_args: _normal_snapshot())
    body = profit_sweep_api.PreviewRequest(policy={
        "operating_mode": "disabled",
        "baseline_mode": "lifetime",
        "reference_capital": "1000",
        "trigger_percent": "0",
        "sweep_percent": "100",
        "minimum_transfer_amount": "25",
    })

    result = asyncio.run(profit_sweep_api.evaluate_now("alice", object(), body))

    assert result["saved_policy"] is False
    assert result["decision"]["would_transfer"] is True
    assert result["decision"]["amount"] == "100"
    assert isolated_api.store.list_policies() == []


def test_evaluate_now_accepts_supported_cex_form_values(
    isolated_api: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Supported CEX users must use the same transient read-only preview path."""

    snapshot = _binance_snapshot()
    snapshot["account_balances"] = {
        "source": {"label": "USD-M Futures", "balance": "1000", "available": "500", "asset": "USDT"},
        "destination": {"label": "Funding", "balance": "20", "available": "20", "asset": "USDT"},
        "max_transferable": "500",
    }
    monkeypatch.setattr(profit_sweep_api, "collect_readonly_snapshot", lambda *_args: snapshot)
    body = profit_sweep_api.PreviewRequest(policy={
        "asset": "USDT",
        "baseline_mode": "lifetime",
        "reference_capital": "1000",
        "trigger_percent": "0",
        "sweep_percent": "100",
        "minimum_transfer_amount": "25",
    })

    result = asyncio.run(profit_sweep_api.evaluate_now("binance", object(), body))

    assert result["snapshot"]["account_balances"]["destination"]["balance"] == "20"
    assert result["decision"]["would_transfer"] is True


def test_cex_preview_uses_policy_asset_for_snapshot_and_ledger(
    isolated_api: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A USDC policy must never evaluate or transfer from the user's default USDT snapshot."""

    isolated_api.store.create_policy(
        "bybit",
        "bybit",
        {
            "operating_mode": "dry",
            "asset": "USDC",
            "baseline_mode": "lifetime",
            "trigger_percent": "0",
            "sweep_percent": "100",
        },
    )
    requested_assets: list[str] = []

    def snapshot(_user_value: Any, _since: int, _until: int, _timeout: float, asset: str) -> dict[str, Any]:
        """Return only the settlement asset explicitly requested by the policy."""

        requested_assets.append(asset)
        value = _bybit_snapshot()
        value["asset"] = {"symbol": asset, "amount_precision": 8}
        return value

    monkeypatch.setattr(profit_sweep_api, "collect_readonly_snapshot", snapshot)

    result = asyncio.run(profit_sweep_api.evaluate_now("bybit", object()))

    assert requested_assets == ["USDC"]
    assert result["snapshot"]["asset"]["symbol"] == "USDC"
    assert isolated_api.store.ledger_net_pnl("bybit", "bybit", "USDT") == "0"


def test_vault_commission_already_in_main_is_not_withdrawn_from_vault(
    isolated_api: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Leader commission income must not inflate the Vault-resident transfer due."""

    _create_dry_policy(isolated_api.store, "vault")
    monkeypatch.setattr(profit_sweep_api, "collect_readonly_snapshot", lambda *_args: _vault_snapshot())

    result = asyncio.run(profit_sweep_api.evaluate_now("vault", object()))

    assert result["decision"]["amount"] == "100"
    assert isolated_api.store.ledger_net_pnl("vault", "hyperliquid", "USDC") == "5"


def test_scheduler_commits_one_dry_decision(
    isolated_api: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A due scheduler tick must commit and journal a Dry decision."""

    _create_dry_policy(isolated_api.store, "alice")
    monkeypatch.setattr(profit_sweep_api, "collect_readonly_snapshot", lambda *_args: _normal_snapshot())
    original_evaluate = profit_sweep_api._evaluate_user
    calls: list[tuple[str, bool, bool]] = []

    async def evaluate_once(user_name: str, *, commit: bool, stabilize: bool = False) -> dict[str, Any]:
        """Run the real evaluator once, then stop the scheduler loop."""

        calls.append((user_name, commit, stabilize))
        result = await original_evaluate(user_name, commit=commit, stabilize=stabilize)
        profit_sweep_api._STOPPING = True
        return result

    monkeypatch.setattr(profit_sweep_api, "_evaluate_user", evaluate_once)
    asyncio.run(profit_sweep_api._scheduler_loop())

    state = isolated_api.store.get_policy("alice")["simulation_state"]
    journal = isolated_api.store.list_simulation_journal("alice")
    assert calls == [("alice", True, True)]
    assert state["simulated_total"] == "100"
    assert state["last_decision"] == "would_transfer"
    assert state["last_successful_scan_at"] is not None
    assert len(journal) == 1
    assert journal[0]["amount"] == "100"


def test_notify_income_updates_only_an_existing_dry_policy(
    isolated_api: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Income hints must ignore missing and disabled policies without creating state."""

    _create_dry_policy(isolated_api.store, "alice")
    isolated_api.store.create_policy("vault", "hyperliquid", {"operating_mode": "disabled"})
    wake = asyncio.Event()
    monkeypatch.setattr(profit_sweep_api, "_SCHEDULER_WAKE", wake)
    monkeypatch.setattr(profit_sweep_api.time, "time", lambda: 1_000)
    disabled_before = isolated_api.store.get_policy("vault")["simulation_state"]

    asyncio.run(profit_sweep_api.notify_income("missing"))
    asyncio.run(profit_sweep_api.notify_income("vault"))
    assert wake.is_set() is False
    asyncio.run(profit_sweep_api.notify_income("alice"))

    dry_state = isolated_api.store.get_policy("alice")["simulation_state"]
    assert dry_state["last_event_at"] == 1_000
    assert dry_state["next_run_at"] == 1_900
    assert isolated_api.store.get_policy("vault")["simulation_state"] == disabled_before
    assert [record["user_name"] for record in isolated_api.store.list_policies()] == ["alice", "vault"]
    assert wake.is_set() is True


def test_startup_shutdown_are_idempotent_without_active_submission(isolated_api: SimpleNamespace) -> None:
    """The API-owned scheduler starts once and stops repeatedly when no submission is active."""

    async def exercise() -> None:
        """Exercise repeated lifecycle calls on one event loop."""

        assert profit_sweep_api.restart_block_reason() == ""
        profit_sweep_api.startup()
        first_task = profit_sweep_api._SCHEDULER_TASK
        profit_sweep_api.startup()
        assert profit_sweep_api._SCHEDULER_TASK is first_task
        assert first_task is not None
        await profit_sweep_api.shutdown()
        await profit_sweep_api.shutdown()
        assert profit_sweep_api._SCHEDULER_TASK is None
        assert profit_sweep_api._SCHEDULER_WAKE is None
        assert profit_sweep_api.restart_block_reason() == ""

    asyncio.run(exercise())


def test_live_activation_uses_fresh_server_snapshot_and_returns_no_secrets(
    isolated_api: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The authenticated Live route owns capability, route, snapshot, and baseline inputs."""

    saved = profit_sweep_api.save_policy(
        "alice",
        profit_sweep_api.PolicyRequest(policy={
            "operating_mode": "dry",
            "baseline_mode": "lifetime",
            "live_activation_baseline_mode": "fresh",
        }),
        object(),
    )
    monkeypatch.setattr(profit_sweep_api, "collect_readonly_snapshot", lambda *_args: _normal_snapshot())

    result = asyncio.run(profit_sweep_api.activate_live(
        "alice",
        profit_sweep_api.LiveActivationRequest(
            expected_policy_fingerprint=saved["policy_fingerprint"]
        ),
        object(),
    ))

    assert result["policy"]["policy"]["operating_mode"] == "live"
    assert result["baseline_mode"] == "fresh"
    assert result["capability"]["routes"] == ["perp_to_spot"]
    assert result["snapshot"]["account"]["balance"] == "1000"
    assert "descriptor" not in result
    assert "route" not in result
    _assert_secret_free(result)


def test_vault_live_activation_uses_canonicalized_agent_transfer(
    isolated_api: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Allow automatic Vault writes after persisted action order is restored."""

    saved = profit_sweep_api.save_policy(
        "vault",
        profit_sweep_api.PolicyRequest(policy={"operating_mode": "dry", "asset": "USDC"}),
        object(),
    )
    monkeypatch.setattr(profit_sweep_api, "collect_readonly_snapshot", lambda *_args: _vault_snapshot())

    result = asyncio.run(profit_sweep_api.activate_live(
        "vault",
        profit_sweep_api.LiveActivationRequest(
            expected_policy_fingerprint=saved["policy_fingerprint"]
        ),
        object(),
    ))

    assert result["policy"]["policy"]["operating_mode"] == "live"
    assert result["capability"]["adapter"] == "hyperliquid_vault"


def test_live_activation_rejects_policy_changed_after_confirmation(
    isolated_api: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The activation transaction must match the exact policy reviewed by the user."""

    saved = profit_sweep_api.save_policy(
        "alice",
        profit_sweep_api.PolicyRequest(policy={"operating_mode": "dry"}),
        object(),
    )
    isolated_api.store.update_policy("alice", {"trigger_percent": "1"})
    monkeypatch.setattr(profit_sweep_api, "collect_readonly_snapshot", lambda *_args: _normal_snapshot())

    with pytest.raises(HTTPException, match="changed after Live confirmation"):
        asyncio.run(profit_sweep_api.activate_live(
            "alice",
            profit_sweep_api.LiveActivationRequest(
                expected_policy_fingerprint=saved["policy_fingerprint"]
            ),
            object(),
        ))

    assert isolated_api.store.get_policy("alice")["policy"]["operating_mode"] == "dry"


def test_active_fresh_policy_can_rebaseline_to_include_dry_retroactively(
    isolated_api: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A confirmed Live-policy edit may include Dry history before any transfer completes."""

    isolated_api.store.create_policy(
        "alice",
        "hyperliquid",
        {
            "operating_mode": "dry",
            "asset": "USDC",
            "baseline_mode": "from_enable",
            "live_activation_baseline_mode": "fresh",
            "trigger_percent": "0",
            "sweep_percent": "50",
            "minimum_transfer_amount": "0",
            "live_minimum_transfer_amount": "0",
            "first_live_catchup_limit_enabled": True,
            "first_live_catchup_limit": "3",
        },
        baseline_net_pnl="100",
    )
    isolated_api.store.evaluate_dry(
        "alice", cumulative_net_pnl="125", max_transferable="1000", now=1
    )
    isolated_api.store.activate_live("alice", "125", baseline_mode="fresh")
    snapshot = _normal_snapshot()
    snapshot["fills"]["events"][0]["closed_pnl"] = "125"
    monkeypatch.setattr(profit_sweep_api, "collect_readonly_snapshot", lambda *_args: snapshot)
    current = profit_sweep_api.get_policy("alice", object())
    policy = {**current["policy"], "live_activation_baseline_mode": "include_dry_period"}

    saved = profit_sweep_api.save_policy(
        "alice",
        profit_sweep_api.PolicyRequest(
            policy=policy,
            expected_generation=current["generation"],
            expected_policy_fingerprint=current["policy_fingerprint"],
            confirmed_live_update=True,
            recalculate_live_baseline=True,
        ),
        object(),
    )
    preview = profit_sweep_api._preview_sync("alice")

    assert saved["live_state"]["active_baseline_mode"] == "include_dry_period"
    assert saved["live_state"]["baseline_pnl"] == "100"
    assert preview["decision"]["state_kind"] == "live"
    assert preview["decision"]["net_pnl"] == "25"
    assert preview["decision"]["sweep_due"] == "12.5"
    assert preview["decision"]["amount"] == "3"


def test_live_preview_applies_existing_utc_daily_limit(
    isolated_api: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Evaluate now must not advertise a transfer after the Live daily cap is exhausted."""

    isolated_api.store.create_policy(
        "alice",
        "hyperliquid",
        {
            "operating_mode": "dry",
            "asset": "USDC",
            "baseline_mode": "lifetime",
            "trigger_percent": "0",
            "sweep_percent": "100",
            "live_minimum_transfer_amount": "0",
            "daily_transfer_limit_enabled": True,
            "daily_transfer_limit": "5",
        },
    )
    isolated_api.store.activate_live("alice", "0", baseline_mode="include_dry_period")
    today = profit_sweep_api.datetime.now(profit_sweep_api.timezone.utc).date().isoformat()
    with sqlite3.connect(isolated_api.store.db_path) as connection:
        connection.execute(
            "UPDATE live_state SET daily_date = ?, daily_total = '5' WHERE user_name = 'alice'",
            (today,),
        )
    snapshot = _normal_snapshot()
    monkeypatch.setattr(profit_sweep_api, "collect_readonly_snapshot", lambda *_args: snapshot)

    preview = profit_sweep_api._preview_sync("alice")

    assert preview["decision"]["state_kind"] == "live"
    assert preview["decision"]["sweep_due"] == "100"
    assert preview["decision"]["amount"] == "0"


def test_live_policy_updates_require_confirmation_and_current_generation(
    isolated_api: SimpleNamespace,
) -> None:
    """Reject stale or unconfirmed changes that alter future real transfers."""

    live = _create_live_policy(
        isolated_api.store,
        "alice",
        "hyperliquid",
        changes={"asset": "USDC"},
    )
    with pytest.raises(HTTPException, match="explicit confirmation"):
        profit_sweep_api.save_policy(
            "alice",
            profit_sweep_api.PolicyRequest(
                policy={"trigger_percent": "1"},
                expected_generation=live["generation"],
                expected_policy_fingerprint=profit_sweep_api._policy_fingerprint(live["policy"]),
            ),
            object(),
        )
    updated = profit_sweep_api.save_policy(
        "alice",
        profit_sweep_api.PolicyRequest(
            policy={"trigger_percent": "1"},
            expected_generation=live["generation"],
            expected_policy_fingerprint=profit_sweep_api._policy_fingerprint(live["policy"]),
            confirmed_live_update=True,
        ),
        object(),
    )
    assert updated["policy"]["trigger_percent"] == "1"

    with pytest.raises(HTTPException, match="another request"):
        profit_sweep_api.save_policy(
            "alice",
            profit_sweep_api.PolicyRequest(
                policy={"trigger_percent": "2"},
                expected_generation=0,
                expected_policy_fingerprint=updated["policy_fingerprint"],
                confirmed_live_update=True,
            ),
            object(),
        )
    with pytest.raises(HTTPException, match="Disable Live"):
        profit_sweep_api.save_policy(
            "alice",
            profit_sweep_api.PolicyRequest(
                policy={"asset": "USDT"},
                expected_generation=updated["generation"],
                expected_policy_fingerprint=updated["policy_fingerprint"],
                confirmed_live_update=True,
            ),
            object(),
        )


def test_live_policy_must_be_disabled_before_baseline_reset_or_deletion(
    isolated_api: SimpleNamespace,
) -> None:
    """Keep active Live accounting from being destructively reset or deleted."""

    live = _create_live_policy(
        isolated_api.store,
        "alice",
        "hyperliquid",
        changes={"asset": "USDC"},
    )
    with pytest.raises(HTTPException, match="Disable Live"):
        profit_sweep_api.reset_baseline(
            "alice",
            profit_sweep_api.BaselineRequest(
                cumulative_net_pnl="0",
                expected_policy_fingerprint=profit_sweep_api._policy_fingerprint(live["policy"]),
            ),
            object(),
        )
    with pytest.raises(HTTPException, match="Disable Live"):
        profit_sweep_api.delete_policy(
            "alice",
            profit_sweep_api.DeletePolicyRequest(
                expected_policy_fingerprint=profit_sweep_api._policy_fingerprint(live["policy"])
            ),
            object(),
        )

    disabled = profit_sweep_api.save_policy(
        "alice",
        profit_sweep_api.PolicyRequest(
            policy={"operating_mode": "disabled"},
            expected_generation=live["generation"],
            expected_policy_fingerprint=profit_sweep_api._policy_fingerprint(live["policy"]),
        ),
        object(),
    )
    assert disabled["policy"]["operating_mode"] == "disabled"
    assert profit_sweep_api.delete_policy(
        "alice",
        profit_sweep_api.DeletePolicyRequest(
            expected_policy_fingerprint=disabled["policy_fingerprint"]
        ),
        object(),
    ) == {"ok": True}


@pytest.mark.parametrize(
    ("user_name", "snapshot_factory", "expected_routes"),
    [
        ("alice", _normal_snapshot, ("perp_to_spot", "spot_to_perp")),
        ("bybit", _bybit_snapshot, ("unified_to_fund", "fund_to_unified")),
        ("binance", _binance_snapshot, ("umfuture_to_funding", "funding_to_umfuture")),
        (
            "bitget-classic",
            lambda: _bitget_snapshot("classic"),
            ("usdt_futures_to_spot", "spot_to_usdt_futures"),
        ),
        ("bitget-uta", lambda: _bitget_snapshot("uta"), ("uta_to_spot", "spot_to_uta")),
    ],
)
def test_manual_test_transfer_default_roundtrip_all_exchange_modes(
    isolated_api: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
    user_name: str,
    snapshot_factory: Any,
    expected_routes: tuple[str, str],
) -> None:
    """Default to one unit and persist one confirmed forward/back pair per adapter."""

    snapshot = snapshot_factory()
    monkeypatch.setattr(profit_sweep_api, "collect_readonly_snapshot", lambda *_args: snapshot)
    submitted: list[dict[str, Any]] = []

    def submit(_user_value: Any, descriptor: dict[str, Any]) -> dict[str, Any]:
        """Capture one sealed descriptor and return transport acceptance."""

        submitted.append(descriptor)
        return {
            "status": "submitted",
            "operation_id": descriptor["operation_id"],
            "submitted_at_ms": int(profit_sweep_api.time.time() * 1000),
        }

    def reconcile(
        _user_value: Any,
        descriptor: dict[str, Any],
        _submission: dict[str, Any],
    ) -> dict[str, Any]:
        """Confirm offline and expose an actual forward receipt for the return amount."""

        result = {"status": "confirmed", "operation_id": descriptor["operation_id"]}
        if descriptor["route"] == expected_routes[0]:
            result["received_amount"] = "0.75"
        return result

    monkeypatch.setattr(profit_sweep_api, "submit_transfer", submit)
    monkeypatch.setattr(profit_sweep_api, "reconcile_transfer", reconcile)
    before = isolated_api.store.list_policies()

    forward = asyncio.run(
        profit_sweep_api.test_transfer(
            user_name,
            _test_request(asset=str(snapshot["asset"]["symbol"])),
            object(),
        )
    )
    back = asyncio.run(
        profit_sweep_api.test_transfer_back(
            user_name,
            forward["operation"]["operation_id"],
            object(),
        )
    )
    listed = profit_sweep_api.get_test_transfers(user_name, object())

    assert forward["status"] == "confirmed"
    assert forward["can_transfer_back"] is True
    assert forward["operation"]["requested_amount"] == "1"
    assert forward["operation"]["asset"] == snapshot["asset"]["symbol"]
    assert back["status"] == "confirmed"
    assert back["can_transfer_back"] is False
    assert back["operation"]["requested_amount"] == "0.75"
    assert [item["route"] for item in submitted] == list(expected_routes)
    assert len(listed["operations"]) == 2
    assert all(item["can_transfer_back"] is False for item in listed["operations"])
    assert "descriptor" not in json.dumps({"forward": forward, "back": back, "listed": listed}).lower()
    assert "route" not in json.dumps({"forward": forward, "back": back, "listed": listed}).lower()
    assert isolated_api.store.list_policies() == before

    with pytest.raises(HTTPException) as duplicate:
        asyncio.run(
            profit_sweep_api.test_transfer_back(
                user_name,
                forward["operation"]["operation_id"],
                object(),
            )
        )
    assert duplicate.value.status_code == 409
    assert len(submitted) == 2


def test_manual_test_transfer_rejects_amount_over_fresh_cap(
    isolated_api: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject a test-transfer value above the current source cap."""

    capped = _normal_snapshot()
    capped["account"]["withdrawable"] = "0.5"
    monkeypatch.setattr(profit_sweep_api, "collect_readonly_snapshot", lambda *_args: capped)
    with pytest.raises(HTTPException) as over:
        asyncio.run(
            profit_sweep_api.test_transfer(
                "alice", _test_request(amount="1"), object()
            )
        )
    assert over.value.status_code == 409
    assert "transferable" in str(over.value.detail)
    assert isolated_api.store.list_test_operations("alice") == []


def test_manual_vault_test_transfer_roundtrip_uses_withdraw_then_deposit(
    isolated_api: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test a Vault through one reconciled withdrawal and one same-Vault deposit."""

    snapshot = _vault_snapshot()
    snapshot["vault"]["always_close_on_withdraw"] = True
    monkeypatch.setattr(profit_sweep_api, "collect_readonly_snapshot", lambda *_args: snapshot)
    submitted: list[dict[str, Any]] = []
    reconcile_attempts: dict[str, int] = {}

    def submit(_user_value: Any, descriptor: dict[str, Any]) -> dict[str, Any]:
        """Capture each sealed Vault descriptor without exchange I/O."""

        submitted.append(descriptor)
        return {
            "status": "submitted",
            "operation_id": descriptor["operation_id"],
            "submitted_at_ms": int(profit_sweep_api.time.time() * 1000),
            "retry_safe": True,
        }

    def reconcile(_user_value: Any, descriptor: dict[str, Any], _submission: dict[str, Any]) -> dict[str, Any]:
        """Return the exact received amount for both Vault test legs."""

        route = descriptor["route"]
        reconcile_attempts[route] = reconcile_attempts.get(route, 0) + 1
        if route == "vault_to_main_perps" and reconcile_attempts[route] < 3:
            return {"status": "pending", "operation_id": descriptor["operation_id"]}
        return {
            "status": "confirmed",
            "operation_id": descriptor["operation_id"],
            "received_amount": descriptor["amount"],
        }

    monkeypatch.setattr(profit_sweep_api, "submit_transfer", submit)
    monkeypatch.setattr(profit_sweep_api, "reconcile_transfer", reconcile)
    monkeypatch.setattr(profit_sweep_api.time, "sleep", lambda _seconds: None)

    forward = asyncio.run(
        profit_sweep_api.test_transfer(
            "vault", _test_request(amount="5"), object()
        )
    )
    returned = asyncio.run(
        profit_sweep_api.test_transfer_back(
            "vault", forward["operation"]["operation_id"], object()
        )
    )

    assert forward["status"] == "confirmed"
    assert forward["operation"]["actual_amount"] == "5"
    assert returned["status"] == "confirmed"
    assert [descriptor["route"] for descriptor in submitted] == [
        "vault_to_main_perps",
        "main_perps_to_vault",
    ]
    assert submitted[0]["request"]["action"]["isDeposit"] is False
    assert submitted[1]["request"]["action"]["isDeposit"] is True
    assert submitted[1]["amount"] == "5"
    assert reconcile_attempts == {"vault_to_main_perps": 3, "main_perps_to_vault": 1}
    persisted = json.dumps(isolated_api.store.list_test_operations("vault"))
    assert "signature" not in persisted.lower()
    assert LEADER_PRIVATE_KEY not in persisted


def test_manual_vault_test_below_deposit_minimum_has_no_transfer_back(
    isolated_api: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Allow a small withdrawal but suppress its unavailable return deposit."""

    monkeypatch.setattr(profit_sweep_api, "collect_readonly_snapshot", lambda *_args: _vault_snapshot())
    monkeypatch.setattr(
        profit_sweep_api,
        "submit_transfer",
        lambda _user_value, descriptor: {
            "status": "submitted",
            "operation_id": descriptor["operation_id"],
            "submitted_at_ms": int(profit_sweep_api.time.time() * 1000),
        },
    )
    monkeypatch.setattr(
        profit_sweep_api,
        "reconcile_transfer",
        lambda _user_value, descriptor, _submission: {
            "status": "confirmed",
            "operation_id": descriptor["operation_id"],
            "received_amount": descriptor["amount"],
        },
    )
    result = asyncio.run(
        profit_sweep_api.test_transfer(
            "vault", _test_request(amount="1"), object()
        )
    )

    assert result["status"] == "confirmed"
    assert result["can_transfer_back"] is False
    assert "require at least 5 USDC" in result["operation"]["transfer_back_reason"]
    listed = profit_sweep_api.get_test_transfers("vault", object())
    assert listed["operations"][0]["can_transfer_back"] is False
    assert listed["operations"][0]["transfer_back_reason"] == result["operation"]["transfer_back_reason"]
    with pytest.raises(HTTPException) as blocked:
        asyncio.run(
            profit_sweep_api.test_transfer_back(
                "vault", result["operation"]["operation_id"], object()
            )
        )
    assert blocked.value.status_code == 409
    assert "at least 5 USDC" in str(blocked.value.detail)


def test_vault_transferable_keeps_strictly_more_than_retained_floor() -> None:
    """Leave one micro-USDC above the strict Hyperliquid Leader minimum."""

    snapshot = _vault_snapshot()
    snapshot["vault"].update({
        "vault_equity": "100.000001",
        "leader_fraction": "1",
        "max_withdrawable": "10",
        "balances": {"account_value": "100.000001", "withdrawable": "100.000001"},
    })

    assert profit_sweep_api._vault_transferable(profit_sweep_api.default_policy(), snapshot) == "0"


def test_manual_vault_test_explains_active_always_close_risk(
    isolated_api: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject a risky Vault test with exact activity counts and remediation."""

    snapshot = _vault_snapshot()
    snapshot["vault"]["always_close_on_withdraw"] = True
    snapshot["vault"]["positions"] = [{"coin": "BTC"}]
    snapshot["vault"]["orders"] = [{"order_id": "1"}, {"order_id": "2"}]
    monkeypatch.setattr(profit_sweep_api, "collect_readonly_snapshot", lambda *_args: snapshot)

    with pytest.raises(HTTPException) as blocked:
        asyncio.run(
            profit_sweep_api.test_transfer(
                "vault", _test_request(amount="5"), object()
            )
        )

    assert blocked.value.status_code == 409
    detail = str(blocked.value.detail)
    assert "alwaysCloseOnWithdraw" in detail
    assert "1 open position(s)" in detail
    assert "2 open order(s)" in detail
    assert "flatten the Vault" in detail
    assert isolated_api.store.list_test_operations("vault") == []


def test_manual_test_transfer_timeout_stays_unknown_without_blind_retry(
    isolated_api: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep an ambiguous operation visible and never submit it again."""

    monkeypatch.setattr(profit_sweep_api, "collect_readonly_snapshot", lambda *_args: _binance_snapshot())
    submissions: list[str] = []

    def timeout(_user_value: Any, descriptor: dict[str, Any]) -> dict[str, Any]:
        """Return the adapter timeout result without network access."""

        submissions.append(descriptor["operation_id"])
        return {
            "status": "unknown",
            "operation_id": descriptor["operation_id"],
            "submitted_at_ms": int(profit_sweep_api.time.time() * 1000),
            "retry_safe": False,
        }

    monkeypatch.setattr(profit_sweep_api, "submit_transfer", timeout)
    monkeypatch.setattr(
        profit_sweep_api,
        "reconcile_transfer",
        lambda _user_value, descriptor, _submission: {
            "status": "unknown",
            "operation_id": descriptor["operation_id"],
            "reason": "timeout",
        },
    )

    result = asyncio.run(
        profit_sweep_api.test_transfer(
            "binance", _test_request(asset="USDT"), object()
        )
    )
    listed = profit_sweep_api.get_test_transfers("binance", object())

    assert result["status"] == "unknown"
    assert result["can_transfer_back"] is False
    assert listed["operations"][0]["status"] == "unknown"
    assert listed["operations"][0]["can_transfer_back"] is False
    assert submissions == [result["operation"]["operation_id"]]
    with pytest.raises(HTTPException) as back:
        asyncio.run(
            profit_sweep_api.test_transfer_back(
                "binance", result["operation"]["operation_id"], object()
            )
        )
    assert back.value.status_code == 409
    assert submissions == [result["operation"]["operation_id"]]


def test_manual_test_transfer_idempotency_key_submits_once_under_concurrency(
    isolated_api: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Concurrent retries with one operation ID must return one durable submission."""

    monkeypatch.setattr(profit_sweep_api, "collect_readonly_snapshot", lambda *_args: _binance_snapshot())
    submissions: list[str] = []

    def submit(_user_value: Any, descriptor: dict[str, Any]) -> dict[str, Any]:
        """Record the sole allowed exchange submission."""

        submissions.append(descriptor["operation_id"])
        return {
            "status": "submitted",
            "operation_id": descriptor["operation_id"],
            "submitted_at_ms": int(profit_sweep_api.time.time() * 1000),
        }

    monkeypatch.setattr(profit_sweep_api, "submit_transfer", submit)
    monkeypatch.setattr(
        profit_sweep_api,
        "reconcile_transfer",
        lambda _user_value, descriptor, _submission: {
            "status": "confirmed",
            "operation_id": descriptor["operation_id"],
        },
    )
    body = _test_request(asset="USDT")

    async def scenario() -> list[dict[str, Any]]:
        """Issue two authenticated requests before either caller receives a result."""

        return list(await asyncio.gather(
            profit_sweep_api.test_transfer("binance", body, object()),
            profit_sweep_api.test_transfer("binance", body, object()),
        ))

    results = asyncio.run(scenario())

    assert [item["status"] for item in results] == ["confirmed", "confirmed"]
    assert submissions == [body.operation_id]
    assert len(isolated_api.store.list_test_operations("binance")) == 1


def test_manual_test_transfer_idempotency_key_rejects_changed_amount_or_asset(
    isolated_api: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A retry UUID cannot be reused for a differently described real transfer."""

    monkeypatch.setattr(profit_sweep_api, "collect_readonly_snapshot", lambda *_args: _binance_snapshot())
    submissions: list[str] = []
    monkeypatch.setattr(
        profit_sweep_api,
        "submit_transfer",
        lambda _user_value, descriptor: submissions.append(descriptor["operation_id"]) or {
            "status": "submitted",
            "operation_id": descriptor["operation_id"],
            "submitted_at_ms": int(profit_sweep_api.time.time() * 1000),
        },
    )
    monkeypatch.setattr(
        profit_sweep_api,
        "reconcile_transfer",
        lambda _user_value, descriptor, _submission: {
            "status": "confirmed",
            "operation_id": descriptor["operation_id"],
        },
    )
    operation_id = str(uuid.uuid4())
    asyncio.run(profit_sweep_api.test_transfer(
        "binance",
        _test_request(amount="1", asset="USDT", operation_id=operation_id),
        object(),
    ))

    with pytest.raises(HTTPException, match="original test amount and asset"):
        asyncio.run(profit_sweep_api.test_transfer(
            "binance",
            _test_request(amount="2", asset="USDT", operation_id=operation_id),
            object(),
        ))

    assert submissions == [operation_id]


def test_submitting_test_transfer_blocks_restart_and_recovers_without_resubmit(
    isolated_api: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Startup must reconcile a crash-after-submit test operation without sending it again."""

    monkeypatch.setattr(profit_sweep_api, "collect_readonly_snapshot", lambda *_args: _binance_snapshot())
    submissions: list[str] = []

    def crash_after_submit(_user_value: Any, descriptor: dict[str, Any]) -> dict[str, Any]:
        """Simulate process loss after the durable submitting claim."""

        submissions.append(descriptor["operation_id"])
        raise RuntimeError("simulated test-transfer crash")

    monkeypatch.setattr(profit_sweep_api, "submit_transfer", crash_after_submit)
    operation_id = str(uuid.uuid4())
    with pytest.raises(RuntimeError, match="test-transfer crash"):
        profit_sweep_api._test_transfer_sync("binance", "1", "USDT", operation_id)

    persisted = isolated_api.store.get_test_operation(operation_id)
    assert persisted["state"] == "submitting"
    assert "test transfer" in profit_sweep_api.restart_block_reason()

    monkeypatch.setattr(
        profit_sweep_api,
        "reconcile_transfer",
        lambda _user_value, descriptor, _submission: {
            "status": "confirmed",
            "operation_id": descriptor["operation_id"],
        },
    )
    profit_sweep_api._reconcile_unresolved_sync()

    assert submissions == [operation_id]
    assert isolated_api.store.get_test_operation(operation_id)["state"] == "confirmed"
    assert profit_sweep_api.restart_block_reason() == ""


def test_public_binance_authentication_failure_has_actionable_permission_reason() -> None:
    """Explain a persisted Binance transfer-auth failure without exposing its descriptor."""

    operation = {
        "operation_id": "binance-failed-operation",
        "parent_id": None,
        "direction": "forward",
        "state": "failed",
        "requested_amount": "1",
        "actual_amount": None,
        "prepared_at": 1,
        "submitted_at": 2,
        "resolved_at": 3,
        "error": {"reason": "failed"},
        "submission": {
            "status": "failed",
            "error": {"category": "exchange_error", "type": "AuthenticationError"},
        },
        "descriptor": {"adapter": "binance_um", "request": {"secret": "must-not-escape"}},
    }

    result = profit_sweep_api._public_test_operation(operation)

    assert result["error"]["reason"] == profit_sweep_api.BINANCE_TRANSFER_PERMISSION_REASON
    assert "descriptor" not in result
    assert "submission" not in result
    assert "must-not-escape" not in json.dumps(result)


def test_public_legacy_hyperliquid_rejection_explains_missing_provider_detail() -> None:
    """Explain why an older Vault failure cannot show the discarded provider reason."""

    operation = {
        "operation_id": "vault-failed-operation",
        "parent_id": None,
        "direction": "forward",
        "state": "failed",
        "requested_amount": "1",
        "actual_amount": None,
        "prepared_at": 1,
        "submitted_at": 2,
        "resolved_at": 3,
        "error": {"category": "exchange_error", "type": "InvalidOrder"},
        "submission": {
            "status": "failed",
            "error": {"category": "exchange_error", "type": "InvalidOrder"},
        },
        "descriptor": {"adapter": "hyperliquid_vault", "request": {"signature": "must-not-escape"}},
    }

    result = profit_sweep_api._public_test_operation(operation)

    assert "before PBGui retained" in result["error"]["reason"]
    assert "descriptor" not in result
    assert "submission" not in result
    assert "must-not-escape" not in json.dumps(result)


def test_crash_after_submit_reconciles_on_startup_without_duplicate_submission(
    isolated_api: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A crash after the external call leaves submitting durable and recovery never resubmits."""

    _create_live_policy(
        isolated_api.store,
        "alice",
        "hyperliquid",
        changes={"asset": "USDC"},
    )
    monkeypatch.setattr(profit_sweep_api, "collect_readonly_snapshot", lambda *_args: _normal_snapshot())
    submissions: list[str] = []

    def crash_after_submit(_user_value: Any, descriptor: dict[str, Any]) -> dict[str, Any]:
        """Simulate process loss after the exchange accepted the request."""

        submissions.append(descriptor["operation_id"])
        raise RuntimeError("simulated process crash")

    monkeypatch.setattr(profit_sweep_api, "submit_transfer", crash_after_submit)
    with pytest.raises(RuntimeError, match="process crash"):
        profit_sweep_api._evaluate_live_sync("alice")

    submitting = isolated_api.store.list_live_intents("alice", unresolved_only=True)[0]
    assert submitting["state"] == "submitting"
    assert "is submitting" in profit_sweep_api.restart_block_reason()

    monkeypatch.setattr(
        profit_sweep_api,
        "reconcile_transfer",
        lambda _user_value, descriptor, _submission: {
            "status": "confirmed",
            "operation_id": descriptor["operation_id"],
        },
    )
    profit_sweep_api._reconcile_unresolved_sync()

    assert submissions == [submitting["operation_id"]]
    assert isolated_api.store.get_live_intent(submitting["operation_id"])["state"] == "confirmed"
    assert isolated_api.store.get_policy("alice")["live_state"]["confirmed_total"] == "100"
    assert profit_sweep_api.restart_block_reason() == ""


def test_binance_unknown_pauses_and_never_submits_a_duplicate(
    isolated_api: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-idempotent Binance ambiguity retains its reservation and pauses automation."""

    _create_live_policy(isolated_api.store, "binance", "binance")
    monkeypatch.setattr(profit_sweep_api, "collect_readonly_snapshot", lambda *_args: _binance_snapshot())
    submissions: list[str] = []

    def unknown_submit(_user_value: Any, descriptor: dict[str, Any]) -> dict[str, Any]:
        """Return the sealed adapter's timeout vocabulary without network I/O."""

        submissions.append(descriptor["operation_id"])
        return {
            "status": "unknown",
            "operation_id": descriptor["operation_id"],
            "submitted_at_ms": int(profit_sweep_api.time.time() * 1000),
            "retry_safe": False,
        }

    monkeypatch.setattr(profit_sweep_api, "submit_transfer", unknown_submit)
    monkeypatch.setattr(
        profit_sweep_api,
        "reconcile_transfer",
        lambda _user_value, descriptor, _submission: {
            "status": "unknown",
            "operation_id": descriptor["operation_id"],
            "reason": "ambiguous_history",
        },
    )

    result = profit_sweep_api._evaluate_live_sync("binance")
    with pytest.raises(ValueError, match="operating_mode=live"):
        profit_sweep_api._evaluate_live_sync("binance")

    assert result["intent"]["state"] == "unknown"
    assert submissions == [result["intent"]["operation_id"]]
    policy = isolated_api.store.get_policy("binance")
    assert policy["policy"]["operating_mode"] == "paused_unknown"
    assert policy["live_state"]["sweep_due"] == "100"
    response = profit_sweep_api.get_intents("binance", object())
    _assert_secret_free(response)
    encoded = json.dumps(response)
    assert '"descriptor"' not in encoded
    assert '"submission"' not in encoded
    assert '"request"' not in encoded


def test_bybit_scheduler_allocates_a_canonical_uuid_operation_id(
    isolated_api: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bybit's persisted operation ID is the exact canonical transfer UUID."""

    _create_live_policy(isolated_api.store, "bybit", "bybit")
    monkeypatch.setattr(profit_sweep_api, "collect_readonly_snapshot", lambda *_args: _bybit_snapshot())
    monkeypatch.setattr(
        profit_sweep_api,
        "submit_transfer",
        lambda _user_value, descriptor: {
            "status": "submitted",
            "operation_id": descriptor["operation_id"],
            "submitted_at_ms": int(profit_sweep_api.time.time() * 1000),
        },
    )
    monkeypatch.setattr(
        profit_sweep_api,
        "reconcile_transfer",
        lambda _user_value, descriptor, _submission: {
            "status": "confirmed",
            "operation_id": descriptor["operation_id"],
        },
    )

    result = profit_sweep_api._evaluate_live_sync("bybit")
    operation_id = result["intent"]["operation_id"]

    assert str(uuid.UUID(operation_id)) == operation_id
    assert result["intent"]["descriptor"]["idempotency"]["value"] == operation_id


def test_vault_main_spot_persists_two_legs_and_uses_received_amount(
    isolated_api: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Vault main-spot completion forwards only reconciled net proceeds in leg two."""

    _create_live_policy(
        isolated_api.store,
        "vault",
        "hyperliquid",
        changes={"asset": "USDC", "vault_destination": "main_spot"},
    )
    monkeypatch.setattr(profit_sweep_api, "collect_readonly_snapshot", lambda *_args: _vault_snapshot())
    submitted_routes: list[str] = []

    def submitted(_user_value: Any, descriptor: dict[str, Any]) -> dict[str, Any]:
        """Capture each fixed route and return a transport-level acceptance."""

        submitted_routes.append(descriptor["route"])
        return {
            "status": "submitted",
            "operation_id": descriptor["operation_id"],
            "submitted_at_ms": int(profit_sweep_api.time.time() * 1000),
        }

    def reconciled(_user_value: Any, descriptor: dict[str, Any], _submission: dict[str, Any]) -> dict[str, Any]:
        """Return actual Vault proceeds for leg one and confirmation for leg two."""

        result = {"status": "confirmed", "operation_id": descriptor["operation_id"]}
        if descriptor["route"] == "vault_to_main_perps":
            result["received_amount"] = "80"
        return result

    monkeypatch.setattr(profit_sweep_api, "submit_transfer", submitted)
    monkeypatch.setattr(profit_sweep_api, "reconcile_transfer", reconciled)

    result = profit_sweep_api._evaluate_live_sync("vault")
    intents = isolated_api.store.list_live_intents("vault")

    assert submitted_routes == ["vault_to_main_perps", "main_perps_to_spot"]
    assert [intent["leg"] for intent in intents] == [1, 2]
    assert intents[0]["parent_id"] == intents[1]["parent_id"]
    assert intents[1]["descriptor"]["amount"] == "80"
    assert intents[1]["state"] == "confirmed"
    assert result["intent_leg2"]["operation_id"] == intents[1]["operation_id"]
    live = isolated_api.store.get_policy("vault")["live_state"]
    assert live["confirmed_total"] == "80"
    assert live["sweep_due"] == "20"


def test_failed_vault_forwarding_pauses_and_recovers_without_second_withdrawal(
    isolated_api: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A confirmed first leg remains blocking until its missing Spot leg is recovered."""

    _create_live_policy(
        isolated_api.store,
        "vault",
        "hyperliquid",
        changes={"asset": "USDC", "vault_destination": "main_spot"},
    )
    monkeypatch.setattr(profit_sweep_api, "collect_readonly_snapshot", lambda *_args: _vault_snapshot())
    submitted_routes: list[str] = []

    def submitted(_user_value: Any, descriptor: dict[str, Any]) -> dict[str, Any]:
        """Capture every external route to prove leg one is never repeated."""

        submitted_routes.append(descriptor["route"])
        return {
            "status": "submitted",
            "operation_id": descriptor["operation_id"],
            "submitted_at_ms": int(profit_sweep_api.time.time() * 1000),
        }

    def reconciled(_user_value: Any, descriptor: dict[str, Any], _submission: dict[str, Any]) -> dict[str, Any]:
        """Confirm both legs and expose the first leg's actual proceeds."""

        result = {"status": "confirmed", "operation_id": descriptor["operation_id"]}
        if descriptor["route"] == "vault_to_main_perps":
            result["received_amount"] = "80"
        return result

    monkeypatch.setattr(profit_sweep_api, "submit_transfer", submitted)
    monkeypatch.setattr(profit_sweep_api, "reconcile_transfer", reconciled)
    original_create = profit_sweep_api._create_vault_leg_two_unchecked
    monkeypatch.setattr(
        profit_sweep_api,
        "_create_vault_leg_two_unchecked",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("Spot preflight unavailable")),
    )

    with pytest.raises(RuntimeError, match="Spot preflight unavailable"):
        profit_sweep_api._evaluate_live_sync("vault")

    policy = isolated_api.store.get_policy("vault")
    leg_one = isolated_api.store.list_live_intents("vault")[0]
    assert policy["policy"]["operating_mode"] == "paused_unknown"
    assert leg_one["state"] == "confirmed"
    assert submitted_routes == ["vault_to_main_perps"]
    visible = profit_sweep_api.get_intents("vault", object())["intents"]
    assert visible[0]["can_reconcile"] is True

    monkeypatch.setattr(profit_sweep_api, "_create_vault_leg_two_unchecked", original_create)
    recovered = profit_sweep_api._reconcile_operation_sync("vault", leg_one["operation_id"])

    assert recovered["route"] == "main_perps_to_spot"
    assert recovered["state"] == "confirmed"
    assert submitted_routes == ["vault_to_main_perps", "main_perps_to_spot"]
    assert isolated_api.store.get_policy("vault")["policy"]["operating_mode"] == "live"


def test_definitive_failed_vault_forwarding_retries_only_leg_two(
    isolated_api: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A rejected Spot leg stays paused and a later reconcile allocates only a new leg two."""

    _create_live_policy(
        isolated_api.store,
        "vault",
        "hyperliquid",
        changes={"asset": "USDC", "vault_destination": "main_spot"},
    )
    monkeypatch.setattr(profit_sweep_api, "collect_readonly_snapshot", lambda *_args: _vault_snapshot())
    submitted_routes: list[str] = []
    spot_attempts = 0

    def submitted(_user_value: Any, descriptor: dict[str, Any]) -> dict[str, Any]:
        """Record each route and return a transport acknowledgement."""

        submitted_routes.append(descriptor["route"])
        return {
            "status": "submitted",
            "operation_id": descriptor["operation_id"],
            "submitted_at_ms": int(profit_sweep_api.time.time() * 1000),
        }

    def reconciled(_user_value: Any, descriptor: dict[str, Any], _submission: dict[str, Any]) -> dict[str, Any]:
        """Reject the first Spot attempt and confirm the retry."""

        nonlocal spot_attempts
        if descriptor["route"] == "vault_to_main_perps":
            return {
                "status": "confirmed",
                "operation_id": descriptor["operation_id"],
                "received_amount": "80",
            }
        spot_attempts += 1
        return {
            "status": "failed" if spot_attempts == 1 else "confirmed",
            "operation_id": descriptor["operation_id"],
        }

    monkeypatch.setattr(profit_sweep_api, "submit_transfer", submitted)
    monkeypatch.setattr(profit_sweep_api, "reconcile_transfer", reconciled)

    initial = profit_sweep_api._evaluate_live_sync("vault")
    leg_one = initial["intent"]
    assert initial["intent_leg2"]["state"] == "failed"
    assert isolated_api.store.get_policy("vault")["policy"]["operating_mode"] == "paused_unknown"
    assert profit_sweep_api.get_intents("vault", object())["intents"][0]["can_reconcile"] is True

    recovered = profit_sweep_api._reconcile_operation_sync("vault", leg_one["operation_id"])

    assert recovered["state"] == "confirmed"
    assert recovered["operation_id"].endswith("leg2-retry-1")
    assert submitted_routes == [
        "vault_to_main_perps",
        "main_perps_to_spot",
        "main_perps_to_spot",
    ]
    assert isolated_api.store.get_policy("vault")["policy"]["operating_mode"] == "live"


def test_vault_main_perps_completes_after_leg_one(
    isolated_api: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A Vault whose destination is main perps settles once without a forwarding leg."""

    _create_live_policy(
        isolated_api.store,
        "vault",
        "hyperliquid",
        changes={"asset": "USDC", "vault_destination": "main_perps"},
    )
    monkeypatch.setattr(profit_sweep_api, "collect_readonly_snapshot", lambda *_args: _vault_snapshot())
    monkeypatch.setattr(
        profit_sweep_api,
        "submit_transfer",
        lambda _user_value, descriptor: {
            "status": "submitted",
            "operation_id": descriptor["operation_id"],
            "submitted_at_ms": int(profit_sweep_api.time.time() * 1000),
        },
    )
    monkeypatch.setattr(
        profit_sweep_api,
        "reconcile_transfer",
        lambda _user_value, descriptor, _submission: {
            "status": "confirmed",
            "operation_id": descriptor["operation_id"],
            "received_amount": "80",
        },
    )

    result = profit_sweep_api._evaluate_live_sync("vault")
    intents = isolated_api.store.list_live_intents("vault")

    assert result["intent"]["state"] == "confirmed"
    assert len(intents) == 1
    assert intents[0]["route"] == "vault_to_main_perps"
    assert intents[0]["parent_id"] == intents[0]["operation_id"]
    live = isolated_api.store.get_policy("vault")["live_state"]
    assert live["confirmed_total"] == "80"
    assert live["sweep_due"] == "20"


def test_pb_api_server_registers_all_profit_sweep_hooks() -> None:
    """PBApiServer must wire the router, lifecycle, blocker, and income hint."""

    source = (ROOT / "PBApiServer.py").read_text(encoding="utf-8")

    assert "router as profit_sweep_router" in source
    assert "startup as profit_sweep_startup" in source
    assert "shutdown as profit_sweep_shutdown" in source
    assert "restart_block_reason as profit_sweep_restart_block_reason" in source
    assert "notify_income as profit_sweep_notify_income" in source
    assert 'app.include_router(profit_sweep_router, prefix="/api/profit-sweep"' in source
    assert "profit_sweep_startup()" in source
    assert '("profit-sweep", profit_sweep_shutdown)' in source
    assert "profit_sweep_restart_block_reason()" in source
    assert "await profit_sweep_notify_income(user_name)" in source


def test_health_is_secret_free_and_non_mutating(isolated_api: SimpleNamespace) -> None:
    """Health diagnostics report Live availability without mutating persisted state."""

    _create_dry_policy(isolated_api.store, "alice")
    policies_before = isolated_api.store.list_policies()
    journal_before = isolated_api.store.list_simulation_journal("alice")

    health = profit_sweep_api.get_health(object())

    assert health["feature_status"] == "live"
    assert health["read_only"] is False
    assert health["scheduler_running"] is False
    assert health["database"]["schema_version"] == 5
    assert isolated_api.store.list_policies() == policies_before
    assert isolated_api.store.list_simulation_journal("alice") == journal_before


def test_all_route_response_projections_are_secret_free(
    isolated_api: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """User, policy, preview, journal, schema, and health responses omit secrets."""

    _create_dry_policy(isolated_api.store, "alice")
    _create_dry_policy(isolated_api.store, "vault")

    def snapshot(user: SimpleNamespace, *_args: Any) -> dict[str, Any]:
        """Select a standard or vault snapshot for the requested synthetic user."""

        return _vault_snapshot() if user.is_vault else _normal_snapshot()

    monkeypatch.setattr(profit_sweep_api, "collect_readonly_snapshot", snapshot)
    standard_preview = asyncio.run(profit_sweep_api.evaluate_now("alice", object()))
    vault_preview = asyncio.run(profit_sweep_api.evaluate_now("vault", object()))
    responses = [
        profit_sweep_api.get_schema(object()),
        profit_sweep_api.list_users(object()),
        profit_sweep_api.list_policies(object()),
        profit_sweep_api.get_policy("alice", object()),
        standard_preview,
        vault_preview,
        profit_sweep_api.get_journal("alice", 100, object()),
        profit_sweep_api.get_intents("alice", object()),
        profit_sweep_api.get_test_transfers("alice", object()),
        profit_sweep_api.get_health(object()),
    ]

    for response in responses:
        _assert_secret_free(response)
        encoded = json.dumps(response)
        assert PRIVATE_KEY not in encoded
        assert API_KEY not in encoded
        assert API_SECRET not in encoded
        assert SESSION_TOKEN not in encoded

    assert vault_preview["snapshot"]["account_kind"] == "vault"
    assert vault_preview["decision"]["effective_cap"] == "200"
