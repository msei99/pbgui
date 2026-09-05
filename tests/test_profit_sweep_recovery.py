"""Offline races, return retries, targeted recovery, and mounted page contracts."""

import asyncio
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
import json
import sqlite3
from threading import Event
import uuid
import ccxt

from fastapi import HTTPException
import pytest
from starlette.requests import Request

import api.profit_sweep as api
from profit_sweep import ProfitSweepStore
import profit_sweep_transfers as transfers
from test_profit_sweep_api import (
    isolated_api, _create_live_policy, _normal_snapshot, _bybit_snapshot,
    _vault_snapshot, _user,
)


def _manual(store, operation_id, *, parent=None, direction="forward"):
    """Prepare a synthetic journal row without any exchange transport."""

    return store.create_test_operation(
        "alice", operation_id=operation_id, parent_id=parent, direction=direction,
        route="perp_to_spot" if direction == "forward" else "spot_to_perp",
        descriptor={"operation_id": operation_id, "route": "perp_to_spot" if direction == "forward" else "spot_to_perp", "amount": "1"},
        requested_amount="1",
    )


@pytest.mark.parametrize("state,safe", [
    ("prepared", False), ("submitting", False), ("unknown", False),
    ("confirmed", False), ("failed", False), ("failed", True),
])
def test_return_slot_requires_definitive_non_transfer(tmp_path, state, safe):
    """Only positively evidenced failed returns release the atomic reservation slot."""

    store = ProfitSweepStore(tmp_path / "state.sqlite3")
    _manual(store, "forward")
    store.transition_test_operation("forward", submission={"status": "submitted"}, claim=True)
    store.reconcile_test_operation("forward", {"status": "confirmed"})
    _manual(store, "back", parent="forward", direction="back")
    if state != "prepared":
        store.transition_test_operation("back", submission={"status": "failed" if state == "failed" else "submitted", "no_transfer": safe}, claim=True)
        if state != "submitting":
            store.reconcile_test_operation("back", {"status": state, "reason": "timeout" if not safe else "exchange_rejected"})
    if state == "failed" and safe:
        _manual(store, "retry", parent="forward", direction="back")
        assert len(store.list_test_operations("alice")) == 3
        with pytest.raises(ValueError, match="already been sent back"):
            _manual(store, "duplicate", parent="forward", direction="back")
    else:
        with pytest.raises(ValueError, match="already been sent back"):
            _manual(store, "retry", parent="forward", direction="back")


def test_v6_migration_keeps_ambiguous_failed_history_blocking(tmp_path):
    """Migrate the old unique index without inventing evidence for historical failures."""

    path = tmp_path / "state.sqlite3"
    store = ProfitSweepStore(path)
    _manual(store, "forward")
    store.transition_test_operation("forward", submission={"status": "submitted"})
    store.reconcile_test_operation("forward", {"status": "confirmed"})
    _manual(store, "back", parent="forward", direction="back")
    store.transition_test_operation("back", submission={"status": "failed"})
    store.reconcile_test_operation("back", {"status": "failed", "reason": "timeout"})
    before = store.list_test_operations("alice")
    with sqlite3.connect(path) as connection:
        connection.execute("DROP INDEX test_operations_one_back")
        connection.execute("ALTER TABLE test_operations DROP COLUMN retry_safe")
        connection.execute("CREATE UNIQUE INDEX test_operations_one_back ON test_operations(parent_id) WHERE direction = 'back' AND parent_id IS NOT NULL")
        connection.execute("PRAGMA user_version=6")
    migrated = ProfitSweepStore(path)
    assert migrated.database_settings()["schema_version"] == 7
    assert migrated.list_test_operations("alice") == before
    with pytest.raises(ValueError, match="already been sent back"):
        _manual(migrated, "retry", parent="forward", direction="back")
    assert ProfitSweepStore(path).list_test_operations("alice") == before


def test_failed_return_slot_allows_only_one_concurrent_retry(tmp_path):
    """Separate store connections cannot both reserve a failed return's retry slot."""

    path = tmp_path / "state.sqlite3"
    store = ProfitSweepStore(path)
    _manual(store, "forward")
    store.transition_test_operation("forward", submission={"status": "submitted"}, claim=True)
    store.reconcile_test_operation("forward", {"status": "confirmed"})
    _manual(store, "back", parent="forward", direction="back")
    store.transition_test_operation("back", submission={"status": "failed", "no_transfer": True}, claim=True)
    store.reconcile_test_operation("back", {"status": "failed"})

    def retry(number):
        """Attempt one new reservation using an independent store owner."""

        try:
            _manual(ProfitSweepStore(path), f"retry-{number}", parent="forward", direction="back")
            return True
        except ValueError:
            return False

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(retry, range(2))) == [False, True]
    assert len(store.list_test_operations("alice")) == 3


def test_explicit_failed_return_retry_is_idempotent(isolated_api, monkeypatch):
    """Use a new confirmed retry ID, preserve history, and never replay the forward leg."""

    monkeypatch.setattr(api, "collect_readonly_snapshot", lambda *args: _normal_snapshot())
    submitted = []

    def submit(user, descriptor):
        """Reject the first return before allowing an explicitly requested retry."""

        submitted.append(deepcopy(descriptor))
        return {"status": "failed" if len(submitted) == 2 else "submitted", "no_transfer": len(submitted) == 2}

    monkeypatch.setattr(api, "submit_transfer", submit)
    monkeypatch.setattr(api, "reconcile_transfer", lambda user, descriptor, submission: {"status": "failed" if submission["status"] == "failed" else "confirmed"})
    forward_id = str(uuid.uuid4())
    api._test_transfer_sync("alice", "1", "USDC", forward_id)
    failed = api._test_transfer_back_sync("alice", forward_id)
    listed = api.get_test_transfers("alice", object())["operations"]
    forward = next(item for item in listed if item["operation_id"] == forward_id)
    assert forward["can_transfer_back"] is True
    assert forward["retry_of"] == failed["operation"]["operation_id"]
    with pytest.raises(ValueError, match="Explicit retry"):
        api._test_transfer_back_sync("alice", forward_id)
    retry_id = str(uuid.uuid4())
    body = api.TransferBackRequest(operation_id=retry_id, retry_of=forward["retry_of"])
    result = asyncio.run(api.test_transfer_back("alice", forward_id, object(), body))
    repeated = asyncio.run(api.test_transfer_back("alice", forward_id, object(), body))
    assert result == repeated
    assert result["status"] == "confirmed"
    assert len(submitted) == 3
    assert submitted[1]["request"]["nonce"] != submitted[2]["request"]["nonce"]
    assert len(isolated_api.store.list_test_operations("alice")) == 3
    assert not next(item for item in api.get_test_transfers("alice", object())["operations"] if item["operation_id"] == forward_id)["can_transfer_back"]


def test_persisted_main_spot_forward_returns_from_spot_not_vault(isolated_api, monkeypatch):
    """A historical leader-Spot test uses its concrete source and no Vault deposit minimum."""

    snapshot = _vault_snapshot()
    snapshot["account_balances"]["destination"]["main_perps"]["balance"] = "0"
    snapshot["account_balances"]["destination"]["main_spot"]["balance"] = "2"
    user = isolated_api.users.find_user("vault")
    operation_id = str(uuid.uuid4())
    descriptor = transfers.prepare_transfer(user, operation_id=operation_id, amount="1", asset="USDC", route="main_perps_to_spot", snapshot=snapshot)
    isolated_api.store.create_test_operation("vault", operation_id=operation_id, parent_id=None, direction="forward", route="main_perps_to_spot", descriptor=descriptor, requested_amount="1")
    isolated_api.store.transition_test_operation(operation_id, submission={"status": "submitted"}, claim=True)
    isolated_api.store.reconcile_test_operation(operation_id, {"status": "confirmed"})
    projected = api.get_test_transfers("vault", object())["operations"][0]
    assert projected["return_endpoints"] == {"source": "Leader Main Spot", "destination": "Leader Main Perps"}
    monkeypatch.setattr(api, "collect_readonly_snapshot", lambda *args: snapshot)
    posted = []

    def submit(user, descriptor):
        """Capture the fixed leader-Spot return instead of sending it."""

        posted.append(descriptor)
        return {"status": "submitted"}

    monkeypatch.setattr(api, "submit_transfer", submit)
    monkeypatch.setattr(api, "reconcile_transfer", lambda *args: {"status": "confirmed"})
    assert api._test_transfer_back_sync("vault", operation_id)["status"] == "confirmed"
    assert posted[0]["route"] == "main_spot_to_perps"
    assert posted[0]["request"]["action"]["type"] == "agentSendAsset"
    assert posted[0]["request"]["action"]["sourceDex"] == "spot"


@pytest.mark.parametrize("code,retry_allowed", [(131001, True), (10005, True), (10014, False), (10000, False), (999999, False)])
def test_real_bybit_return_rejection_unlocks_only_definitive_retry(isolated_api, monkeypatch, code, retry_allowed):
    """Persist real parser exceptions through the API and enforce the resulting retry decision."""

    client = ccxt.bybit()
    calls = []

    def transfer(params):
        """A successful forward, parsed refusal, and explicit retry all remain offline."""

        calls.append(params)
        if len(calls) == 2:
            response = {"retCode": code, "retMsg": "synthetic refusal", "result": {}}
            client.handle_errors(200, "OK", "https://api.bybit.com/v5/asset/transfer/inter-transfer", "POST", {}, json.dumps(response), response, {}, "")
        return {"retCode": 0, "result": {"transferId": params["transferId"]}}

    class Owner:
        """Own a pure parser client with no open network resources."""

        def close(self):
            """No network resources were acquired."""

    monkeypatch.setattr(client, "privatePostV5AssetTransferInterTransfer", transfer)
    monkeypatch.setattr(transfers, "_owned_client", lambda *args: (Owner(), client))
    monkeypatch.setattr(api, "collect_readonly_snapshot", lambda *args: _bybit_snapshot())
    monkeypatch.setattr(api, "reconcile_transfer", lambda user, descriptor, submission: {"status": "confirmed" if submission["status"] == "submitted" else submission["status"]})
    forward_id = str(uuid.uuid4())
    api._test_transfer_sync("bybit", "1", "USDT", forward_id)
    returned = api._test_transfer_back_sync("bybit", forward_id)
    assert returned["status"] == ("failed" if retry_allowed else "unknown")
    projected = next(item for item in api.get_test_transfers("bybit", object())["operations"] if item["operation_id"] == forward_id)
    assert projected["can_transfer_back"] is retry_allowed
    retry_id = str(uuid.uuid4())
    if retry_allowed:
        assert api._test_transfer_back_sync("bybit", forward_id, retry_id, projected["retry_of"])["status"] == "confirmed"
        assert len(calls) == 3
    else:
        with pytest.raises(ValueError, match="already been sent back"):
            api._test_transfer_back_sync("bybit", forward_id, retry_id, returned["operation"]["operation_id"])
        assert len(calls) == 2


@pytest.mark.parametrize("route,labels", [
    ("vault_to_main_perps", {"source": "Leader Main Perps", "destination": "Hyperliquid Vault"}),
    ("main_perps_to_spot", {"source": "Leader Main Spot", "destination": "Leader Main Perps"}),
    ("perp_to_spot", {"source": "Hyperliquid Spot", "destination": "Hyperliquid Perps"}),
    ("<img src=x onerror=alert(1)>", None),
])
def test_return_projection_uses_only_fixed_labels(isolated_api, route, labels):
    """Never derive modal labels from the selected capability or untrusted stored addresses."""

    operation = _manual(isolated_api.store, "label-test")
    operation["descriptor"].update({"route": route, "source": "<script>bad</script>", "destination": "secret-address"})
    public = api._public_test_operation(operation)
    assert public["return_endpoints"] == labels
    assert "<script>" not in json.dumps(public)
    assert "secret-address" not in json.dumps(public)


@pytest.mark.parametrize("stage", ["submit", "reconcile", "persist"])
def test_unexpected_post_claim_failure_is_unknown_and_targeted_recovery_never_submits(isolated_api, monkeypatch, stage):
    """Bound exceptions after a claim, redact diagnostics, and recover only by reads."""

    monkeypatch.setattr(api, "collect_readonly_snapshot", lambda *args: _normal_snapshot())
    messages = []
    monkeypatch.setattr(api, "_log", lambda *args, **kwargs: messages.append((args, kwargs)))
    secret = "SECRET-SIGNED-PAYLOAD-DO-NOT-LOG"

    def fail(*args, **kwargs):
        """Raise a synthetic secret-bearing provider error."""

        raise RuntimeError(secret)

    monkeypatch.setattr(api, "submit_transfer", fail if stage == "submit" else lambda *args: {"status": "submitted"})
    monkeypatch.setattr(api, "reconcile_transfer", fail if stage == "reconcile" else lambda *args: {"status": "confirmed"})
    transition = isolated_api.store.transition_test_operation

    def fail_after_claim(*args, **kwargs):
        """Model a persistence failure after the external call but not at the claim."""

        if not kwargs.get("claim"):
            raise RuntimeError(secret)
        return transition(*args, **kwargs)

    if stage == "persist":
        monkeypatch.setattr(isolated_api.store, "transition_test_operation", fail_after_claim)
    operation_id = str(uuid.uuid4())
    result = api._test_transfer_sync("alice", "1", "USDC", operation_id)
    assert result["status"] == "unknown"
    assert result["operation"]["can_reconcile"] is True
    assert secret not in json.dumps([result, messages])
    before = isolated_api.store.get_test_operation(operation_id)["descriptor"]
    monkeypatch.setattr(api, "submit_transfer", fail)
    monkeypatch.setattr(api, "submit_browser_signed_transfer", fail)
    monkeypatch.setattr(api, "_reconcile_unresolved_sync", fail)
    monkeypatch.setattr(api, "reconcile_transfer", lambda *args: {"status": "confirmed"})
    result = asyncio.run(api.reconcile_test_transfer("alice", operation_id, object()))
    assert result["status"] == "confirmed"
    assert "descriptor" not in result["operation"]
    assert isolated_api.store.get_test_operation(operation_id)["descriptor"] == before
    assert asyncio.run(api.reconcile_test_transfer("alice", operation_id, object())) == result
    with pytest.raises(HTTPException) as exc:
        asyncio.run(api.reconcile_test_transfer("vault", operation_id, object()))
    assert exc.value.status_code == 404


@pytest.mark.parametrize("kind", ["test", "top_up", "browser"])
def test_shared_manual_executor_preserves_unknown_across_all_signing_paths(isolated_api, monkeypatch, kind):
    """The common post-claim guard also covers productive transfers and browser signatures."""

    operation_id = str(uuid.uuid4())
    descriptor = {"operation_id": operation_id, "route": "spot_to_perp", "amount": "1", "request": {"nonce": 123}}
    operation = isolated_api.store.create_test_operation(
        "alice", operation_id=operation_id, parent_id=None, direction="forward", route="spot_to_perp",
        descriptor=descriptor, requested_amount="1", operation_kind="top_up" if kind == "top_up" else "test",
    )

    def unexpected(*args):
        """Model an escaped exception from either signing transport."""

        raise RuntimeError("sensitive request")

    monkeypatch.setattr(api, "submit_transfer", unexpected)
    monkeypatch.setattr(api, "submit_browser_signed_transfer", unexpected)
    result = api._execute_test_operation(_user("alice"), operation, browser_signature={} if kind == "browser" else None)
    assert result["state"] == "unknown"
    assert result["descriptor"] == descriptor
    assert result["retry_safe"] is False


def test_all_api_nonce_paths_share_actual_signer_counter(isolated_api, monkeypatch):
    """Forward, return, productive, Live, and Vault leg-two paths cannot bypass allocation."""

    now = 1_800_000_000_000
    monkeypatch.setattr(api.time, "time", lambda: now / 1000)
    monkeypatch.setattr(api, "collect_readonly_snapshot", lambda user, *args: _vault_snapshot() if user.is_vault else _normal_snapshot())
    submitted = []

    def submit(user, descriptor):
        """Capture all newly allocated wire nonces without a transport."""

        submitted.append(deepcopy(descriptor))
        return {"status": "submitted"}

    monkeypatch.setattr(api, "submit_transfer", submit)
    monkeypatch.setattr(api, "reconcile_transfer", lambda user, descriptor, submission: {"status": "confirmed", "received_amount": descriptor["amount"]})
    forward = str(uuid.uuid4())
    api._test_transfer_sync("alice", "1", "USDC", forward)
    api._test_transfer_back_sync("alice", forward)
    api._top_up_sync("vault", "5", str(uuid.uuid4()), "main_perps_to_vault")
    _create_live_policy(isolated_api.store, "alice", "hyperliquid", changes={"asset": "USDC"})
    api._evaluate_live_sync("alice")
    _create_live_policy(isolated_api.store, "vault", "hyperliquid", changes={"asset": "USDC", "vault_destination": "main_spot"})
    api._evaluate_live_sync("vault")
    assert len(submitted) == 6
    assert [item["request"]["nonce"] for item in submitted] == list(range(now, now + 6))
    saved = deepcopy(submitted[0])
    api._test_transfer_sync("alice", "1", "USDC", forward)
    assert len(submitted) == 6
    assert isolated_api.store.get_test_operation(forward)["descriptor"] == saved


@pytest.mark.parametrize("before_save", [True, False])
def test_policy_save_and_live_evaluation_have_one_account_order(isolated_api, monkeypatch, before_save):
    """A save either waits for the old transfer or precedes evaluation under its new cap."""

    record = _create_live_policy(isolated_api.store, "alice", "hyperliquid", changes={"asset": "USDC"})
    body = api.PolicyRequest(policy={"single_transfer_limit_enabled": True, "single_transfer_limit": "1"}, expected_generation=record["generation"], expected_policy_fingerprint=api._policy_fingerprint(record["policy"]), confirmed_live_update=True)
    entered, release = Event(), Event()
    observed = []
    monkeypatch.setattr(api, "collect_readonly_snapshot", lambda *args: _normal_snapshot())
    prepare = api._prepare_persisted_intent

    def gate(*args, **kwargs):
        """Stop after decision but before reservation to exercise the original race."""

        entered.set()
        assert release.wait(5)
        return prepare(*args, **kwargs)

    def submit(user, descriptor):
        """Capture the effective cap at the exact simulated submission boundary."""

        policy = isolated_api.store.get_policy("alice")["policy"]
        observed.append((descriptor["amount"], policy["single_transfer_limit_enabled"]))
        return {"status": "submitted"}

    monkeypatch.setattr(api, "_prepare_persisted_intent", gate)
    monkeypatch.setattr(api, "submit_transfer", submit)
    monkeypatch.setattr(api, "reconcile_transfer", lambda *args: {"status": "confirmed"})

    async def exercise():
        """Prove serialization without sleep-based worker scheduling assumptions."""

        if before_save:
            await api.save_policy("alice", body, object())
            release.set()
            await api._evaluate_live_user("alice")
        else:
            evaluation = asyncio.create_task(api._evaluate_live_user("alice"))
            assert await asyncio.to_thread(entered.wait, 5)
            saving = asyncio.create_task(api.save_policy("alice", body, object()))
            await asyncio.sleep(0)
            assert not saving.done()
            assert not isolated_api.store.get_policy("alice")["policy"]["single_transfer_limit_enabled"]
            release.set()
            await asyncio.gather(evaluation, saving)

    try:
        asyncio.run(exercise())
    finally:
        release.set()
    assert observed == ([("1", True)] if before_save else [("100", False)])


def test_cancelled_save_holds_lock_until_worker_finishes_but_other_accounts_run(isolated_api, monkeypatch):
    """Cancellation cannot abandon an owned save or serialize unrelated accounts."""

    entered, release, other = Event(), Event(), Event()

    def save(user, body):
        """Keep one account worker alive while a second account completes."""

        if user == "alice":
            entered.set()
            assert release.wait(5)
        else:
            other.set()
        return {}

    monkeypatch.setattr(api, "_save_policy_sync", save)

    async def exercise():
        """Cancel the waiter and verify ownership remains until actual completion."""

        saving = asyncio.create_task(api.save_policy("alice", api.PolicyRequest(), object()))
        assert await asyncio.to_thread(entered.wait, 5)
        saving.cancel()
        await asyncio.sleep(0)
        assert api._EVALUATION_LOCKS["alice"].locked()
        await api.save_policy("bybit", api.PolicyRequest(), object())
        assert other.is_set()
        assert not saving.done()
        saving.cancel()
        await asyncio.sleep(0)
        assert not saving.done()
        assert api._EVALUATION_LOCKS["alice"].locked()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await saving
        assert not api._EVALUATION_LOCKS["alice"].locked()
        assert not api._ACTIVE_OPERATION_TASKS

    try:
        asyncio.run(exercise())
    finally:
        release.set()


@pytest.mark.parametrize("forwarding", [False, True])
@pytest.mark.parametrize("entry", ["startup", "explicit"])
@pytest.mark.parametrize("cancel", [False, True])
def test_recovery_submissions_and_policy_save_share_account_lock(isolated_api, monkeypatch, forwarding, entry, cancel):
    """Prepared recovery and missing Vault leg-two recovery cannot run over a completed save."""

    name = "vault" if forwarding else "alice"
    user = isolated_api.users.find_user(name)
    store = isolated_api.store
    record = _create_live_policy(store, name, "hyperliquid", changes={"asset": "USDC", "vault_destination": "main_spot" if forwarding else "main_perps"})
    snapshot = _vault_snapshot() if forwarding else _normal_snapshot()
    decision = store.evaluate_live(name, cumulative_net_pnl="100", max_transferable="200")
    operation_id = "recovery-root-leg1" if forwarding else "recovery-normal"
    intent = api._prepare_persisted_intent(
        user, record, snapshot, amount="100", operation_id=operation_id,
        parent_id="recovery-root" if forwarding else None, leg=1,
        route="vault_to_main_perps" if forwarding else "perp_to_spot", reservation_guard=decision["reservation_guard"],
    )
    if forwarding:
        store.transition_live_intent(operation_id, "submitting", submission={"status": "submitted", "reconciliation": {"status": "confirmed", "received_amount": "100"}}, claim=True)
        store.reconcile_live_intent(operation_id, {"status": "confirmed"}, settle_financial=False)
    entered, release = Event(), Event()
    observed = []

    def submit(user, descriptor):
        """Delay only the actual recovered submission, retaining the real recovery chain."""

        entered.set()
        assert release.wait(5)
        observed.append((descriptor["amount"], store.get_policy(name)["policy"]["single_transfer_limit_enabled"]))
        return {"status": "submitted"}

    monkeypatch.setattr(api, "submit_transfer", submit)
    monkeypatch.setattr(api, "collect_readonly_snapshot", lambda *args: snapshot)
    monkeypatch.setattr(api, "reconcile_transfer", lambda *args: {"status": "confirmed", "received_amount": "100"})
    monkeypatch.setattr(api, "_STOPPING", True)
    body = api.PolicyRequest(policy={"single_transfer_limit_enabled": True, "single_transfer_limit": "1"}, expected_generation=record["generation"], expected_policy_fingerprint=api._policy_fingerprint(record["policy"]), confirmed_live_update=True)

    async def exercise():
        """Run the production scheduler entry or explicit route while saving both accounts."""

        recovery = asyncio.create_task(api._scheduler_loop() if entry == "startup" else api.reconcile_intent(name, intent["operation_id"], object()))
        assert await asyncio.to_thread(entered.wait, 5)
        saving = asyncio.create_task(api.save_policy(name, body, object()))
        await asyncio.sleep(0)
        assert not saving.done()
        assert api._EVALUATION_LOCKS[name].locked()
        await api.save_policy("bybit", api.PolicyRequest(), object())
        assert not saving.done()
        if cancel:
            recovery.cancel()
            await asyncio.sleep(0)
            recovery.cancel()
            await asyncio.sleep(0)
            assert not recovery.done()
            assert not saving.done()
            assert api._EVALUATION_LOCKS[name].locked()
        release.set()
        results = await asyncio.gather(recovery, saving, return_exceptions=True)
        assert isinstance(results[0], asyncio.CancelledError) if cancel else not isinstance(results[0], BaseException)
        assert not isinstance(results[1], BaseException)

    try:
        asyncio.run(exercise())
    finally:
        release.set()
    assert observed == [("100", False)]
    assert store.get_policy(name)["policy"]["single_transfer_limit"] == "1"
    assert not api._ACTIVE_OPERATION_TASKS


@pytest.mark.parametrize("page", [api.get_main_page, api.get_transfers_main_page])
@pytest.mark.parametrize("server", [("example.com", 443), ("::1", 8000)])
@pytest.mark.parametrize("prefix", ["", "/pbgui", "/mount space"])
def test_pages_use_mounted_same_origin_urls(page, server, prefix):
    """IPv6 and proxy mounts never leak an unbracketed authority into API URLs."""

    request = Request({"type": "http", "scheme": "https", "server": server, "root_path": prefix, "path": "/main_page", "headers": [], "query_string": b""})
    response = page(request, object())
    source = response.body.decode()
    encoded = prefix.replace(" ", "%20")
    # The shared helper escapes percent signs for inline-script safety.
    escaped = encoded.replace("%", "\\u0025")
    assert f'window.API_BASE = "{escaped}/api/profit-sweep"' in source
    assert f'window.PBGUI_BASE_PREFIX = "{escaped}"' in source
    assert f'src="{encoded}/app/pbgui_nav.js' in source
    assert "https://::1" not in source
    assert "%%API_BASE%%" not in source
    assert response.headers["cache-control"] == "no-store"
