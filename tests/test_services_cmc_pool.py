"""Focused contracts for the authenticated Services CMC pool API and UI."""

from __future__ import annotations

import json
from pathlib import Path
import threading
import time
from types import SimpleNamespace

import pytest
from fastapi.routing import APIRoute
from pydantic import ValidationError

from api.auth import require_auth
import api.services as services
from cluster_credential_publisher import CredentialPublicationError
from credential_store import CredentialStore


class _Publisher:
    """Record cluster publication calls without creating cluster key material."""

    def __init__(self) -> None:
        self.calls: list[tuple] = []

    def publish_cmc(self, credential_id: str, generation=None, *, state=None, pool_metadata=None):
        """Record a CMC publication."""
        self.calls.append(("publish", credential_id, generation, state, pool_metadata))
        return {"status": "published"}

    def disable_cmc(self, credential_id: str):
        """Record a CMC disable operation."""
        self.calls.append(("disable", credential_id))
        return {"status": "disabled"}

    def publish_tombstone(self, credential_id: str, kind: str):
        """Record a credential tombstone."""
        self.calls.append(("tombstone", credential_id, kind))
        return {"status": "tombstoned"}


def _install_store(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> tuple[CredentialStore, _Publisher]:
    """Bind Services handlers to an isolated credential store and fake publisher."""
    store = CredentialStore(tmp_path / "credentials")
    publisher = _Publisher()
    monkeypatch.setattr(services, "_cmc_credential_store", lambda: store)
    monkeypatch.setattr(services, "_cmc_credential_publisher", lambda selected: publisher)
    return store, publisher


def _assert_secret_free(payload: object, *secrets: str) -> None:
    """Assert serialized API output contains none of the submitted secrets."""
    encoded = json.dumps(payload, sort_keys=True)
    for secret in secrets:
        assert secret not in encoded
    assert "ciphertext" not in encoded.lower()


def test_cmc_pool_routes_have_exact_methods_and_shared_auth() -> None:
    """Every required CMC pool route uses the shared authentication dependency."""
    expected = {
        ("/cmc-pool", "GET"),
        ("/cmc-pool/keys", "POST"),
        ("/cmc-pool/keys/{key_id}", "PATCH"),
        ("/cmc-pool/keys/{key_id}/rotate", "POST"),
        ("/cmc-pool/keys/{key_id}/disable", "POST"),
        ("/cmc-pool/keys/{key_id}", "DELETE"),
        ("/cmc-pool/usage", "GET"),
        ("/cmc-pool/leases", "GET"),
        ("/cmc-pool/operations/{operation_id}", "GET"),
        ("/cmc-pool/authority/transfer", "POST"),
    }
    found: set[tuple[str, str]] = set()
    for route in services.router.routes:
        if not isinstance(route, APIRoute) or not route.path.startswith("/cmc-pool"):
            continue
        for method in route.methods or set():
            if method in {"GET", "POST", "PATCH", "DELETE"}:
                found.add((route.path, method))
        assert any(dependency.call is require_auth for dependency in route.dependant.dependencies)
    assert found == expected


def test_cmc_pool_crud_is_secret_free_and_keeps_external_shared_key_active(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Create, patch, rotate, disable, and delete use the store and cluster publisher."""
    store, publisher = _install_store(monkeypatch, tmp_path)

    created = services.create_cmc_pool_key(
        services.CmcPoolKeyCreate(
            api_key="create-secret",
            label="External shared",
            active=True,
            imported=True,
            shared=True,
        ),
        SimpleNamespace(),
    )
    credential_id = created["credential"]["id"]
    stored = store.get_cmc(credential_id)
    assert stored["active"] is True
    assert stored["origin"] == "imported"
    assert stored["shared"] is True
    assert created["credential"]["source"] == "imported"
    assert publisher.calls[-1][3] == "active"

    patched = services.patch_cmc_pool_key(
        credential_id,
        services.CmcPoolKeyPatch(label="Renamed", shared=False),
        SimpleNamespace(),
    )
    assert patched["credential"]["label"] == "Renamed"
    assert patched["credential"]["shared"] is False

    rotated = services.rotate_cmc_pool_key(
        credential_id,
        services.CmcPoolKeyRotate(api_key="rotate-secret"),
        SimpleNamespace(),
    )
    assert rotated["credential"]["generation"] == 2
    assert store.load_cmc_key(credential_id) == "rotate-secret"

    disabled = services.disable_cmc_pool_key(credential_id, SimpleNamespace())
    assert disabled["credential"]["active"] is False
    assert publisher.calls[-1] == ("disable", credential_id)

    reenabled = services.patch_cmc_pool_key(
        credential_id,
        services.CmcPoolKeyPatch(active=True),
        SimpleNamespace(),
    )
    assert reenabled["credential"]["active"] is True
    assert publisher.calls[-1][3] == "active"

    deleted = services.delete_cmc_pool_key(credential_id, SimpleNamespace())
    assert deleted["ok"] is True
    assert deleted["key_id"] == credential_id
    assert deleted["publication_status"] == "tombstoned"
    assert deleted["operation_id"]
    assert store.list_cmc() == []
    assert publisher.calls[-1] == ("tombstone", credential_id, "cmc_api_key")
    for payload in (created, patched, rotated, disabled, reenabled, deleted):
        _assert_secret_free(payload, "create-secret", "rotate-secret")


def test_cmc_pool_models_separate_secret_and_metadata_contracts() -> None:
    """Only create and rotate bodies accept an API key secret."""
    with pytest.raises(ValidationError):
        services.CmcPoolKeyPatch(api_key="must-not-be-accepted")
    with pytest.raises(ValidationError):
        services.PBCoinDataSettings(api_key="legacy-secret")
    assert set(services.PBCoinDataSettings.model_fields) == {
        "fetch_limit", "fetch_interval", "metadata_interval", "mapping_interval",
    }


def test_cmc_pool_status_usage_and_legacy_route_are_secret_free(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pool, usage, and compatibility status all derive from CmcPoolClient.status."""
    status = {
        "soft_credit_limit": 100,
        "day": "2026-07-14",
        "active_credentials": 1,
        "keys": [{
            "id": "cmc_0123456789abcdef0123456789abcdef",
            "label": "Main",
            "active": True,
            "origin": "imported",
            "shared": True,
            "generation": 3,
            "status": "active",
            "used_credits": 12,
            "provider_remaining": 88,
            "provider_used": 85,
            "provider_limit": 100,
            "desired_state": "active",
            "desired_generation": 4,
            "quota_domain_id": "quota-1",
            "api_key": "status-secret",
        }],
    }
    monkeypatch.setattr(services, "_cmc_pool_client", lambda: SimpleNamespace(status=lambda: status))

    usage = services.get_cmc_pool_usage(SimpleNamespace())
    pool = services.get_cmc_pool(SimpleNamespace())
    legacy = services.get_pbcoindata_key_status(SimpleNamespace())

    assert usage["keys"][0]["provider_remaining"] == 88
    assert usage["keys"][0]["provider_used"] == 85
    assert usage["keys"][0]["provider_limit"] == 100
    assert usage["keys"][0]["materialized_generation"] == 3
    assert usage["keys"][0]["desired_generation"] == 4
    assert usage["keys"][0]["local_state"] == "active"
    assert pool["ready"] is True
    assert pool["health"] == "healthy"
    assert pool["warnings"] == ["Main: monthly CMC usage is at or above 80%"]
    assert legacy["ok"] is True
    for payload in (usage, pool, legacy):
        _assert_secret_free(payload, "status-secret")


def test_cmc_pool_leases_are_transformed_to_public_summary(monkeypatch: pytest.MonkeyPatch) -> None:
    """Lease diagnostics expose counters and bounded public lease fields only."""
    state = {
        "keys": {"cmc_id": {}},
        "requests": {
            "request-1": {"lease": {
                "lease_id": "lease-1",
                "credential_id": "cmc_id",
                "secret_generation": 2,
                "quota_domain_id": "quota-1",
                "recipient": "node-a",
                "credits_micros": 2_500_000,
                "request_count": 1,
                "granted_at": 10,
                "expires_at": 20,
                "api_key": "lease-secret",
            }},
        },
        "leases": {"lease-1": {"terminal": False}},
        "provider_events": {},
        "domains": {
            "quota-1": {
                "quota_domain_id": "quota-1",
                "authority_epoch": 3,
                "day": "2026-07-15",
                "month": "2026-07",
                "day_reserved_credits_micros": 10_000_000,
                "day_used_credits_micros": 75_000_000,
                "month_reserved_credits_micros": 5_000_000,
                "month_used_credits_micros": 80_000_000,
                "uncertain_credits_micros": 2_500_000,
                "concurrent_leases": 1,
                "limits": {
                    "daily_credits_micros": 100_000_000,
                    "monthly_credits_micros": 100_000_000,
                },
                "provider_remaining_micros": 15_000_000,
                "provider_reset_at": 30,
            },
        },
    }
    monkeypatch.setattr(services, "_cmc_lease_authority", lambda: SimpleNamespace(status=lambda: state))
    monkeypatch.setattr(services, "read_cmc_cluster_snapshot", lambda _root: {
        "cluster_nodes": {"nodes": {"node-a": {"pbname": "master-a"}}},
        "desired_state": {"cmc_pool": {"authorities": {
            "quota-1": {"authority_node_id": "node-a", "authority_epoch": 3, "updated_at": 90},
        }}},
    })
    monkeypatch.setattr(services, "read_local_identity", lambda _root: {"node_id": "node-a"})
    monkeypatch.setattr(services, "get_monitor", lambda: None)
    monkeypatch.setattr(services.time, "time", lambda: 100)

    payload = services.get_cmc_pool_leases(SimpleNamespace())

    assert payload["authority"]["active_leases"] == 1
    assert payload["leases"][0]["credits"] == 2.5
    assert payload["leases"][0]["authority_epoch"] is None
    assert payload["key_usage"][0]["reserved_credits"] == 0
    assert payload["domains"][0]["authority_node"] == "master-a"
    assert payload["domains"][0]["authority_reachable"] is True
    assert payload["domains"][0]["authority_updated_at"] == 90
    assert payload["domains"][0]["authority_state_age_seconds"] == 10
    assert payload["domains"][0]["uncertain_credits"] == 2.5
    assert payload["domains"][0]["provider_remaining"] == 15
    assert len(payload["warnings"]) == 2
    _assert_secret_free(payload, "lease-secret")


@pytest.mark.parametrize("credential_id", ["../cmc_bad", "cmc_bad", "tradfi_0123456789abcdef0123456789abcdef"])
def test_cmc_pool_rejects_invalid_ids_with_422(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    credential_id: str,
) -> None:
    """CredentialStore validation rejects traversal and wrong-kind IDs at the API boundary."""
    _install_store(monkeypatch, tmp_path)
    with pytest.raises(services.HTTPException) as exc_info:
        services.disable_cmc_pool_key(credential_id, SimpleNamespace())
    assert exc_info.value.status_code == 422


def test_cmc_pool_maps_missing_and_publication_errors(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Missing credentials are 404 and cluster publication conflicts are 409."""
    store, _publisher = _install_store(monkeypatch, tmp_path)
    missing_id = "cmc_0123456789abcdef0123456789abcdef"
    with pytest.raises(services.HTTPException) as missing:
        services.disable_cmc_pool_key(missing_id, SimpleNamespace())
    assert missing.value.status_code == 404

    class FailingPublisher:
        """Publisher that reports a cluster state conflict."""

        def publish_cmc(self, *args, **kwargs):
            """Raise a publication conflict."""
            raise CredentialPublicationError("CMC pool entry is conflicted")

    monkeypatch.setattr(services, "_cmc_credential_publisher", lambda selected: FailingPublisher())
    with pytest.raises(services.HTTPException) as conflict:
        services.create_cmc_pool_key(
            services.CmcPoolKeyCreate(api_key="conflict-secret"),
            SimpleNamespace(),
        )
    assert conflict.value.status_code == 409
    assert "conflicted" in str(conflict.value.detail)
    assert len(store.list_cmc()) == 1


def test_pbcoindata_settings_return_pool_summary_without_legacy_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """PBCoinData settings contain intervals and a secret-free pool summary only."""
    class FakeCoinData:
        """Minimal settings source for the Services handler."""

        fetch_limit = 5000
        fetch_interval = 24
        metadata_interval = 1
        mapping_interval = 24

    monkeypatch.setattr("PBCoinData.CoinData", FakeCoinData)
    monkeypatch.setattr(services, "_cmc_pool_payload", lambda: {"ready": True, "active_credentials": 1})

    payload = services.get_pbcoindata_settings(SimpleNamespace())

    assert "api_key" not in payload
    assert payload["cmc_pool"] == {"ready": True, "active_credentials": 1}


def test_create_failure_stays_unselectable_and_same_operation_resumes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A failed active publication leaves one pending record and a retry commits that record."""

    store = CredentialStore(tmp_path / "credentials")

    class FlakyPublisher(_Publisher):
        """Fail the first final activation after accepting pending publication."""

        def __init__(self) -> None:
            super().__init__()
            self.failed = False

        def publish_cmc(self, credential_id: str, generation=None, *, state=None, pool_metadata=None):
            if state == "active" and not self.failed:
                self.failed = True
                raise RuntimeError("activation failed")
            return super().publish_cmc(credential_id, generation, state=state, pool_metadata=pool_metadata)

    publisher = FlakyPublisher()
    monkeypatch.setattr(services, "_cmc_credential_store", lambda: store)
    monkeypatch.setattr(services, "_cmc_credential_publisher", lambda _store: publisher)
    request = services.CmcPoolKeyCreate(
        api_key="retry-secret",
        operation_id="create-retry-1",
    )

    with pytest.raises(services.HTTPException) as first:
        services.create_cmc_pool_key(request, SimpleNamespace())
    assert first.value.status_code == 500
    pending = store.list_cmc()
    assert len(pending) == 1
    assert pending[0]["pending"] is True
    assert store.active_cmc_credentials() == []

    resumed = services.create_cmc_pool_key(request, SimpleNamespace())
    assert resumed["credential"]["id"] == pending[0]["id"]
    assert len(store.list_cmc()) == 1
    assert store.active_cmc_credentials()[0]["api_key"] == "retry-secret"


def test_rotate_retry_returns_durable_same_result_without_second_generation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """An uncertain rotate retry with the same client ID cannot rotate twice."""

    store, _publisher = _install_store(monkeypatch, tmp_path)
    record = store.create_cmc("old-secret")
    request = services.CmcPoolKeyRotate(
        api_key="replacement-secret",
        operation_id="rotate-idempotent-1",
    )

    first = services.rotate_cmc_pool_key(record["id"], request, SimpleNamespace())
    second = services.rotate_cmc_pool_key(record["id"], request, SimpleNamespace())
    status = services.get_cmc_pool_operation("rotate-idempotent-1", SimpleNamespace())

    assert second == first
    assert store.get_cmc(record["id"])["generation"] == 2
    assert status["status"] == "complete"
    assert status["result"] == first


def test_patch_and_disable_hold_one_shared_credential_mutation_lock(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Concurrent active-state handlers never overlap their publication transactions."""

    store = CredentialStore(tmp_path / "credentials")
    record = store.create_cmc("race-secret", active=False)

    class ObservedPublisher(_Publisher):
        """Measure publication overlap across two handler threads."""

        def __init__(self) -> None:
            super().__init__()
            self.current = 0
            self.maximum = 0
            self.guard = threading.Lock()

        def _observe(self, callback):
            with self.guard:
                self.current += 1
                self.maximum = max(self.maximum, self.current)
            time.sleep(0.02)
            try:
                return callback()
            finally:
                with self.guard:
                    self.current -= 1

        def publish_cmc(self, credential_id: str, generation=None, *, state=None, pool_metadata=None):
            return self._observe(
                lambda: super(ObservedPublisher, self).publish_cmc(
                    credential_id,
                    generation,
                    state=state,
                    pool_metadata=pool_metadata,
                )
            )

        def disable_cmc(self, credential_id: str):
            return self._observe(lambda: super(ObservedPublisher, self).disable_cmc(credential_id))

    publisher = ObservedPublisher()
    monkeypatch.setattr(services, "_cmc_credential_store", lambda: store)
    monkeypatch.setattr(services, "_cmc_credential_publisher", lambda _store: publisher)
    errors: list[Exception] = []

    def activate() -> None:
        try:
            services.patch_cmc_pool_key(
                record["id"],
                services.CmcPoolKeyPatch(active=True),
                SimpleNamespace(),
            )
        except Exception as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    def disable() -> None:
        try:
            services.disable_cmc_pool_key(record["id"], SimpleNamespace())
        except Exception as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    threads = [threading.Thread(target=activate), threading.Thread(target=disable)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)

    assert errors == []
    assert publisher.maximum == 1


def test_authority_transfer_returns_operation_id_and_refreshed_pool(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The authenticated authority transfer returns a stable operation ID and no secret."""

    store = CredentialStore(tmp_path / "credentials")

    class AuthorityPublisher:
        """Return one signed authority transition without credential material."""

        def set_cmc_authority(self, domain_id, node_id, *, expected_epoch=None):
            assert domain_id == "quota-a"
            assert node_id == "node-b"
            assert expected_epoch == 2
            return {
                "status": "transferred",
                "authority_node_id": node_id,
                "authority_epoch": 3,
                "operation_id": "cluster-authority-op",
            }

    monkeypatch.setattr(services, "_cmc_credential_store", lambda: store)
    monkeypatch.setattr(services, "_cmc_credential_publisher", lambda _store: AuthorityPublisher())
    monkeypatch.setattr(services, "_cmc_pool_payload", lambda: {"ready": True, "keys": []})

    payload = services.transfer_cmc_pool_authority(
        services.CmcAuthorityTransfer(
            quota_domain_id="quota-a",
            authority_node_id="node-b",
            expected_epoch=2,
            operation_id="browser-authority-op",
        ),
        SimpleNamespace(),
    )

    assert payload["operation_id"] == "browser-authority-op"
    assert payload["cluster_operation_id"] == "cluster-authority-op"
    assert payload["authority"]["authority_epoch"] == 3
    assert "api_key" not in json.dumps(payload)


def test_services_frontend_uses_pool_manager_without_native_dialogs_or_legacy_key_form() -> None:
    """Services UI uses the pool endpoints, selected rows, and explicit shared dialogs."""
    source = Path("frontend/services_monitor.html").read_text(encoding="utf-8")

    assert 'id="coindata-api-key"' not in source
    assert "loadCmcKeyStatus" not in source
    assert "'/cmc-pool/keys'" in source
    assert "'/cmc-pool/usage'" not in source
    assert "'/cmc-pool/leases'" in source
    assert "_cmcUsage = _cmcPool;" in source
    assert 'id="cmc-key-secret"' in source
    assert "if (input && input.value === context.secretValue) input.value = '';" in source
    assert ".cmc-table tbody tr.selected td" in source
    assert "window.PBGuiDialogs.confirm" in source
    assert "method: _cmcModalMode === 'edit' ? 'PATCH' : 'POST'" in source
    assert "Re-enable" in source
    assert "Materialized / Desired" in source
    assert "Uncertain Spend" in source
    assert "Provider Used" in source
    assert "Provider Limit" in source
    assert "cmcNumber(item.provider_used)" in source
    assert "cmcNumber(item.provider_limit)" in source
    assert "Authority Epoch" in source
    assert "_cmcLoadGeneration" in source
    assert "_cmcMutationBusy" in source
    assert "Another credential mutation is already in progress" in source
    assert "_cmcPendingMutation" in source
    assert "sameCmcMutation" in source
    assert "resolvePendingCmcMutation" in source
    assert "'/cmc-pool/operations/' + encodeURIComponent(pending.operationId)" in source
    assert "record.status === 'complete'" in source
    assert "clearCmcMutationContext(resolved.context, true)" in source
    assert "clearCmcMutationContext(resolved.context, false)" in source
    assert "Retry will check operation" in source
    assert "input.value === context.secretValue" in source
    assert "cancelCmcMutationContext('key')" in source
    assert "cancelCmcMutationContext('authority')" in source
    assert "identifierField: 'request_id'" in source
    assert "action: 'cmc_delete'" in source
    assert "action: enable ? 'cmc_patch' : 'cmc_disable'" in source
    assert "operation_id=' + encodeURIComponent(context.operationId)" not in source
    assert "sessionStorage" not in source
    assert "localStorage" not in source
    assert "/cmc-pool/authority/transfer" in source
    assert "cmc-authority-target" in source
    assert "authority_state_age_seconds" in source
    assert "window.confirm(" not in source
    assert "window.alert(" not in source
    assert "cmc-key-modal').addEventListener('click'" not in source
