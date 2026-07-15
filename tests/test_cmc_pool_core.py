"""Tests for local CMC pool fairness, accounting, and failover state."""

from __future__ import annotations

import json
import multiprocessing
import os
from pathlib import Path
import stat

import pytest

from cmc_pool import CmcPoolClient, CmcPoolExhaustedError, estimate_endpoint_credits
from credential_store import CredentialStore
from master.cluster_state import ensure_local_identity


def _strict_desired_state(
    tmp_path: Path,
    secrets: dict,
    entries: dict,
) -> dict:
    """Build strict local lifecycle metadata around mutable desired mappings."""

    identity = ensure_local_identity(tmp_path / "cluster", role="master", pbname="local")
    node_id = identity["node_id"]
    for secret in secrets.values():
        secret.update({
            "recipient_generation": 1,
            "recipient_ids": [node_id],
            "secret_kind": "cmc_api_key",
        })
    return {
        "cluster_nodes": {
            "credential_membership_generation": 1,
            "nodes": {node_id: {
                "role": "master",
                "credential_protocol_version": 2,
                "credential_capable": True,
                "signing_key_id": "ed25519:test",
                "encryption_key_id": "x25519:test",
            }},
        },
        "desired_state": {
            "secrets": secrets,
            "cmc_pool": {"entries": entries, "authorities": {}},
            "credential_materialization_acks": {node_id: {
                "membership_generation": 1,
                "credential_generations": {
                    credential_id: int(secret["generation"])
                    for credential_id, secret in secrets.items()
                },
                "recipient_generations": {credential_id: 1 for credential_id in secrets},
            }},
        },
    }


class _Clock:
    """Controllable timestamp source for deterministic cooldown tests."""

    def __init__(self, value: float = 1_700_000_000.0) -> None:
        """Initialize the clock at a fixed epoch."""
        self.value = value

    def __call__(self) -> float:
        """Return the current test timestamp."""
        return self.value


class _LeaseProvider:
    """Optional lease provider that chooses the final eligible key."""

    def __init__(self) -> None:
        """Initialize recorded lease and settlement calls."""
        self.candidates = []
        self.settlements = []

    def acquire(self, candidates, **_kwargs):
        """Lease the last candidate using a test token."""
        self.candidates = candidates
        return {"credential_id": candidates[-1]["id"], "lease_token": "lease-1"}

    def settle(self, token, **kwargs) -> None:
        """Record best-effort lease settlement."""
        self.settlements.append((token, kwargs))


class _BrokenLeaseProvider:
    """Lease provider that demonstrates mandatory local fallback."""

    def acquire(self, _candidates, **_kwargs):
        """Fail to provide a lease."""
        raise OSError("lease service unavailable")


def _acquire_in_process(root: str, state_root: str, queue) -> None:
    """Acquire once in a child process and return the selected credential ID."""
    try:
        pool = CmcPoolClient(
            credentials_root=root,
            state_root=state_root,
            soft_credit_limit=1000,
        )
        acquisition = pool.acquire("/v1/cryptocurrency/map", estimated_credits=1)
        queue.put((True, acquisition.credential_id))
    except Exception as exc:  # pragma: no cover - surfaced by the parent assertion
        queue.put((False, repr(exc)))


def _pool_with_keys(
    tmp_path: Path,
    count: int,
    *,
    clock=None,
    soft_credit_limit: float = 100,
    lease_provider=None,
) -> tuple[CredentialStore, CmcPoolClient, list[dict]]:
    """Create a pool fixture with local, imported, and shared key metadata."""
    store = CredentialStore(tmp_path / "credentials")
    records = []
    for index in range(count):
        records.append(
            store.create_cmc(
                f"secret-{index}",
                label=f"key-{index}",
                origin="imported" if index == 1 else "local",
                shared=index == 2,
            )
        )
    pool = CmcPoolClient(
        store,
        state_root=tmp_path / "pool-state",
        soft_credit_limit=soft_credit_limit,
        cooldown_seconds=30,
        lease_provider=lease_provider,
        clock=clock or _Clock(),
    )
    return store, pool, records


def test_selection_is_fair_across_local_imported_and_shared_keys(tmp_path: Path) -> None:
    """Equal-cost acquisitions round-robin over every active credential source."""
    _store, pool, records = _pool_with_keys(tmp_path, 3)

    selected = [
        pool.acquire("/v1/cryptocurrency/map", estimated_credits=1).credential_id
        for _ in range(9)
    ]

    assert set(selected) == {record["id"] for record in records}
    assert {credential_id: selected.count(credential_id) for credential_id in set(selected)} == {
        record["id"]: 3 for record in records
    }


def test_optional_lease_is_preferred_and_settled_without_secrets(tmp_path: Path) -> None:
    """A valid lease wins selection while lease payloads remain metadata-only."""
    lease_provider = _LeaseProvider()
    _store, pool, _records = _pool_with_keys(tmp_path, 2, lease_provider=lease_provider)

    acquisition = pool.acquire("/v1/cryptocurrency/quotes/latest")
    result = pool.settle(acquisition, status_code=200, actual_credits=1)

    assert acquisition.credential_id == lease_provider.candidates[-1]["id"]
    assert all("api_key" not in candidate for candidate in lease_provider.candidates)
    assert lease_provider.settlements[0][0] == "lease-1"
    assert result["last_outcome"] == "success"


def test_broken_optional_lease_falls_back_to_local_selection(tmp_path: Path) -> None:
    """Lease service failure never prevents local key acquisition."""
    _store, pool, records = _pool_with_keys(
        tmp_path,
        1,
        lease_provider=_BrokenLeaseProvider(),
    )

    acquisition = pool.acquire("/v1/cryptocurrency/map")

    assert acquisition.credential_id == records[0]["id"]
    assert acquisition.lease_token is None


def test_invalid_rate_limited_and_exhausted_keys_fail_over(tmp_path: Path) -> None:
    """Provider outcomes remove failed keys from selection using distinct states."""
    clock = _Clock()
    _store, pool, _records = _pool_with_keys(tmp_path, 3, clock=clock)
    invalid = pool.acquire("/v1/cryptocurrency/map")
    pool.settle(invalid, status_code=401)

    limited = pool.acquire("/v1/cryptocurrency/map")
    assert limited.credential_id != invalid.credential_id
    pool.settle(limited, status_code=429, retry_after=30)

    exhausted = pool.acquire("/v1/cryptocurrency/map")
    assert exhausted.credential_id not in {invalid.credential_id, limited.credential_id}
    pool.settle(exhausted, status_code=200, provider_status={"remaining": 0})

    with pytest.raises(CmcPoolExhaustedError):
        pool.acquire("/v1/cryptocurrency/map")
    statuses = {item["id"]: item["status"] for item in pool.status()["keys"]}
    assert statuses[invalid.credential_id] == "invalid"
    assert statuses[limited.credential_id] == "cooldown"
    assert statuses[exhausted.credential_id] == "exhausted"


def test_transient_error_cooldown_expires(tmp_path: Path) -> None:
    """A transport failure cools a key temporarily rather than invalidating it."""
    clock = _Clock()
    _store, pool, records = _pool_with_keys(tmp_path, 1, clock=clock)
    failed = pool.acquire("/v1/cryptocurrency/map")
    pool.settle(failed, error=TimeoutError("provider timeout"))

    with pytest.raises(CmcPoolExhaustedError):
        pool.acquire("/v1/cryptocurrency/map")
    clock.value += 31

    assert pool.acquire("/v1/cryptocurrency/map").credential_id == records[0]["id"]


def test_generation_replacement_recovers_an_invalid_credential(tmp_path: Path) -> None:
    """Invalidation applies only to the failed immutable key generation."""
    store, pool, records = _pool_with_keys(tmp_path, 1)
    failed = pool.acquire("/v1/cryptocurrency/map")
    pool.settle(failed, status_code=403)
    store.update_cmc(records[0]["id"], api_key="replacement-secret")

    replacement = pool.acquire("/v1/cryptocurrency/map")

    assert replacement.credential_id == records[0]["id"]
    assert replacement.generation == 2
    assert replacement.api_key == "replacement-secret"


def test_soft_budget_and_retry_attempts_persist_across_clients(tmp_path: Path) -> None:
    """Each retry is a new acquisition and shared state enforces the local budget."""
    store, first, _records = _pool_with_keys(tmp_path, 1, soft_credit_limit=2)
    attempt_one = first.acquire("/v1/cryptocurrency/map", estimated_credits=1)
    first.settle(attempt_one, status_code=500, retry_after=0)
    second = CmcPoolClient(
        store,
        state_root=tmp_path / "pool-state",
        soft_credit_limit=2,
        cooldown_seconds=30,
        clock=_Clock(),
    )
    attempt_two = second.acquire("/v1/cryptocurrency/map", estimated_credits=1)

    assert attempt_one.acquisition_id != attempt_two.acquisition_id
    assert second.status()["keys"][0]["used_credits"] == 2
    with pytest.raises(CmcPoolExhaustedError):
        second.acquire("/v1/cryptocurrency/map", estimated_credits=1)


def test_provider_headers_update_usage_status_without_secret_leakage(tmp_path: Path) -> None:
    """CMC credit headers settle counters while status and repr omit API keys."""
    _store, pool, _records = _pool_with_keys(tmp_path, 1)
    acquisition = pool.acquire("/v1/cryptocurrency/listings/latest", {"limit": 250})
    pool.settle(
        acquisition,
        status_code=200,
        actual_credits=4,
        provider_status={
            "X-RateLimit-Credit-Limit": "10000",
            "X-RateLimit-Credit-Remaining": "9996",
            "X-RateLimit-Credit-Used": "4",
        },
    )

    serialized = json.dumps(pool.status())
    assert "secret-0" not in serialized
    assert "secret-0" not in repr(acquisition)
    assert "api_key" not in serialized
    key_status = pool.status()["keys"][0]
    assert key_status["used_credits"] == 4
    assert key_status["provider_remaining"] == 9996
    assert key_status["provider_limit"] == 10000


def test_zero_credit_acquisition_counts_attempt_and_settles_lease_once(tmp_path: Path) -> None:
    """Free provider calls reserve no credits but retain attempt and exact lease accounting."""
    lease_provider = _LeaseProvider()
    _store, pool, _records = _pool_with_keys(tmp_path, 1, lease_provider=lease_provider)

    acquisition = pool.acquire("/v1/key/info", estimated_credits=0)
    pool.settle(acquisition, status_code=200, actual_credits=0)
    pool.settle(acquisition, status_code=200, actual_credits=0)

    key_status = pool.status()["keys"][0]
    assert key_status["used_credits"] == 0
    assert key_status["total_acquisitions"] == 1
    assert len(lease_provider.settlements) == 1


@pytest.mark.parametrize(
    ("endpoint", "params", "expected"),
    [
        ("/v1/cryptocurrency/listings/latest", {"limit": 250}, 2.0),
        ("/v2/cryptocurrency/quotes/latest", {"id": "1,2", "convert": "USD,EUR"}, 2.0),
        ("/v1/cryptocurrency/map", {"limit": 1000}, 10.0),
        ("/v1/tools/price-conversion", {}, 1.0),
    ],
)
def test_endpoint_credit_estimation(endpoint: str, params: dict, expected: float) -> None:
    """Endpoint estimates account for result cardinality and conversions."""
    assert estimate_endpoint_credits(endpoint, params) == expected


def test_multiprocess_acquisitions_preserve_usage_and_fairness(tmp_path: Path) -> None:
    """Cross-process state locking prevents lost usage reservations."""
    root = tmp_path / "credentials"
    state_root = tmp_path / "pool-state"
    store = CredentialStore(root)
    records = [store.create_cmc(f"secret-{index}") for index in range(2)]
    process_count = 8
    context = multiprocessing.get_context("spawn")
    queue = context.Queue()
    processes = [
        context.Process(
            target=_acquire_in_process,
            args=(str(root), str(state_root), queue),
        )
        for _ in range(process_count)
    ]

    for process in processes:
        process.start()
    results = [queue.get(timeout=20) for _ in processes]
    for process in processes:
        process.join(timeout=20)
        assert process.exitcode == 0

    assert all(success for success, _ in results), results
    selected = [value for _, value in results]
    assert {credential_id: selected.count(credential_id) for credential_id in set(selected)} == {
        record["id"]: process_count // 2 for record in records
    }
    status_payload = CmcPoolClient(
        credentials_root=root,
        state_root=state_root,
        soft_credit_limit=1000,
    ).status()
    assert sum(item["used_credits"] for item in status_payload["keys"]) == process_count


def test_pool_state_permissions_are_owner_only(tmp_path: Path) -> None:
    """Persistent usage, acquisition, and lock state is private on POSIX."""
    _store, pool, _records = _pool_with_keys(tmp_path, 1)
    pool.acquire("/v1/cryptocurrency/map")

    if os.name != "posix":
        pytest.skip("POSIX permission assertions")
    for path in [pool.state_root, *pool.state_root.rglob("*")]:
        if path.is_dir():
            assert stat.S_IMODE(path.stat().st_mode) == 0o700, path
        elif path.is_file():
            assert stat.S_IMODE(path.stat().st_mode) == 0o600, path


def test_desired_state_disables_and_exactly_reactivates_materialized_key(tmp_path: Path) -> None:
    """Remote state gates a local generation without fabricating missing materialization."""

    store = CredentialStore(tmp_path / "credentials")
    record = store.create_cmc("secret-value", origin="cluster", shared=True)
    entries = {record["id"]: {
                "key_id": record["id"],
                "secret_id": record["id"],
                "state": "disabled",
                "quota_domain_id": "shared-plan",
            }}
    desired = _strict_desired_state(
        tmp_path,
        {record["id"]: {"generation": 1}},
        entries,
    )
    pool = CmcPoolClient(
        store,
        state_root=tmp_path / "pool",
        desired_state_provider=lambda: desired,
    )

    with pytest.raises(CmcPoolExhaustedError):
        pool.acquire("/v1/cryptocurrency/map")
    assert store.get_cmc(record["id"])["active"] is False

    desired["desired_state"]["cmc_pool"]["entries"][record["id"]]["state"] = "active"
    assert pool.acquire("/v1/cryptocurrency/map").generation == 1
    desired["desired_state"]["secrets"][record["id"]]["generation"] = 2
    with pytest.raises(CmcPoolExhaustedError):
        pool.acquire("/v1/cryptocurrency/map")
    assert store.get_cmc(record["id"])["active"] is False


def test_partial_desired_materialization_keeps_only_exact_generation_eligible(tmp_path: Path) -> None:
    """One exact desired key cannot hide another stale desired pool entry."""

    store = CredentialStore(tmp_path / "credentials")
    exact = store.create_cmc("exact", origin="cluster", shared=True)
    stale = store.create_cmc("stale", origin="cluster", shared=True)
    entries = {
        record["id"]: {
            "key_id": record["id"],
            "secret_id": record["id"],
            "state": "active",
        }
        for record in (exact, stale)
    }
    desired = _strict_desired_state(tmp_path, {
            exact["id"]: {"generation": 1},
            stale["id"]: {"generation": 2},
        }, entries)
    pool = CmcPoolClient(
        store,
        state_root=tmp_path / "pool",
        desired_state_provider=lambda: desired,
    )

    status = pool.status()
    assert status["active_credentials"] == 0
    with pytest.raises(CmcPoolExhaustedError):
        pool.acquire("/v1/cryptocurrency/map")
    assert store.get_cmc(stale["id"])["active"] is False


def test_cluster_claim_fails_closed_when_desired_state_becomes_unavailable(tmp_path: Path) -> None:
    """A prior cluster claim prevents fallback to local/imported records on metadata loss."""

    store = CredentialStore(tmp_path / "credentials")
    record = store.create_cmc("claimed-secret", origin="local")
    desired = _strict_desired_state(
        tmp_path,
        {record["id"]: {"generation": 1}},
        {record["id"]: {"key_id": record["id"], "secret_id": record["id"], "state": "active"}},
    )
    snapshots = [desired, None]
    pool = CmcPoolClient(
        store,
        state_root=tmp_path / "pool",
        desired_state_provider=lambda: snapshots.pop(0) if snapshots else None,
    )

    assert pool.acquire("/v1/cryptocurrency/map").credential_id == record["id"]
    with pytest.raises(CmcPoolExhaustedError):
        pool.acquire("/v1/cryptocurrency/map")
    assert json.loads((tmp_path / "pool" / "cluster_claim.json").read_text())["cluster_metadata_claimed"] is True


def test_provider_disabled_code_is_terminal_for_only_the_failed_generation(tmp_path: Path) -> None:
    """Documented disabled-key codes fence a generation while 429 remains cooldown-only."""

    store, pool, records = _pool_with_keys(tmp_path, 1)
    failed = pool.acquire("/v1/cryptocurrency/map")
    result = pool.settle(failed, status_code=400, provider_status={"error_code": 1007})
    assert result["status"] == "provider_disabled"
    with pytest.raises(CmcPoolExhaustedError):
        pool.acquire("/v1/cryptocurrency/map")

    store.update_cmc(records[0]["id"], api_key="replacement")
    replacement = pool.acquire("/v1/cryptocurrency/map")
    assert replacement.generation == 2
    assert "secret" not in json.dumps(pool.status())
