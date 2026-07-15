"""Tests for the owner-only CMC and TradFi credential store core."""

from __future__ import annotations

import json
import multiprocessing
import os
from pathlib import Path
import stat

import pytest

from credential_store import CredentialNotFoundError, CredentialStore


def _create_cmc_in_process(root: str, index: int, queue) -> None:
    """Create one credential in a child process and return its ID."""
    try:
        record = CredentialStore(root).create_cmc(
            f"process-secret-{index}",
            label=f"process-{index}",
        )
        queue.put((True, record["id"]))
    except Exception as exc:  # pragma: no cover - surfaced by the parent assertion
        queue.put((False, repr(exc)))


def test_cmc_generations_are_immutable_and_metadata_has_no_secret(tmp_path: Path) -> None:
    """CMC replacement creates a new file and metadata never returns either key."""
    root = tmp_path / "credentials"
    store = CredentialStore(root)
    created = store.create_cmc("first-secret", label="primary")

    updated = store.update_cmc(created["id"], api_key="second-secret", shared=True)

    assert created["id"] == updated["id"]
    assert updated["generation"] == 2
    assert store.load_cmc_key(created["id"], generation=1) == "first-secret"
    assert store.load_cmc_key(created["id"]) == "second-secret"
    metadata_json = json.dumps(
        {
            "created": created,
            "updated": updated,
            "get": store.get_cmc(created["id"]),
            "list": store.list_cmc(),
        }
    )
    catalog_text = (root / "catalog.json").read_text(encoding="utf-8")
    assert "first-secret" not in metadata_json
    assert "second-secret" not in metadata_json
    assert "first-secret" not in catalog_text
    assert "second-secret" not in catalog_text
    assert (root / "cmc" / created["id"] / "generation-1.json").exists()
    assert (root / "cmc" / created["id"] / "generation-2.json").exists()


def test_tradfi_crud_keeps_secret_mapping_out_of_catalog(tmp_path: Path) -> None:
    """TradFi CRUD versions secrets and exposes only provider profile metadata."""
    root = tmp_path / "credentials"
    store = CredentialStore(root)
    created = store.create_tradfi(
        "polygon",
        {"api_key": "polygon-secret", "account": "private-account"},
        label="stocks",
    )

    assert store.load_tradfi_credentials(created["id"]) == {
        "api_key": "polygon-secret",
        "account": "private-account",
    }
    updated = store.update_tradfi(
        created["id"],
        provider="tiingo",
        credentials={"token": "tiingo-secret"},
        active=False,
    )
    assert updated["generation"] == 2
    assert updated["provider"] == "tiingo"
    assert store.load_tradfi_credentials(created["id"]) == {"token": "tiingo-secret"}
    public_text = json.dumps(store.list_tradfi()) + (root / "catalog.json").read_text()
    assert "polygon-secret" not in public_text
    assert "private-account" not in public_text
    assert "tiingo-secret" not in public_text

    store.delete_tradfi(created["id"])

    assert store.list_tradfi() == []
    with pytest.raises(CredentialNotFoundError):
        store.get_tradfi(created["id"])


def test_imported_and_shared_cmc_records_remain_active(tmp_path: Path) -> None:
    """Imported and shared metadata does not make a credential unusable."""
    store = CredentialStore(tmp_path / "credentials")
    imported = store.create_cmc(
        "imported-secret",
        origin="imported",
        shared=True,
    )

    active = store.active_cmc_credentials()

    assert active == [{**imported, "api_key": "imported-secret"}]


def test_store_repairs_owner_only_permissions(tmp_path: Path) -> None:
    """Every created credential directory and file is owner-only on POSIX."""
    root = tmp_path / "credentials"
    store = CredentialStore(root)
    cmc = store.create_cmc("cmc-secret")
    tradfi = store.create_tradfi("alpaca", {"key": "id", "secret": "value"})

    if os.name != "posix":
        pytest.skip("POSIX permission assertions")
    assert cmc["id"] and tradfi["id"]
    for path in [root, *root.rglob("*")]:
        if path.is_dir():
            assert stat.S_IMODE(path.stat().st_mode) == 0o700, path
        elif path.is_file():
            assert stat.S_IMODE(path.stat().st_mode) == 0o600, path


def test_store_rejects_traversal_and_symlinked_secret_paths(tmp_path: Path) -> None:
    """Untrusted IDs and symlink substitutions cannot escape the credential root."""
    root = tmp_path / "credentials"
    store = CredentialStore(root)

    with pytest.raises(ValueError):
        store.get_cmc("../../outside")

    outside = tmp_path / "outside"
    outside.mkdir()
    (root / "cmc").symlink_to(outside, target_is_directory=True)
    with pytest.raises(RuntimeError, match="symlink"):
        store.create_cmc("blocked-secret")
    assert list(outside.iterdir()) == []


def test_symlinked_configured_root_is_rejected(tmp_path: Path) -> None:
    """A configured credentials root itself cannot be a symlink."""
    target = tmp_path / "target"
    target.mkdir()
    linked_root = tmp_path / "linked"
    linked_root.symlink_to(target, target_is_directory=True)

    with pytest.raises(RuntimeError, match="symlink"):
        CredentialStore(linked_root)


def test_multiprocess_creates_do_not_lose_catalog_entries(tmp_path: Path) -> None:
    """The advisory lock serializes concurrent catalog read-modify-write cycles."""
    root = tmp_path / "credentials"
    process_count = 8
    context = multiprocessing.get_context("spawn")
    queue = context.Queue()
    processes = [
        context.Process(target=_create_cmc_in_process, args=(str(root), index, queue))
        for index in range(process_count)
    ]

    for process in processes:
        process.start()
    results = [queue.get(timeout=20) for _ in processes]
    for process in processes:
        process.join(timeout=20)
        assert process.exitcode == 0

    assert all(success for success, _ in results), results
    ids = [value for _, value in results]
    assert len(set(ids)) == process_count
    assert len(CredentialStore(root).list_cmc()) == process_count


@pytest.mark.parametrize("kind", ["cmc", "tradfi"])
def test_update_resumes_identical_generation_after_catalog_write_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    kind: str,
) -> None:
    """A crash between immutable secret creation and catalog commit is retryable."""

    store = CredentialStore(tmp_path / "credentials")
    if kind == "cmc":
        record = store.create_cmc("old-secret")
        update = lambda: store.update_cmc(record["id"], api_key="new-secret")
    else:
        record = store.create_tradfi("tiingo", {"api_key": "old-secret"})
        update = lambda: store.update_tradfi(record["id"], credentials={"api_key": "new-secret"})
    original = store._write_catalog_unlocked
    failed = False

    def fail_once(catalog: dict) -> None:
        nonlocal failed
        if not failed:
            failed = True
            raise OSError("catalog failure")
        original(catalog)

    monkeypatch.setattr(store, "_write_catalog_unlocked", fail_once)
    with pytest.raises(OSError, match="catalog failure"):
        update()

    resumed = update()
    assert resumed["generation"] == 2
    loaded = (
        store.load_cmc_key(record["id"])
        if kind == "cmc"
        else store.load_tradfi_credentials(record["id"])["api_key"]
    )
    assert loaded == "new-secret"


@pytest.mark.parametrize("kind", ["cmc", "tradfi"])
def test_update_rejects_different_payload_for_stranded_next_generation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    kind: str,
) -> None:
    """A stranded immutable generation cannot be overwritten by a different retry."""

    store = CredentialStore(tmp_path / "credentials")
    record = (
        store.create_cmc("old-secret")
        if kind == "cmc"
        else store.create_tradfi("tiingo", {"api_key": "old-secret"})
    )
    original = store._write_catalog_unlocked
    monkeypatch.setattr(store, "_write_catalog_unlocked", lambda _catalog: (_ for _ in ()).throw(OSError("crash")))
    with pytest.raises(OSError, match="crash"):
        if kind == "cmc":
            store.update_cmc(record["id"], api_key="first-retry")
        else:
            store.update_tradfi(record["id"], credentials={"api_key": "first-retry"})
    monkeypatch.setattr(store, "_write_catalog_unlocked", original)

    with pytest.raises(RuntimeError, match="different content"):
        if kind == "cmc":
            store.update_cmc(record["id"], api_key="different-retry")
        else:
            store.update_tradfi(record["id"], credentials={"api_key": "different-retry"})
    current = store.get_cmc(record["id"]) if kind == "cmc" else store.get_tradfi(record["id"])
    assert current["generation"] == 1


def test_pending_records_are_unselectable_and_tradfi_has_explicit_active_id(tmp_path: Path) -> None:
    """Pending records stay out of local readers and provider selection is explicit."""

    root = tmp_path / "credentials"
    store = CredentialStore(root)
    first = store.create_tradfi("tiingo", {"api_key": "first"})
    second = store.create_tradfi(
        "tiingo",
        {"api_key": "second"},
        pending=True,
        operation_id="replace-tiingo",
    )
    pending_cmc = store.create_cmc(
        "pending-cmc",
        pending=True,
        operation_id="create-cmc",
    )

    assert [item["id"] for item in store.list_tradfi(active_only=True)] == [first["id"]]
    assert store.active_cmc_credentials() == []
    assert store.list_cmc()[0]["active"] is False
    store.set_pending_stage("tradfi", second["id"], "replace-tiingo", "activated")
    store.finalize_pending_mutation("tradfi", second["id"], "replace-tiingo")

    catalog = json.loads((root / "catalog.json").read_text(encoding="utf-8"))
    assert catalog["active_tradfi_profiles"] == {"tiingo": second["id"]}
    assert [item["id"] for item in store.list_tradfi(active_only=True)] == [second["id"]]
    assert store.get_cmc(pending_cmc["id"])["pending"] is True
