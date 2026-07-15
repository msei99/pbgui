"""Order-independent rolling credential bootstrap regression coverage."""

from __future__ import annotations

import itertools
import json
import os
from pathlib import Path
import threading
import time

import pytest

import credential_rolling_bootstrap as rolling
from credential_rolling_bootstrap import bootstrap_local_legacy_credentials
from credential_store import CredentialStore
from secure_files import read_regular_file_nofollow


ROLLING_NODES = ("main-api", "local-pbcluster", "vps", "second-master")


def _write_ini(root: Path, *, cmc_key: str = "", pb7dir: Path | None = None, tradfi: str = "") -> Path:
    """Write one isolated local legacy source."""

    lines = ["[main]", f"pb7dir = {pb7dir or ''}"]
    if cmc_key:
        lines.extend(["", "[coinmarketcap]", f"api_key = {cmc_key}"])
    if tradfi:
        lines.extend(["", "[tradfi_profiles]", f"tiingo_api_key = {tradfi}"])
    path = root / "pbgui.ini"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


@pytest.mark.parametrize("order", list(itertools.permutations(ROLLING_NODES)))
def test_every_node_upgrade_order_keeps_local_credentials_available(
    tmp_path: Path,
    order: tuple[str, ...],
) -> None:
    """API, PBCluster, VPS, and another master may update first or last."""

    roots = {name: tmp_path / name for name in ROLLING_NODES}
    for name, root in roots.items():
        root.mkdir()
        _write_ini(root, cmc_key=f"key-{name}")

    for name in order:
        result = bootstrap_local_legacy_credentials(roots[name])
        assert result["status"] == "ready"
        store = CredentialStore(roots[name] / "data" / "credentials")
        assert [item["api_key"] for item in store.active_cmc_credentials()] == [f"key-{name}"]
        assert not (roots[name] / "data" / "cluster" / "node_identity.json").exists()

    for name in reversed(order):
        assert bootstrap_local_legacy_credentials(roots[name])["status"] == "ready"
        assert (roots[name] / "pbgui.ini").read_text(encoding="utf-8").endswith(f"api_key = key-{name}\n")


def test_long_pause_and_old_peer_state_do_not_freeze_or_delete_local_source(tmp_path: Path) -> None:
    """Repeated mixed-version cycles remain local and non-destructive before freeze."""

    ini = _write_ini(tmp_path, cmc_key="pause-key")
    original = ini.read_bytes()
    desired = tmp_path / "data" / "cluster" / "desired_state.json"
    desired.parent.mkdir(parents=True)
    desired.write_text(json.dumps({"credential_migration": {"frozen": False}}), encoding="utf-8")

    for _ in range(20):
        assert bootstrap_local_legacy_credentials(tmp_path)["status"] == "ready"

    assert ini.read_bytes() == original
    store = CredentialStore(tmp_path / "data" / "credentials")
    assert store.active_cmc_credentials()[0]["api_key"] == "pause-key"


def test_shadow_write_restart_reuses_planned_id(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """A restart after the planned ID is durable recovers the same vault generation."""

    _write_ini(tmp_path, cmc_key="crash-key")
    original_write = rolling._write_state
    crashed = False

    def crash_after_planned_id(path: Path, state: dict[str, object]) -> None:
        nonlocal crashed
        original_write(path, state)
        sources = state.get("sources") if isinstance(state, dict) else None
        has_planned = any(
            bool(source.get("credentials"))
            for source in (sources or {}).values()
            if isinstance(source, dict)
        )
        if has_planned and not crashed:
            crashed = True
            raise RuntimeError("simulated restart")

    monkeypatch.setattr(rolling, "_write_state", crash_after_planned_id)
    with pytest.raises(RuntimeError, match="simulated restart"):
        bootstrap_local_legacy_credentials(tmp_path)
    state_path = tmp_path / "data" / "credentials" / "legacy_shadow" / "state.json"
    planned = next(iter(next(iter(json.loads(state_path.read_text())["sources"].values()))["credentials"].values()))

    monkeypatch.setattr(rolling, "_write_state", original_write)
    bootstrap_local_legacy_credentials(tmp_path)
    store = CredentialStore(tmp_path / "data" / "credentials")
    assert [record["id"] for record in store.list_cmc()] == [planned]
    assert store.load_cmc_key(planned) == "crash-key"


def test_restart_before_planned_id_still_imports_source(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """A restart after source metadata alone cannot falsely mark bootstrap complete."""

    _write_ini(tmp_path, cmc_key="early-crash-key")
    original_write = rolling._write_state
    crashed = False

    def crash_after_source(path: Path, state: dict[str, object]) -> None:
        nonlocal crashed
        original_write(path, state)
        if state.get("sources") and not crashed:
            crashed = True
            raise RuntimeError("simulated early restart")

    monkeypatch.setattr(rolling, "_write_state", crash_after_source)
    with pytest.raises(RuntimeError, match="simulated early restart"):
        bootstrap_local_legacy_credentials(tmp_path)

    monkeypatch.setattr(rolling, "_write_state", original_write)
    bootstrap_local_legacy_credentials(tmp_path)
    store = CredentialStore(tmp_path / "data" / "credentials")
    assert [item["api_key"] for item in store.active_cmc_credentials()] == ["early-crash-key"]


def test_duplicate_equal_tradfi_sources_use_one_active_shadow(tmp_path: Path) -> None:
    """Equal INI and PB7 provider credentials dedupe by constant-time vault comparison."""

    pb7 = tmp_path / "pb7"
    pb7.mkdir()
    (pb7 / "api-keys.json").write_text(
        json.dumps({"tradfi": {"tiingo": {"api_key": "same-tiingo-key"}}}),
        encoding="utf-8",
    )
    _write_ini(tmp_path, pb7dir=pb7, tradfi="same-tiingo-key")

    bootstrap_local_legacy_credentials(tmp_path)

    store = CredentialStore(tmp_path / "data" / "credentials")
    records = store.list_tradfi(active_only=True)
    assert len(records) == 1
    assert records[0]["origin"] == "legacy_shadow"
    assert store.load_tradfi_credentials(records[0]["id"]) == {"api_key": "same-tiingo-key"}


def test_cluster_materialization_promotes_shadow_without_new_generation(tmp_path: Path) -> None:
    """Frozen inventory publication reuses a matching shadow ID and generation."""

    _write_ini(tmp_path, cmc_key="promote-key")
    bootstrap_local_legacy_credentials(tmp_path)
    store = CredentialStore(tmp_path / "data" / "credentials")
    shadow = store.list_cmc()[0]

    promoted = store.materialize_cluster_secret(
        shadow["id"],
        "cmc_api_key",
        1,
        "promote-key",
        metadata={"active": True},
    )

    assert promoted["id"] == shadow["id"]
    assert promoted["generation"] == 1
    assert promoted["origin"] == "cluster"


def test_freeze_never_reads_changed_source_and_cutoff_retires_unmatched_shadow(tmp_path: Path) -> None:
    """No shadow fallback remains active after the replicated v2 cutoff."""

    ini = _write_ini(tmp_path, cmc_key="before-freeze")
    bootstrap_local_legacy_credentials(tmp_path)
    store = CredentialStore(tmp_path / "data" / "credentials")
    shadow_id = store.list_cmc()[0]["id"]
    ini.write_text("[coinmarketcap]\napi_key = changed-after-freeze\n", encoding="utf-8")
    desired = tmp_path / "data" / "cluster" / "desired_state.json"
    desired.parent.mkdir(parents=True, exist_ok=True)
    desired.write_text(
        json.dumps({"credential_migration": {"frozen": True}, "secrets": {}}),
        encoding="utf-8",
    )

    assert bootstrap_local_legacy_credentials(tmp_path)["status"] == "frozen"
    assert store.load_cmc_key(shadow_id) == "before-freeze"
    assert ini.read_text(encoding="utf-8").endswith("changed-after-freeze\n")

    desired.write_text(
        json.dumps({
            "credential_migration": {"frozen": True, "cutoff": {"cutoff_generation": 1}},
            "secrets": {},
        }),
        encoding="utf-8",
    )
    assert bootstrap_local_legacy_credentials(tmp_path)["status"] == "cutoff"
    assert store.list_cmc(active_only=True) == []
    assert store.get_cmc(shadow_id)["origin"] == "legacy_shadow"
    assert store.get_cmc(shadow_id)["active"] is False


@pytest.mark.parametrize("cutoff_call", [1, 4, 5])
def test_cutoff_before_during_or_after_shadow_write_retires_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    cutoff_call: int,
) -> None:
    """A cutoff injected around lookup/write/state commit never leaves a consumable shadow."""
    _write_ini(tmp_path, cmc_key="race-key")
    calls = 0

    def flags(_root: Path) -> tuple[bool, bool]:
        nonlocal calls
        calls += 1
        return False, calls >= cutoff_call

    monkeypatch.setattr(rolling, "_migration_flags", flags)
    result = bootstrap_local_legacy_credentials(tmp_path)
    assert result["status"] == "cutoff"
    assert CredentialStore(tmp_path / "data" / "credentials").active_cmc_credentials() == []


@pytest.mark.parametrize("freeze_call", [4, 5])
def test_freeze_during_or_after_shadow_write_retires_only_new_racing_shadow(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    freeze_call: int,
) -> None:
    """A freeze racing creation cannot expose the just-created fallback."""
    _write_ini(tmp_path, cmc_key="freeze-race-key")
    calls = 0

    def flags(_root: Path) -> tuple[bool, bool]:
        nonlocal calls
        calls += 1
        return calls >= freeze_call, False

    monkeypatch.setattr(rolling, "_migration_flags", flags)
    assert bootstrap_local_legacy_credentials(tmp_path)["status"] == "frozen"
    assert CredentialStore(tmp_path / "data" / "credentials").active_cmc_credentials() == []


@pytest.mark.parametrize("state_payload", ["{broken", json.dumps({"version": 999, "sources": {}})])
def test_cutoff_retires_catalog_shadows_even_when_private_state_is_corrupt_or_newer(
    tmp_path: Path,
    state_payload: str,
) -> None:
    """Catalog-origin retirement precedes parsing and quarantines unsafe private state."""
    _write_ini(tmp_path, cmc_key="catalog-key")
    bootstrap_local_legacy_credentials(tmp_path)
    state_path = tmp_path / "data" / "credentials" / "legacy_shadow" / "state.json"
    state_path.write_text(state_payload, encoding="utf-8")
    desired = tmp_path / "data" / "cluster" / "desired_state.json"
    desired.parent.mkdir(parents=True, exist_ok=True)
    desired.write_text(json.dumps({"credential_migration": {"cutoff": {"min_protocol": 2}}}), encoding="utf-8")

    assert bootstrap_local_legacy_credentials(tmp_path)["status"] == "cutoff"
    assert CredentialStore(tmp_path / "data" / "credentials").active_cmc_credentials() == []
    assert list(state_path.parent.glob("state.quarantine-*.json"))


def test_equal_inactive_shadow_is_not_reactivated(tmp_path: Path) -> None:
    """Equality dedupe preserves an explicit inactive shadow state."""
    _write_ini(tmp_path, cmc_key="inactive-key")
    bootstrap_local_legacy_credentials(tmp_path)
    store = CredentialStore(tmp_path / "data" / "credentials")
    credential_id = store.list_cmc()[0]["id"]
    store.update_cmc(credential_id, active=False)

    bootstrap_local_legacy_credentials(tmp_path)

    assert store.get_cmc(credential_id)["active"] is False
    assert len(store.list_cmc()) == 1


def test_tradfi_bootstrap_preserves_newer_selected_profile(tmp_path: Path) -> None:
    """A legacy TradFi shadow never displaces an already selected vault profile."""
    _write_ini(tmp_path, tradfi="legacy-tiingo")
    store = CredentialStore(tmp_path / "data" / "credentials")
    selected = store.create_tradfi("tiingo", {"api_key": "new-tiingo"}, active=True)

    bootstrap_local_legacy_credentials(tmp_path)

    records = store.list_tradfi(active_only=False)
    assert next(item for item in records if item["id"] == selected["id"])["active"] is True
    shadow = next(item for item in records if item["origin"] == "legacy_shadow")
    assert shadow["active"] is False


def test_bootstrap_serializes_equality_and_commit_with_api_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An API-style mutation cannot enter between shadow equality lookup and commit."""
    _write_ini(tmp_path, cmc_key="bootstrap-key")
    entered_lookup = threading.Event()
    release_lookup = threading.Event()
    api_entered = threading.Event()
    original_find = rolling._find_equal_credential

    def blocked_find(store: CredentialStore, item: dict[str, object]) -> str:
        entered_lookup.set()
        assert release_lookup.wait(timeout=5)
        return original_find(store, item)

    monkeypatch.setattr(rolling, "_find_equal_credential", blocked_find)
    bootstrap_thread = threading.Thread(target=bootstrap_local_legacy_credentials, args=(tmp_path,))
    bootstrap_thread.start()
    assert entered_lookup.wait(timeout=5)

    def api_mutation() -> None:
        from credential_store import credential_mutation_lock

        with credential_mutation_lock(tmp_path / "data" / "credentials"):
            api_entered.set()
            CredentialStore(tmp_path / "data" / "credentials").create_cmc("api-key")

    api_thread = threading.Thread(target=api_mutation)
    api_thread.start()
    time.sleep(0.05)
    assert not api_entered.is_set()
    release_lookup.set()
    bootstrap_thread.join(timeout=5)
    api_thread.join(timeout=5)
    assert api_entered.is_set()
    assert len(CredentialStore(tmp_path / "data" / "credentials").list_cmc()) == 2


def test_descriptor_read_rejects_symlink_replacement_after_validation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Replacing a validated source with a symlink cannot redirect its descriptor read."""
    source = tmp_path / "pbgui.ini"
    outside = tmp_path.parent / f"outside-{tmp_path.name}.ini"
    source.write_text("safe", encoding="utf-8")
    outside.write_text("secret", encoding="utf-8")
    original_open = os.open
    swapped = False

    def swapping_open(path: object, flags: int, *args: object, **kwargs: object) -> int:
        nonlocal swapped
        if path == "pbgui.ini" and kwargs.get("dir_fd") is not None and not swapped:
            swapped = True
            source.unlink()
            source.symlink_to(outside)
        return original_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(os, "open", swapping_open)
    with pytest.raises(OSError):
        read_regular_file_nofollow(source, tmp_path)
    assert swapped
