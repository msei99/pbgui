"""Regression coverage for secret-free VPS credential capability metadata."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from api import v7_instances
import monitor_agent
import vps_manager_core
import vps_manager_service
from credential_store import CredentialStore
from master.cluster_state import ensure_local_identity
from vps_manager_core import VPS
from vps_manager_service import CREDENTIAL_CAPABILITY_FIELDS, VPSManagerService


ROOT = Path(__file__).resolve().parents[1]
LEGACY_FIELD = "coinmarketcap_api_key"


def test_vps_manager_production_paths_do_not_handle_raw_cmc_keys() -> None:
    """Frontend, service, API, and VPS automation must not transport raw CMC keys."""
    paths = [
        ROOT / "vps_manager_service.py",
        ROOT / "api" / "vps_manager.py",
        ROOT / "api" / "v7_instances.py",
        ROOT / "master" / "async_monitor.py",
        ROOT / "monitor_agent.py",
        ROOT / "frontend" / "vps_manager.html",
        ROOT / "frontend" / "v7_edit.html",
        ROOT / "setup" / "vps_service_control.sh",
        ROOT / "setup" / "installer" / "core.py",
        ROOT / "setup" / "installer" / "scripts" / "remote_master_bootstrap.sh",
        *ROOT.glob("vps-*.yml"),
    ]

    for path in paths:
        source = path.read_text(encoding="utf-8")
        assert LEGACY_FIELD not in source, path
        assert "check_cmc_api_key" not in source, path
        assert "config.get('coinmarketcap', 'api_key'" not in source, path
        assert 'config.get("coinmarketcap", "api_key"' not in source, path
        assert "section: coinmarketcap" not in source, path
        assert "option: api_key" not in source, path

    core_source = (ROOT / "vps_manager_core.py").read_text(encoding="utf-8")
    assert core_source.count(LEGACY_FIELD) == 1
    assert f'    "{LEGACY_FIELD}",' in core_source
    assert not (ROOT / "vps-update-coindata.yml").exists()


def test_vps_inventory_load_discards_and_next_save_scrubs_legacy_key(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Legacy inventory remains readable but its raw key never enters memory or survives save."""
    monkeypatch.setattr(vps_manager_core, "PBGDIR", tmp_path)
    host_dir = tmp_path / "data" / "vpsmanager" / "hosts" / "runner-a"
    host_dir.mkdir(parents=True)
    path = host_dir / "runner-a.json"
    path.write_text(
        json.dumps({"_hostname": "runner-a", "ip": "203.0.113.10", LEGACY_FIELD: "must-not-survive"}),
        encoding="utf-8",
    )

    vps = VPS()
    vps.load(path)

    assert not hasattr(vps, LEGACY_FIELD)
    assert "must-not-survive" not in repr(vps.__dict__)
    path.write_text(
        json.dumps({"_hostname": "runner-a", "_revision": 3, "ip": "203.0.113.11", LEGACY_FIELD: "concurrent-secret"}),
        encoding="utf-8",
    )
    vps.save()
    saved = json.loads(path.read_text(encoding="utf-8"))
    assert LEGACY_FIELD not in saved
    assert "concurrent-secret" not in path.read_text(encoding="utf-8")


def test_remote_capability_metadata_preserves_unknowns_and_reported_values() -> None:
    """Missing monitor evidence stays unknown while reported capability remains exact."""
    service = object.__new__(VPSManagerService)
    service._host_meta = lambda state: (state or {}).get("meta") or {}

    unknown = service._credential_capability_metadata("runner-a", {"meta": {}})
    assert unknown["credential_active"] is None
    assert unknown["credential_reason"] == "CMC credential capability has not been reported"

    reported = {
        "credential_protocol_version": 2,
        "credential_active": True,
        "cmc_catalog_generation": 7,
        "cmc_materialized_generation": 7,
        "cmc_active_key_count": 3,
        "cmc_provider_used": 42.5,
        "cmc_provider_limit": 100.0,
        "cmc_provider_usage_age_seconds": 4.5,
        "cmc_authority_reachable": True,
        "cmc_authority_state_age_seconds": 8.25,
    }
    metadata = service._credential_capability_metadata("runner-a", {"meta": reported})
    assert metadata["credential_reason"] == "CMC credential pool active"
    assert {key: metadata[key] for key in reported} == reported


def test_local_capability_requires_complete_materialization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Protocol support alone cannot fabricate active credential readiness."""
    monkeypatch.setattr(vps_manager_service, "PBGDIR", tmp_path)
    node_id = "pbgui-node-00000000-0000-4000-8000-000000000001"
    materialized = {
        "cluster_nodes": {
            "credential_membership_generation": 1,
            "nodes": {node_id: {
                "credential_protocol_version": 2,
                "credential_capable": True,
                "signing_key_id": "ed25519:test",
                "encryption_key_id": "x25519:test",
                "role": "master",
            }},
        },
        "desired_state": {
            "cmc_pool": {"entries": {"cmc_" + "1" * 32: {
                "key_id": "cmc_" + "1" * 32,
                "secret_id": "cmc_" + "1" * 32,
                "state": "active",
                "catalog_generation": 4,
            }}},
            "secrets": {"cmc_" + "1" * 32: {
                "generation": 4,
                "recipient_generation": 1,
                "recipient_ids": [node_id],
                "secret_kind": "cmc_api_key",
            }},
            "credential_materialization_acks": {node_id: {
                "membership_generation": 1,
                "credential_generations": {"cmc_" + "1" * 32: 4},
                "recipient_generations": {"cmc_" + "1" * 32: 1},
            }},
        },
    }
    monkeypatch.setattr(vps_manager_service, "rebuild_materialized_state", lambda *_args, **_kwargs: materialized)
    monkeypatch.setattr(vps_manager_service, "read_local_identity", lambda *_args, **_kwargs: {"node_id": node_id})
    catalog_path = tmp_path / "data" / "credentials" / "catalog.json"
    catalog_path.parent.mkdir(parents=True)
    catalog_path.write_text(json.dumps({"cmc": {}}), encoding="utf-8")

    metadata = object.__new__(VPSManagerService)._local_credential_capability_metadata()

    assert metadata["credential_protocol_version"] == 2
    assert metadata["cmc_catalog_generation"] == 4
    assert metadata["cmc_materialized_generation"] == 0
    assert metadata["cmc_active_key_count"] == 0
    assert metadata["credential_active"] is False
    assert metadata["credential_reason"] == "Desired CMC generation is not fully materialized"

    credential_id = "cmc_" + "1" * 32
    catalog_path.write_text(
        json.dumps({"cmc": {credential_id: {"id": credential_id, "active": True, "generation": 4}}}),
        encoding="utf-8",
    )

    ready = object.__new__(VPSManagerService)._local_credential_capability_metadata()

    assert ready["cmc_materialized_generation"] == 4
    assert ready["cmc_active_key_count"] == 1
    assert ready["credential_active"] is True
    assert ready["credential_reason"] == "CMC credential pool active"


@pytest.mark.parametrize(
    ("active_count", "expected_active", "expected_reason"),
    [
        (2, True, "CMC credential pool active"),
        (0, False, "No active materialized CMC credentials"),
    ],
)
def test_v7_local_master_capability_uses_local_pool(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    active_count: int,
    expected_active: bool,
    expected_reason: str,
) -> None:
    """The local master ignores monitor metadata and resolves its local pool directly."""

    (tmp_path / "pbgui.ini").write_text("[main]\npbname=master\n", encoding="utf-8")

    class FakeStore:
        """Secret-free local credential catalog double."""

        def __init__(self, root: Path) -> None:
            self.root = root

        def list_cmc(self, *, active_only: bool = False) -> list[dict]:
            assert active_only is True
            return [
                {"id": f"cmc_{index}", "generation": index + 3, "active": True}
                for index in range(active_count)
            ]

    class FakePool:
        """Aggregate local pool status double without any secret values."""

        def __init__(self, **_kwargs) -> None:
            pass

        @staticmethod
        def status() -> dict:
            return {"active_credentials": active_count, "keys": []}

    monkeypatch.setattr(v7_instances, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(v7_instances, "CredentialStore", FakeStore)
    monkeypatch.setattr(v7_instances, "CmcPoolClient", FakePool)
    monkeypatch.setattr(
        v7_instances,
        "_monitor",
        SimpleNamespace(store=SimpleNamespace(host_meta={"master": {"credential_active": not expected_active}})),
    )

    detail = v7_instances._host_dropdown_detail("master")

    assert detail["credential_active"] is expected_active
    assert detail["credential_reason"] == expected_reason
    assert detail["credential_protocol_version"] == 2
    assert detail["cmc_active_key_count"] == active_count
    assert detail["cmc_catalog_generation"] == (active_count + 2 if active_count else 0)
    assert detail["cmc_materialized_generation"] == (active_count + 2 if active_count else 0)
    assert detail["dynamic_ignore_allowed"] is expected_active


def test_monitor_agent_pbcoindata_expectation_never_reads_ini_or_host_meta(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PBCoinData expected state comes directly from the local pool snapshot."""

    monkeypatch.setattr(
        monitor_agent,
        "_read_ini",
        lambda: pytest.fail("PBCoinData capability must not read INI"),
    )
    monkeypatch.setattr(
        monitor_agent,
        "_read_json",
        lambda *_args, **_kwargs: pytest.fail("PBCoinData capability must not read host_meta"),
    )
    monkeypatch.setattr(
        monitor_agent,
        "_local_credential_capability",
        lambda: {"credential_active": True},
    )

    assert monitor_agent._service_expected("PBCoinData") is True


def test_monitor_host_meta_overrides_collector_capability_with_local_pool(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Agent host metadata publishes current local capability and no collector secret."""

    monkeypatch.setattr(monitor_agent, "DATA_DIR", tmp_path)
    monkeypatch.setattr(monitor_agent, "_embedded_monitor_script", lambda _name: "collector")
    monkeypatch.setattr(
        monitor_agent,
        "_run_shell_script",
        lambda *_args, **_kwargs: {
            "credential_active": False,
            "coinmarketcap_api_key": "must-not-survive",
        },
    )
    monkeypatch.setattr(
        monitor_agent,
        "_local_credential_capability",
        lambda: {
            "credential_protocol_version": 2,
            "credential_active": True,
            "credential_reason": "CMC credential pool active",
            "cmc_catalog_generation": 7,
            "cmc_materialized_generation": 7,
            "cmc_active_key_count": 2,
            "cmc_provider_used": 42.5,
            "cmc_provider_limit": 100.0,
            "cmc_provider_usage_age_seconds": 4.5,
            "cmc_authority_reachable": True,
        },
    )

    monitor_agent._run_host_meta()

    payload = json.loads((tmp_path / "host_meta.json").read_text(encoding="utf-8"))
    assert payload["credential_active"] is True
    assert payload["cmc_active_key_count"] == 2
    assert payload["cmc_provider_used"] == 42.5
    assert payload["cmc_provider_limit"] == 100.0
    assert payload["cmc_provider_usage_age_seconds"] == 4.5
    assert payload["cmc_authority_reachable"] is True
    assert "must-not-survive" not in json.dumps(payload)


def test_monitor_requires_every_desired_active_generation_exactly(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A partial or stale desired pool is not reported ready by the local monitor."""

    ensure_local_identity(
        tmp_path / "data" / "cluster",
        role="master",
        pbname="local",
        cluster_id="pbgui-cluster-00000000-0000-4000-8000-000000000001",
        node_id="pbgui-node-00000000-0000-4000-8000-000000000001",
    )
    store = CredentialStore(tmp_path / "data" / "credentials")
    exact = store.create_cmc("exact", origin="cluster", shared=True)
    stale = store.create_cmc("stale", origin="cluster", shared=True)
    orphan = store.create_cmc("orphan", origin="cluster", shared=True)
    desired = {
        "cluster_nodes": {
            "credential_membership_generation": 1,
            "nodes": {"pbgui-node-00000000-0000-4000-8000-000000000001": {
                "credential_protocol_version": 2,
                "credential_capable": True,
                "signing_key_id": "ed25519:test",
                "encryption_key_id": "x25519:test",
                "role": "master",
            }},
        },
        "desired_state": {
            "secrets": {
                exact["id"]: {"generation": 1, "recipient_generation": 1, "recipient_ids": ["pbgui-node-00000000-0000-4000-8000-000000000001"], "secret_kind": "cmc_api_key"},
                stale["id"]: {"generation": 2, "recipient_generation": 1, "recipient_ids": ["pbgui-node-00000000-0000-4000-8000-000000000001"], "secret_kind": "cmc_api_key"},
                orphan["id"]: {"generation": 1, "recipient_generation": 1, "recipient_ids": ["pbgui-node-00000000-0000-4000-8000-000000000001"], "secret_kind": "cmc_api_key"},
            },
            "credential_materialization_acks": {"pbgui-node-00000000-0000-4000-8000-000000000001": {
                "membership_generation": 1,
                "credential_generations": {exact["id"]: 1, stale["id"]: 2, orphan["id"]: 1},
                "recipient_generations": {exact["id"]: 1, stale["id"]: 1, orphan["id"]: 1},
            }},
            "cmc_pool": {"entries": {
                exact["id"]: {
                    "key_id": exact["id"],
                    "secret_id": exact["id"],
                    "state": "active",
                },
                stale["id"]: {
                    "key_id": stale["id"],
                    "secret_id": stale["id"],
                    "state": "active",
                },
            }},
        },
    }
    monkeypatch.setattr(monitor_agent, "PBGDIR", tmp_path)
    monkeypatch.setattr(
        monitor_agent,
        "_read_materialized_snapshot",
        lambda *_args, **_kwargs: desired,
    )

    partial = monitor_agent._local_credential_capability()
    assert partial["credential_active"] is False

    store.update_cmc(stale["id"], api_key="stale-generation-two")
    ready = monitor_agent._local_credential_capability()
    assert ready["credential_active"] is True
    assert ready["cmc_active_key_count"] == 2
    assert store.get_cmc(orphan["id"])["active"] is False
    assert "generation-two" not in json.dumps(ready)


def test_monitor_collectors_do_not_probe_legacy_cmc_ini_or_credits() -> None:
    """Agent and embedded/direct collectors contain no raw CMC INI probes."""

    agent_source = (ROOT / "monitor_agent.py").read_text(encoding="utf-8")
    collector_source = (ROOT / "master" / "async_monitor.py").read_text(encoding="utf-8")

    for source in (agent_source, collector_source):
        assert "config.get('coinmarketcap', 'api_key'" not in source
        assert 'config.get("coinmarketcap", "api_key"' not in source
        assert "cfg.get('coinmarketcap', 'credits_left'" not in source
        assert 'cfg.get("coinmarketcap", "credits_left"' not in source


def test_v7_frontend_explains_pool_state_and_generations_without_secret() -> None:
    """V7 editor wording exposes capability diagnostics but never a credential value."""

    source = (ROOT / "frontend" / "v7_edit.html").read_text(encoding="utf-8")

    assert "CMC pool inactive:" in source
    assert "CMC pool status unknown:" in source
    assert "catalog generation " in source
    assert "materialized generation " in source
    assert "active keys " in source
    assert "CMC pool status stale:" in source
    assert "_hostCapabilityGeneration" in source
    assert "_hostCapabilityController" in source
    assert "request_id=" in source
    assert "detail.credential_stale !== true" in source
    assert "Host capability status is unknown:" in source
    assert "hostsResp = { hosts: ['disabled']" in source
    assert LEGACY_FIELD not in source


def test_vps_frontend_renders_reason_and_guards_detail_requests() -> None:
    """VPS credential diagnostics include the reason and reject stale detail responses."""
    source = (ROOT / "frontend" / "vps_manager.html").read_text(encoding="utf-8")

    assert "Credential Reason" in source
    assert "status.credential_reason || capabilityStateText" in source
    assert "detailRequestGeneration" in source
    assert "detailAbortController" in source
    assert "generation !== store.detailRequestGeneration" in source
    assert "store.detailRequestGeneration += 1" in source
    assert "store.detailAbortController.abort()" in source
    assert "CMC Provider Usage Age" in source
    assert "CMC Provider Used / Limit" in source
    assert "status.cmc_provider_used" in source
    assert "status.cmc_provider_limit" in source
    assert "CMC Authority Reachable" in source
    assert "CMC Authority State Age" in source


def test_vps_provider_usage_metadata_uses_newest_secret_free_observation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """VPS capability reports the counters paired with its newest usage age."""

    monkeypatch.setattr(vps_manager_service.time, "time", lambda: 110.0)
    payload = vps_manager_service._cmc_provider_usage_metadata({
        "keys": [
            {"last_settled_at": 90, "provider_used": 80, "provider_limit": 100},
            {"provider_usage_updated_at": 105, "provider_used": 45.5, "provider_limit": 75},
        ],
    })

    assert payload == {
        "cmc_provider_used": 45.5,
        "cmc_provider_limit": 75.0,
        "cmc_provider_usage_age_seconds": 5.0,
    }
