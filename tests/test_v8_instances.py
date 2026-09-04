"""Focused offline tests for the PB8 live-instance CRUD surface."""

from __future__ import annotations

import asyncio
import copy
import json
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from starlette.datastructures import URL

from api import v8_instances
from secure_files import atomic_write_private_text


def _install_test_pipeline(monkeypatch: pytest.MonkeyPatch) -> list[dict]:
    """Replace only the external PB8 helper while retaining atomic persistence."""

    prepared_calls: list[dict] = []

    def prepare(config: dict, **_kwargs) -> dict:
        prepared_calls.append(copy.deepcopy(config))
        result = copy.deepcopy(config)
        result["prepared_by_pb8"] = True
        return result

    def save(config: dict, path: Path) -> dict:
        atomic_write_private_text(Path(path), json.dumps(config, indent=4) + "\n")
        return copy.deepcopy(config)

    def load(path: Path) -> dict:
        return json.loads(Path(path).read_text(encoding="utf-8"))

    monkeypatch.setattr(v8_instances, "prepare_pb8_config", prepare)
    monkeypatch.setattr(v8_instances, "save_prepared_pb8_config", save)
    monkeypatch.setattr(v8_instances, "load_pb8_config", load)
    return prepared_calls


def _configure_root(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Point PB8 live and cluster state at an isolated PBGui root."""

    monkeypatch.setattr(v8_instances, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(v8_instances, "_master_hostname", lambda: "master-a")
    monkeypatch.setattr(v8_instances, "_monitor", None)
    monkeypatch.setattr(v8_instances, "_available_users", lambda: [{"name": "alice", "exchange": "binance"}])
    monkeypatch.setattr(v8_instances, "_user_exchange_cache", (0.0, {}))


def _payload(*, enabled_on: str = "disabled", note: str = "first") -> dict:
    """Return one minimal PB8 live editor payload."""

    return {
        "config": {
            "live": {"user": "alice"},
            "bot": {"long": {"risk": {"n_positions": 3}}},
            "pbgui": {"enabled_on": enabled_on, "note": note, "version": 999},
        }
    }


def test_available_users_are_filtered_by_pb8_live_exchange_capabilities(monkeypatch) -> None:
    """PB8 Run must include Bitunix/WEEX users without exposing PB7-only entries."""
    users = SimpleNamespace(
        list=lambda: ["bitunix-user", "weex-user", "unsupported-user"],
        find_exchange=lambda name: {
            "bitunix-user": "bitunix",
            "weex-user": "weex",
            "unsupported-user": "unsupported",
        }[name],
    )
    import User

    monkeypatch.setattr(User, "Users", lambda: users)
    monkeypatch.setattr(
        v8_instances,
        "get_pb8_exchange_metadata",
        lambda: {"live": ["bitunix", "weex"]},
    )

    assert v8_instances._available_users() == [
        {"name": "bitunix-user", "exchange": "bitunix"},
        {"name": "weex-user", "exchange": "weex"},
    ]


def test_pb8_symbol_and_status_routes_use_official_market_identifier_bridge(monkeypatch) -> None:
    """PB8 metadata routes must preserve collision-safe IDs and exact imported values."""
    calls = []

    def resolve(exchanges, identifiers=None, **_kwargs):
        calls.append((exchanges, identifiers))
        return {
            "contract_version": 1,
            "exchanges": list(exchanges),
            "symbols": ["BTC", "bitget::ABCUSDT", "bitget::1000ABCUSDT"],
            "catalog": [
                {"config_id": "BTC", "coin": "BTC", "resolutions": []},
                {"config_id": "bitget::ABCUSDT", "coin": "ABC", "resolutions": []},
                {"config_id": "bitget::1000ABCUSDT", "coin": "1000ABC", "resolutions": []},
            ],
            "statuses": {
                value: {"input": value, "normalized": value, "status": "valid", "reason": "resolved"}
                for value in identifiers or []
            },
        }

    monkeypatch.setattr(v8_instances, "get_pb8_market_identifiers", resolve)

    symbols = v8_instances.get_v8_symbols("bitget", session=None)
    statuses = v8_instances.get_v8_coin_statuses(
        {"exchanges": ["bitget"], "coins": ["1000ABC/USDT:USDT", "all"]}, session=None
    )

    assert symbols["symbols"] == ["BTC", "bitget::ABCUSDT", "bitget::1000ABCUSDT"]
    assert statuses["statuses"]["1000ABC/USDT:USDT"]["normalized"] == "1000ABC/USDT:USDT"
    assert statuses["statuses"]["all"]["normalized"] == "all"
    assert calls == [(["bitget"], None), (["bitget"], ["1000ABC/USDT:USDT"])]


def test_pb8_empty_status_request_still_returns_catalog(monkeypatch) -> None:
    """New and all-only editors need PB8 catalog options without submitted identifiers."""
    monkeypatch.setattr(
        v8_instances,
        "get_pb8_market_identifiers",
        lambda exchanges, identifiers=None: {
            "contract_version": 1,
            "exchanges": exchanges,
            "symbols": ["BTC"],
            "catalog": [{"config_id": "BTC", "coin": "BTC", "resolutions": []}],
            "statuses": {},
        },
    )

    result = v8_instances.get_v8_coin_statuses(
        {"exchanges": ["bitget"], "coins": []}, session=None
    )

    assert result["symbols"] == ["BTC"]


def test_pb8_coin_filter_projects_coindata_policy_onto_resolver_catalog(monkeypatch) -> None:
    """CoinData filtering must return PB8 config IDs rather than ambiguous short names."""
    from api import editor_market_data

    monkeypatch.setattr(
        editor_market_data,
        "filter_symbols",
        lambda *_args, **_kwargs: (["BTC", "1000ABC", "MISSING", "USD1"], ["ABC"]),
    )
    monkeypatch.setattr(
        v8_instances,
        "get_pb8_market_identifiers",
        lambda *_args, **_kwargs: {
            "catalog": [
                {"config_id": "BTC", "coin": "BTC", "resolutions": [{"exchange": "bitget", "symbol": "BTC/USDT:USDT"}]},
                {"config_id": "bitget::ABCUSDT", "coin": "ABC", "resolutions": [{"exchange": "bitget", "symbol": "ABC/USDT:USDT"}]},
                {"config_id": "bitget::1000ABCUSDT", "coin": "1000ABC", "resolutions": [{"exchange": "bitget", "symbol": "1000ABC/USDT:USDT"}]},
                {"config_id": "USD1", "coin": "USD1", "resolutions": []},
            ]
        },
    )

    result = v8_instances.filter_v8_coins("bitget", 0, 10.0, False, False, "", session=None)

    assert result == {
        "approved": ["BTC", "bitget::1000ABCUSDT"],
        "ignored": ["bitget::ABCUSDT"],
        "unresolved": ["MISSING", "USD1"],
    }


def test_pb8_coin_filter_projects_namespaced_hyperliquid_aliases(monkeypatch) -> None:
    """CoinData XYZ-TSLA policy must project onto PB8's xyz:TSLA config identifier."""
    from api import editor_market_data

    monkeypatch.setattr(editor_market_data, "filter_symbols", lambda *_args, **_kwargs: (["XYZ-TSLA"], []))
    monkeypatch.setattr(
        v8_instances,
        "get_pb8_market_identifiers",
        lambda *_args, **_kwargs: {
            "catalog": [
                {"config_id": "xyz:TSLA", "coin": "xyz:TSLA", "resolutions": [{"exchange": "hyperliquid", "symbol": "xyz:TSLA/USDC:USDC"}]}
            ]
        },
    )

    result = v8_instances.filter_v8_coins("hyperliquid", 0, 10.0, False, False, "", session=None)

    assert result == {"approved": ["xyz:TSLA"], "ignored": [], "unresolved": []}


def test_pb8_market_route_maps_resolver_unavailability_to_503(monkeypatch) -> None:
    """Incomplete PB8 catalogs must fail as retryable service errors without CoinData fallback."""
    monkeypatch.setattr(
        v8_instances,
        "get_pb8_market_identifiers",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            v8_instances.PB8MarketDataUnavailableError("resolver unavailable")
        ),
    )
    monkeypatch.setattr(v8_instances, "_log", lambda *_args, **_kwargs: None)

    with pytest.raises(v8_instances.HTTPException) as exc_info:
        v8_instances.get_v8_symbols("bitget", session=None)

    assert exc_info.value.status_code == 503
    assert "resolver unavailable" in str(exc_info.value.detail)


@pytest.mark.parametrize(
    "body",
    [
        {"exchanges": [], "coins": ["BTC"]},
        {"exchanges": "bitget", "coins": ["BTC"]},
        {"exchanges": [" bitget "], "coins": ["BTC"]},
        {"exchanges": ["bitget"], "coins": "BTC"},
        {"exchanges": ["bitget"], "coins": [" BTC "]},
    ],
)
def test_pb8_market_status_rejects_malformed_client_requests(body) -> None:
    """Malformed resolver requests must return HTTP 422 instead of empty success or 503."""
    with pytest.raises(v8_instances.HTTPException) as exc_info:
        v8_instances.get_v8_coin_statuses(body, session=None)

    assert exc_info.value.status_code == 422


def test_pb8_override_references_preserve_exact_market_identifiers() -> None:
    """Scoped, namespaced, and exact-symbol override keys must not be uppercased or collapsed."""
    config = {
        "coin_overrides": {
            "bitget::1000ABCUSDT": {"override_config_path": "scaled.json"},
            "xyz:TSLA": {"override_config_path": "hip3.json"},
            "1000ABC/USDT:USDT": {"override_config_path": "symbol.json"},
        }
    }

    assert v8_instances._referenced_overrides(config) == {
        "bitget::1000ABCUSDT": "scaled.json",
        "xyz:TSLA": "hip3.json",
        "1000ABC/USDT:USDT": "symbol.json",
    }

    with pytest.raises(v8_instances.HTTPException, match="duplicate coin override identifier"):
        v8_instances._referenced_overrides(
            {"coin_overrides": {"ABC": {}, " ABC ": {"override_config_path": "other.json"}}}
        )


def test_save_publishes_canonical_pb8_manifest_and_explicit_upsert(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A save owns metadata, writes privately, and records PB8-only desired state."""

    _configure_root(monkeypatch, tmp_path)
    prepared_calls = _install_test_pipeline(monkeypatch)

    first = asyncio.run(v8_instances.save_v8_instance_config("alice", _payload(), True, session=None))
    second = asyncio.run(v8_instances.save_v8_instance_config("alice", _payload(note="second"), False, session=None))

    config_path = tmp_path / "data" / "run_v8" / "alice" / "config.json"
    saved = json.loads(config_path.read_text(encoding="utf-8"))
    desired = json.loads((tmp_path / "data" / "cluster" / "desired_state.json").read_text(encoding="utf-8"))
    operations = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in sorted((tmp_path / "data" / "cluster" / "oplog").glob("*/*.json"))
    ]
    upserts = [item for item in operations if item["op"] == "UPSERT_PB8_CONFIG"]

    assert len(prepared_calls) == 2
    assert first["operation"] == "UPSERT_PB8_CONFIG"
    assert second["version"] == 2
    assert saved["pbgui"] == {
        "enabled_on": "disabled",
        "note": "second",
        "version": 2,
        "runtime": "pb8",
    }
    assert saved["prepared_by_pb8"] is True
    assert config_path.stat().st_mode & 0o777 == 0o600
    assert [item["version"] for item in upserts] == ["1", "2"]
    assert upserts[-1]["desired_state"] == "stopped"
    assert upserts[-1]["config_manifest_hash"].startswith("sha256:")
    assert desired["pb8_instances"]["alice"]["version"] == "2"
    assert desired["instances"] == {}


@pytest.mark.parametrize(
    ("requested", "expected"),
    (("panic", "panic"), ("graceful_stop", "graceful_stop"), ("tp_only", "tp_only"), ("normal", "")),
)
def test_pb8_forced_mode_uses_versioned_bundle_save_and_preserves_overrides(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    requested: str,
    expected: str,
) -> None:
    """PB8 P/G/T actions back up, version, validate, publish, and retain sparse files."""
    _configure_root(monkeypatch, tmp_path)
    _install_test_pipeline(monkeypatch)
    payload = _payload()
    if requested == "normal":
        payload["config"]["live"].update({"forced_mode_long": "panic", "forced_mode_short": "tp_only"})
    payload["config"]["coin_overrides"] = {"BTC": {"override_config_path": "BTC.json"}}
    payload["override_configs"] = {"BTC.json": {"bot": {"long": {"risk": {"n_positions": 1}}}}}
    asyncio.run(v8_instances.save_v8_instance_config("alice", payload, True, session=None))

    body = {"mode": requested}
    if requested == "normal":
        body["expected_version"] = 1
    result = asyncio.run(v8_instances.set_v8_instance_forced_mode("alice", body, session=None))

    bundle = tmp_path / "data" / "run_v8" / "alice"
    saved = json.loads((bundle / "config.json").read_text(encoding="utf-8"))
    desired = json.loads((tmp_path / "data" / "cluster" / "desired_state.json").read_text(encoding="utf-8"))
    assert saved["live"]["forced_mode_long"] == expected
    assert saved["live"]["forced_mode_short"] == expected
    assert saved["pbgui"]["version"] == 2
    assert (bundle / "BTC.json").is_file()
    assert (tmp_path / "data" / "backup" / "v8" / "alice" / "1" / "config.json").is_file()
    assert desired["pb8_instances"]["alice"]["version"] == "2"
    assert result["forced_mode"] == expected
    assert result["mode"] == requested
    assert result["version"] == 2
    assert result["backup_id"] == "1"


@pytest.mark.parametrize("mode", ("manual", "off", "clear", "", None, False))
def test_pb8_forced_mode_rejects_unknown_mode(mode: object) -> None:
    """PB8 forced-mode actions reject missing and ambiguous clear aliases."""
    with pytest.raises(v8_instances.HTTPException, match="mode must be") as error:
        asyncio.run(v8_instances.set_v8_instance_forced_mode("alice", {"mode": mode}, session=None))

    assert error.value.status_code == 400


@pytest.mark.parametrize("expected_version", (None, "1", False, -1))
def test_pb8_normal_mode_requires_explicit_version(expected_version: object) -> None:
    """Normal mode requires an exact nonnegative integer config version."""

    with pytest.raises(v8_instances.HTTPException, match="requires expected_version") as error:
        asyncio.run(v8_instances.set_v8_instance_forced_mode(
            "alice", {"mode": "normal", "expected_version": expected_version}, session=None,
        ))

    assert error.value.status_code == 400


def test_pb8_normal_mode_rejects_stale_version(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """A stale browser must not clear a newer global emergency mode."""

    _configure_root(monkeypatch, tmp_path)
    _install_test_pipeline(monkeypatch)
    payload = _payload()
    payload["config"]["live"].update({"forced_mode_long": "panic", "forced_mode_short": "panic"})
    asyncio.run(v8_instances.save_v8_instance_config("alice", payload, True, session=None))

    with pytest.raises(v8_instances.HTTPException, match="changed; refresh") as error:
        asyncio.run(v8_instances.set_v8_instance_forced_mode(
            "alice", {"mode": "normal", "expected_version": 0}, session=None,
        ))

    saved = json.loads((tmp_path / "data" / "run_v8" / "alice" / "config.json").read_text(encoding="utf-8"))
    assert error.value.status_code == 409
    assert saved["pbgui"]["version"] == 1
    assert saved["live"]["forced_mode_long"] == "panic"


def test_fast_pb8_activation_has_three_second_transport_budget(monkeypatch: pytest.MonkeyPatch) -> None:
    """A PB8 save reserves time for PBRun to react within the five-second target."""

    captured: dict[str, object] = {}

    async def to_thread(function, *args, **kwargs):
        captured.update({"function": function, "args": args, "kwargs": kwargs})
        return {
            "ok": True,
            "direct": True,
            "node_id": "node-b",
            "pbname": "vps-b",
            "materialization": {"ok": True},
        }

    monkeypatch.setattr(v8_instances.asyncio, "to_thread", to_thread)

    result = asyncio.run(v8_instances._activate_pb8_target("alice", {"op": "UPSERT_PB8_CONFIG"}))

    assert captured["function"] is v8_instances.push_pb8_activation
    assert captured["kwargs"] == {"timeout": 3}
    assert result["direct"] is True
    assert result["pending"] is False


def test_instance_name_must_match_exchange_user(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """PB8 rejects custom deployment names so one exchange user maps to exactly one live instance."""

    _configure_root(monkeypatch, tmp_path)

    with pytest.raises(v8_instances.HTTPException, match="must match live.user") as mismatched_save:
        asyncio.run(v8_instances.save_v8_instance_config("custom-name", _payload(), True, session=None))
    with pytest.raises(v8_instances.HTTPException, match="must match target_user") as mismatched_copy:
        asyncio.run(v8_instances.copy_v8_instance_config(
            "alice",
            {"target_user": "bob", "target_name": "custom-name", "config": _payload()["config"]},
            session=None,
        ))

    assert mismatched_save.value.status_code == 422
    assert mismatched_copy.value.status_code == 422


def test_failed_operation_publication_restores_previous_config(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A cluster publication failure cannot leave an unpublished local edit behind."""

    _configure_root(monkeypatch, tmp_path)
    _install_test_pipeline(monkeypatch)
    asyncio.run(v8_instances.save_v8_instance_config("alice", _payload(note="stable"), True, session=None))
    config_path = tmp_path / "data" / "run_v8" / "alice" / "config.json"
    previous = config_path.read_bytes()
    monkeypatch.setattr(v8_instances, "_record_upsert", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("oplog unavailable")))

    with pytest.raises(v8_instances.HTTPException) as error:
        asyncio.run(v8_instances.save_v8_instance_config("alice", _payload(note="unpublished"), False, session=None))

    assert error.value.status_code == 500
    assert config_path.read_bytes() == previous


def test_cluster_rollout_blocker_prevents_backup_before_save(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A rollout HTTP 409 must leave backup history untouched."""
    _configure_root(monkeypatch, tmp_path)
    _install_test_pipeline(monkeypatch)
    asyncio.run(v8_instances.save_v8_instance_config("alice", _payload(note="stable"), True, session=None))
    backups: list[str] = []
    monkeypatch.setattr(
        v8_instances,
        "_snapshot_v8_bundle",
        lambda *_args, **_kwargs: backups.append("created") or "1",
    )
    monkeypatch.setattr(
        v8_instances,
        "_ensure_pb8_cluster_rollout_ready",
        lambda _identity: (_ for _ in ()).throw(
            v8_instances.HTTPException(status_code=409, detail="PB8 Cluster rollout is not ready")
        ),
    )

    with pytest.raises(v8_instances.HTTPException) as blocked:
        asyncio.run(v8_instances.save_v8_instance_config("alice", _payload(note="blocked"), False, session=None))

    assert blocked.value.status_code == 409
    assert backups == []


def test_save_publishes_exact_override_bundle_and_removes_stale_files(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Config, sparse overrides, manifest, and stale-file cleanup share one transaction."""

    _configure_root(monkeypatch, tmp_path)
    _install_test_pipeline(monkeypatch)
    payload = _payload()
    payload["config"]["coin_overrides"] = {
        "BTC": {"override_config_path": "BTC.json"},
    }
    payload["override_configs"] = {
        "BTC.json": {"bot": {"long": {"risk": {"n_positions": 1}}}},
    }

    first = asyncio.run(v8_instances.save_v8_instance_config("alice", payload, True, session=None))
    bundle = tmp_path / "data" / "run_v8" / "alice"
    assert first["overrides"] == ["BTC.json"]
    assert json.loads((bundle / "BTC.json").read_text(encoding="utf-8"))["bot"]["long"]["risk"]["n_positions"] == 1
    assert (bundle / "BTC.json").stat().st_mode & 0o777 == 0o600

    second_payload = _payload(note="without override")
    second_payload["expected_version"] = 1
    asyncio.run(v8_instances.save_v8_instance_config("alice", second_payload, False, session=None))

    assert not (bundle / "BTC.json").exists()
    assert sorted(path.name for path in bundle.iterdir()) == ["config.json"]
    backup = tmp_path / "data" / "backup" / "v8" / "alice" / "1"
    assert json.loads((backup / "config.json").read_text(encoding="utf-8"))["pbgui"]["version"] == 1
    assert (backup / "BTC.json").is_file()


def test_backup_retention_draft_and_delete_use_complete_pb8_bundles(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """PB8 backup APIs retain exact bundles, prune versions, and load cookie-only editor drafts."""

    _configure_root(monkeypatch, tmp_path)
    _install_test_pipeline(monkeypatch)
    v8_instances._drafts.clear()
    v8_instances.put_v8_backup_settings({"max_versions": 1}, session=None)
    first = _payload(note="v1")
    first["config"]["coin_overrides"] = {"BTC": {"override_config_path": "BTC.json"}}
    first["override_configs"] = {"BTC.json": {"bot": {"long": {"risk": {"n_positions": 1}}}}}
    asyncio.run(v8_instances.save_v8_instance_config("alice", first, True, session=None))
    second = copy.deepcopy(first)
    second["config"]["pbgui"]["note"] = "v2"
    asyncio.run(v8_instances.save_v8_instance_config("alice", second, False, session=None))
    third = copy.deepcopy(first)
    third["config"]["pbgui"]["note"] = "v3"
    asyncio.run(v8_instances.save_v8_instance_config("alice", third, False, session=None))

    instance_backups = tmp_path / "data" / "backup" / "v8" / "alice"
    assert sorted(path.name for path in instance_backups.iterdir() if path.is_dir()) == ["2"]
    monkeypatch.setattr(v8_instances, "_list_instances", lambda: [{"name": "alice", "running_on": []}])
    listed = v8_instances.list_v8_backups(session=None)
    assert listed["backups"][0]["timestamps"] == ["2"]
    assert listed["backups"][0]["currently_exists"] is True

    request = SimpleNamespace(url_for=lambda _name: URL("http://test/api/v8/edit_page"))
    draft = v8_instances.create_v8_backup_draft("alice", "2", request, session=None)
    loaded = v8_instances.get_v8_editor_draft(draft["draft_id"], session=None)
    assert "token=" not in draft["edit_url"]
    assert loaded["config"]["pbgui"]["version"] == 3
    assert loaded["config"]["pbgui"]["from_backup_config"] == {"name": "alice", "timestamp": "2"}
    assert loaded["override_configs"]["BTC"]["bot"]["long"]["risk"]["n_positions"] == 1

    deleted = v8_instances.delete_v8_backup("alice", "2", session=None)
    assert deleted["timestamp"] == "2"
    assert not instance_backups.exists()


def test_save_rejects_stale_editor_version(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """A stale structured editor cannot overwrite a newer PB8 bundle."""

    _configure_root(monkeypatch, tmp_path)
    _install_test_pipeline(monkeypatch)
    asyncio.run(v8_instances.save_v8_instance_config("alice", _payload(), True, session=None))
    newer = _payload(note="newer")
    newer["expected_version"] = 1
    asyncio.run(v8_instances.save_v8_instance_config("alice", newer, False, session=None))
    stale = _payload(note="stale")
    stale["expected_version"] = 1

    with pytest.raises(v8_instances.HTTPException, match="Reload before saving") as error:
        asyncio.run(v8_instances.save_v8_instance_config("alice", stale, False, session=None))

    assert error.value.status_code == 409


def test_editor_draft_round_trips_override_payloads_by_coin(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Backtest-to-Run drafts retain sparse files without exposing filesystem paths."""

    _configure_root(monkeypatch, tmp_path)
    _install_test_pipeline(monkeypatch)
    v8_instances._drafts.clear()
    config = _payload()["config"]
    config["coin_overrides"] = {"BTC": {"override_config_path": "BTC.json"}}

    created = v8_instances.create_v8_editor_draft(
        {
            "config": config,
            "override_configs": {"BTC.json": {"bot": {"long": {"risk": {"n_positions": 1}}}}},
        },
        session=None,
    )
    loaded = v8_instances.get_v8_editor_draft(created["draft_id"], session=None)

    assert loaded["override_configs"] == {
        "BTC": {"bot": {"long": {"risk": {"n_positions": 1}}}},
    }
    with pytest.raises(v8_instances.HTTPException, match="missing from the draft"):
        v8_instances.create_v8_editor_draft({"config": config}, session=None)


def test_copy_publishes_disabled_pb8_bundle_with_override_files(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Copy preserves the complete sparse bundle while assigning a different user."""

    _configure_root(monkeypatch, tmp_path)
    _install_test_pipeline(monkeypatch)
    monkeypatch.setattr(v8_instances, "_available_users", lambda: [
        {"name": "alice", "exchange": "binance"},
        {"name": "bob", "exchange": "bybit"},
    ])
    source = _payload()
    source["config"]["coin_overrides"] = {"BTC": {"override_config_path": "BTC.json"}}
    source["override_configs"] = {"BTC.json": {"bot": {"long": {"risk": {"n_positions": 1}}}}}
    asyncio.run(v8_instances.save_v8_instance_config("alice", source, True, session=None))

    copy_config = copy.deepcopy(source["config"])
    copied = asyncio.run(v8_instances.copy_v8_instance_config(
        "alice",
        {
            "target_user": "bob",
            "config": copy_config,
            "override_configs": source["override_configs"],
        },
        session=None,
    ))

    target = tmp_path / "data" / "run_v8" / "bob"
    target_config = json.loads((target / "config.json").read_text(encoding="utf-8"))
    assert copied["source"] == "alice"
    assert target_config["live"]["user"] == "bob"
    assert target_config["pbgui"]["enabled_on"] == "disabled"
    assert (target / "BTC.json").is_file()


def test_delete_records_pb8_tombstone_before_local_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Delete removes the local bundle only after publishing PB8 desired state."""

    _configure_root(monkeypatch, tmp_path)
    _install_test_pipeline(monkeypatch)
    asyncio.run(v8_instances.save_v8_instance_config("alice", _payload(), True, session=None))

    result = v8_instances.delete_v8_instance("alice", session=None)
    desired = json.loads((tmp_path / "data" / "cluster" / "desired_state.json").read_text(encoding="utf-8"))

    assert result["operation"] == "DELETE_PB8_INSTANCE"
    assert result["backup_id"] == "1"
    assert not (tmp_path / "data" / "run_v8" / "alice").exists()
    assert (tmp_path / "data" / "backup" / "v8" / "alice" / "1" / "config.json").is_file()
    assert desired["pb8_tombstones"]["alice"]["version"] == "1"
    assert desired["tombstones"] == {}


def test_host_list_contains_only_confirmed_pb8_targets_and_unknown_current(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Host discovery excludes PB7-only/setup-failed hosts but preserves one unknown legacy target."""

    _configure_root(monkeypatch, tmp_path)
    monkeypatch.setattr(v8_instances.pbgui_purefunc, "pb8_runtime_status", lambda: {"ready": True})
    entries = {
        "pb8-vps": SimpleNamespace(runtime_profile="pb8", setup_status="successful"),
        "combined-vps": SimpleNamespace(runtime_profile="pb7_pb8", setup_status="successful"),
        "pb7-vps": SimpleNamespace(runtime_profile="pb7", setup_status="successful"),
        "inventory-drift": SimpleNamespace(runtime_profile="pb7", setup_status=None),
        "runtime-broken": SimpleNamespace(runtime_profile="pb8", setup_status="successful"),
        "failed-vps": SimpleNamespace(runtime_profile="pb8", setup_status="failed"),
    }
    monkeypatch.setattr(v8_instances, "_managed_vps_entries", lambda: entries)
    monkeypatch.setattr(
        v8_instances,
        "_remote_cluster_target_status",
        lambda _host: {"ready": True, "reason": "joined"},
    )
    now = time.time()
    v8_instances._monitor = SimpleNamespace(
        enabled_hosts=["fresh-ready", "fresh-not-ready", "legacy-unknown"],
        store=SimpleNamespace(host_meta={
            "fresh-ready": {"generated_at": now, "pb8ready": True},
            "fresh-not-ready": {"generated_at": now, "pb8ready": False},
            "inventory-drift": {"generated_at": now, "pb8ready": True},
            "runtime-broken": {"generated_at": now, "pb8ready": False},
            "legacy-unknown": {"generated_at": now - 1000, "pb8ready": True},
        }),
    )
    instance = tmp_path / "data" / "run_v8" / "legacy"
    instance.mkdir(parents=True)
    (instance / "config.json").write_text(json.dumps({
        "live": {"user": "alice"},
        "pbgui": {"runtime": "pb8", "version": 1, "enabled_on": "legacy-unknown", "note": ""},
    }), encoding="utf-8")

    result = v8_instances.get_v8_hosts(name="legacy", request_id="req-1", session=None)

    assert result["request_id"] == "req-1"
    assert result["hosts"] == [
        "disabled", "combined-vps", "fresh-ready", "inventory-drift", "master-a", "pb8-vps", "legacy-unknown"
    ]
    assert result["host_capabilities"]["inventory-drift"]["source"] == "host_meta"
    assert result["host_capabilities"]["legacy-unknown"]["legacy_preserved"] is True
    assert "pb7-vps" not in result["hosts"]
    assert "failed-vps" not in result["hosts"]
    assert "fresh-not-ready" not in result["hosts"]
    assert "runtime-broken" not in result["hosts"]


def test_pb8_host_list_excludes_older_config_schemas(monkeypatch: pytest.MonkeyPatch) -> None:
    """A host must not be selectable when its PB8 schema is older than the config."""

    monkeypatch.setattr(
        v8_instances.pbgui_purefunc,
        "pb8_runtime_status",
        lambda: {"ready": True, "config_schema": "v8.1.0"},
    )
    monkeypatch.setattr(v8_instances, "_master_hostname", lambda: "master-a")
    monkeypatch.setattr(v8_instances, "_managed_vps_entries", lambda: {
        "current-vps": SimpleNamespace(runtime_profile="pb8", setup_status="successful"),
        "old-vps": SimpleNamespace(runtime_profile="pb8", setup_status="successful"),
    })
    monkeypatch.setattr(
        v8_instances,
        "_remote_cluster_target_status",
        lambda _host: {"ready": True, "reason": "joined"},
    )
    now = time.time()
    v8_instances._monitor = SimpleNamespace(
        enabled_hosts=["current-vps", "old-vps"],
        store=SimpleNamespace(host_meta={
            "current-vps": {"generated_at": now, "pb8ready": True, "pb8_config_schema": "v8.1.0"},
            "old-vps": {"generated_at": now, "pb8ready": True, "pb8_config_schema": "v8.0.0"},
        }),
    )

    result = v8_instances.get_v8_hosts(name="", config_schema="v8.1.0", session=None)

    assert result["hosts"] == ["disabled", "current-vps", "master-a"]
    assert result["host_capabilities"]["current-vps"]["schema_compatible"] is True
    assert result["host_capabilities"]["old-vps"]["schema_compatible"] is False
    assert "old-vps" not in result["hosts"]


def test_pb8_target_requires_joined_remote_cluster_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    """An installed PB8 runtime is not deployable before Cluster Remote Join completes."""

    monkeypatch.setattr(v8_instances, "_managed_vps_entries", lambda: {
        "new-runner": SimpleNamespace(runtime_profile="pb8", setup_status="successful"),
    })
    monkeypatch.setattr(v8_instances, "_monitor", None)
    monkeypatch.setattr(
        v8_instances,
        "_remote_cluster_target_status",
        lambda _host: {"ready": False, "reason": "Host has not completed Cluster Remote Join"},
    )

    capability = v8_instances._host_runtime_capability("new-runner")

    assert capability["pb8_capable"] is False
    assert capability["cluster_ready"] is False
    assert capability["source"] == "cluster_state"
    assert capability["reason"] == "Host has not completed Cluster Remote Join"


def test_pb8_host_mapping_replaces_tombstoned_node_id(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Defensive PB8 publication rotates a stale mapping for a removed predecessor."""

    _configure_root(monkeypatch, tmp_path)
    root = tmp_path / "data" / "cluster"
    root.mkdir(parents=True)
    old_node_id = "pbgui-node-00000000-0000-4000-8000-000000000198"
    new_node_id = "pbgui-node-00000000-0000-4000-8000-000000000197"
    (root / "host_node_ids.json").write_text(json.dumps({
        "schema_version": 1,
        "hosts": {"new-runner": {"node_id": old_node_id, "created_at": 1, "role": "vps"}},
    }), encoding="utf-8")
    monkeypatch.setattr(v8_instances, "cluster_node_was_removed", lambda _root, node_id: node_id == old_node_id)
    monkeypatch.setattr(v8_instances, "generate_node_id", lambda: new_node_id)

    resolved = v8_instances._host_node_mapping("new-runner")
    saved = json.loads((root / "host_node_ids.json").read_text(encoding="utf-8"))

    assert resolved == new_node_id
    assert saved["hosts"]["new-runner"]["node_id"] == new_node_id


def test_instance_list_merges_exact_pb8_runtime_observations(monkeypatch: pytest.MonkeyPatch) -> None:
    """PB8 list status distinguishes confirmed running data from an unobserved process."""

    monkeypatch.setattr(v8_instances.pbgui_purefunc, "pb8_runtime_status", lambda: {"ready": False})
    monkeypatch.setattr(v8_instances, "_available_users", lambda: [{"name": "alice", "exchange": "binance"}])
    monkeypatch.setattr(v8_instances, "_user_exchange_cache", (0.0, {}))
    monkeypatch.setattr(v8_instances, "_monitor", SimpleNamespace(store=SimpleNamespace(v8_instances={
        "vps-a": [{"name": "running", "running": True, "rv": 3, "cv": 3}],
    })))
    rows = [
        {"name": "running", "user": "alice", "enabled_on": "vps-a", "version": 3, "status": "desired_running", "desired_status": "desired_running"},
        {"name": "unknown", "user": "alice", "enabled_on": "vps-b", "version": 1, "status": "desired_running", "desired_status": "desired_running"},
    ]

    enriched = v8_instances._enrich_v8_runtime(rows)

    assert enriched[0]["status"] == "synced"
    assert enriched[0]["running_on"] == ["vps-a"]
    assert enriched[0]["running_version"] == 3
    assert enriched[0]["exchange"] == "binance"
    assert enriched[1]["status"] == "collecting"


def test_instance_list_reports_active_pb8_strategy(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """PB8 Run list rows should expose canonical live.strategy_kind."""
    _configure_root(monkeypatch, tmp_path)
    _install_test_pipeline(monkeypatch)
    config_dir = tmp_path / "data" / "run_v8" / "alice"
    config_dir.mkdir(parents=True)
    (config_dir / "config.json").write_text(json.dumps({
        "live": {"user": "alice", "strategy_kind": "trailing_martingale"},
        "bot": {},
        "pbgui": {"enabled_on": "disabled", "version": 1},
    }), encoding="utf-8")
    monkeypatch.setattr(v8_instances, "_desired_pb8_state", lambda: ({}, {}, {}))

    rows = v8_instances._list_instances()

    assert len(rows) == 1
    assert rows[0]["strategy"] == "trailing_martingale"
    assert rows[0]["forced_mode_long"] == ""
    assert rows[0]["forced_mode_short"] == ""


@pytest.mark.parametrize("runtime_ready", (False, True))
def test_instance_list_distinguishes_runtime_failure_from_config_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    runtime_ready: bool,
) -> None:
    """Run rows classify global PB8 readiness separately from one invalid config."""
    _configure_root(monkeypatch, tmp_path)
    config_dir = tmp_path / "data" / "run_v8" / "alice"
    config_dir.mkdir(parents=True)
    (config_dir / "config.json").write_text(json.dumps({
        "live": {"user": "alice"},
        "bot": {},
        "pbgui": {"enabled_on": "disabled", "version": 1},
    }), encoding="utf-8")
    reason = "PB8 Rust extension has no source fingerprint stamp; rerun the PB8 update on this host."
    monkeypatch.setattr(
        v8_instances.pbgui_purefunc,
        "pb8_runtime_status",
        lambda: {"ready": runtime_ready, "errors": [] if runtime_ready else [reason]},
    )
    monkeypatch.setattr(
        v8_instances,
        "load_pb8_config",
        lambda _path: (_ for _ in ()).throw(RuntimeError("invalid live.user" if runtime_ready else reason)),
    )
    monkeypatch.setattr(v8_instances, "_desired_pb8_state", lambda: ({}, {}, {}))

    rows = v8_instances._list_instances()

    assert rows[0]["status"] == "config_error"
    assert rows[0]["load_error"] == ("invalid live.user" if runtime_ready else reason)
    assert rows[0]["runtime_error"] == ("" if runtime_ready else reason)


def test_instance_list_surfaces_remote_pb8_runtime_blocker(monkeypatch: pytest.MonkeyPatch) -> None:
    """An observed PB8 launch failure is shown as blocked rather than generic sync needed."""

    monkeypatch.setattr(v8_instances.pbgui_purefunc, "pb8_runtime_status", lambda: {"ready": False})
    monkeypatch.setattr(v8_instances, "_available_users", lambda: [])
    monkeypatch.setattr(v8_instances, "_user_exchange_cache", (0.0, {}))
    monkeypatch.setattr(v8_instances, "_monitor", SimpleNamespace(store=SimpleNamespace(v8_instances={
        "vps-a": [{
            "name": "blocked",
            "running": False,
            "rv": 0,
            "cv": 1,
            "blocked": True,
            "blocked_reason": "PB8 Rust extension has no source fingerprint stamp; rerun Update PB8 on this host",
            "cluster_gate": "runtime_not_ready",
        }],
    })))
    rows = [{
        "name": "blocked",
        "user": "alice",
        "enabled_on": "vps-a",
        "version": 1,
        "status": "desired_running",
        "desired_status": "desired_running",
    }]

    enriched = v8_instances._enrich_v8_runtime(rows)

    assert enriched[0]["status"] == "blocked"
    assert enriched[0]["blocked_on"] == ["vps-a"]
    assert enriched[0]["pb8_update_required_on"] == ["vps-a"]
    assert "rerun Update PB8" in enriched[0]["blocked_reason"]


def test_instance_list_does_not_request_pb8_update_for_other_blockers(monkeypatch: pytest.MonkeyPatch) -> None:
    """Cluster and process-exit blockers must not trigger the PB8 update prompt."""

    monkeypatch.setattr(v8_instances.pbgui_purefunc, "pb8_runtime_status", lambda: {"ready": False})
    monkeypatch.setattr(v8_instances, "_available_users", lambda: [])
    monkeypatch.setattr(v8_instances, "_user_exchange_cache", (0.0, {}))
    monkeypatch.setattr(v8_instances, "_monitor", SimpleNamespace(store=SimpleNamespace(v8_instances={
        "vps-a": [{
            "name": "blocked",
            "running": False,
            "rv": 0,
            "cv": 1,
            "blocked": True,
            "blocked_reason": "Cluster desired state assigns this instance to another node",
            "cluster_gate": "wrong_host",
        }],
    })))
    rows = [{
        "name": "blocked",
        "user": "alice",
        "enabled_on": "vps-a",
        "version": 1,
        "status": "desired_running",
        "desired_status": "desired_running",
    }]

    enriched = v8_instances._enrich_v8_runtime(rows)

    assert enriched[0]["status"] == "blocked"
    assert enriched[0]["pb8_update_required_on"] == []


def test_editor_metadata_covers_live_logging_monitor_and_empty_objects(monkeypatch: pytest.MonkeyPatch) -> None:
    """Runtime metadata exposes every editable PB8 runtime section, including empty JSON objects."""

    monkeypatch.setattr(
        v8_instances,
        "get_pb8_template_config",
        lambda: pytest.fail("editor metadata must reuse the optimizer template"),
    )
    monkeypatch.setattr(v8_instances, "get_pb8_optimize_metadata", lambda: {
        "template": {
            "bot": {"long": {}, "short": {}},
            "live": {"startup_phase_budgets": {}, "market_orders_allowed": False},
            "logging": {"level": 1, "live_event_debug_profiles": []},
            "monitor": {"enabled": True, "snapshot_interval_seconds": 1.0},
        },
        "strategies": ["trailing_martingale"],
        "strategy_specs": {},
        "strategy_defaults": {},
    })
    monkeypatch.setattr(v8_instances.pbgui_purefunc, "pb8_runtime_status", lambda: {"ready": True})

    metadata = v8_instances.get_v8_editor_metadata(session=None)

    assert metadata["params"]["live"]["startup_phase_budgets"] == {"type": "json", "default": {}}
    assert metadata["params"]["live"]["market_orders_allowed"]["type"] == "boolean"
    assert metadata["params"]["logging"]["level"]["default"] == 1
    assert metadata["params"]["logging"]["live_event_debug_profiles"]["type"] == "array"
    assert metadata["params"]["monitor"]["enabled"]["type"] == "boolean"
    assert metadata["params"]["monitor"]["snapshot_interval_seconds"]["type"] == "number"


def test_target_validation_rejects_pb7_only_and_new_unknown_but_preserves_unchanged_unknown(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Server-side target checks fail closed except for an unchanged unknown legacy assignment."""

    _configure_root(monkeypatch, tmp_path)
    capabilities = {
        "pb7-only": {"pb8_capable": False, "reason": "VPS runtime profile is pb7"},
        "unknown": {"pb8_capable": None, "reason": "metadata unavailable"},
    }
    monkeypatch.setattr(v8_instances, "_host_runtime_capability", lambda host: capabilities[host])

    with pytest.raises(v8_instances.HTTPException) as incompatible:
        asyncio.run(v8_instances._ensure_target_compatible("new", "pb7-only"))
    with pytest.raises(v8_instances.HTTPException) as unknown:
        asyncio.run(v8_instances._ensure_target_compatible("new", "unknown"))

    instance = tmp_path / "data" / "run_v8" / "legacy"
    instance.mkdir(parents=True)
    (instance / "config.json").write_text(json.dumps({"pbgui": {"enabled_on": "unknown"}}), encoding="utf-8")
    asyncio.run(v8_instances._ensure_target_compatible("legacy", "unknown"))

    assert incompatible.value.status_code == 409
    assert unknown.value.status_code == 409


def test_target_validation_rejects_older_pb8_config_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    """Server-side save validation must reject bypassed stale-runtime assignments."""

    monkeypatch.setattr(v8_instances, "_monitor", None)
    monkeypatch.setattr(v8_instances, "_host_runtime_capability", lambda _host: {
        "pb8_capable": True,
        "config_schema": "v8.0.0",
    })

    with pytest.raises(v8_instances.HTTPException) as incompatible:
        asyncio.run(v8_instances._ensure_target_compatible("alice", "old-vps", "v8.1.0"))

    assert incompatible.value.status_code == 409
    assert "supports only v8.0.0" in incompatible.value.detail
    assert "Update PB8" in incompatible.value.detail


def test_pb8_publication_waits_for_all_active_cluster_replicas(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """PB8 operations cannot enter an only partially upgraded Cluster oplog."""
    _configure_root(monkeypatch, tmp_path)
    root = v8_instances._cluster_root()
    identity = v8_instances.ensure_local_identity(root, role="master", pbname="master-a")
    remote_id = v8_instances.generate_node_id()
    monkeypatch.setattr(v8_instances, "_cluster_nodes", lambda: {
        str(identity["node_id"]): {"pbname": "master-a", "enabled": True, "state_replica": True},
        remote_id: {"pbname": "master-b", "enabled": True, "state_replica": True},
    })

    with pytest.raises(v8_instances.HTTPException, match="master-b") as blocked:
        v8_instances._ensure_pb8_cluster_rollout_ready(identity)

    (root / "sync_status.json").write_text(json.dumps({
        "finished_at": int(time.time()),
        "peers": [{"node_id": remote_id, "pb8_capability": True}],
    }), encoding="utf-8")
    v8_instances._ensure_pb8_cluster_rollout_ready(identity)
    assert blocked.value.status_code == 409


def test_frontend_and_server_register_the_complete_pb8_live_surface() -> None:
    """PB7 and PB8 Run use the shared structured editor without browser bearer tokens."""

    run_source = Path("frontend/v7_run.html").read_text(encoding="utf-8")
    edit_source = Path("frontend/v7_edit.html").read_text(encoding="utf-8")
    adapter_source = Path("frontend/js/run_editor_adapter.js").read_text(encoding="utf-8")
    list_adapter_source = Path("frontend/js/run_list_adapter.js").read_text(encoding="utf-8")
    api_source = Path("api/v8_instances.py").read_text(encoding="utf-8")
    nav_source = Path("frontend/pbgui_nav.js").read_text(encoding="utf-8")
    server_source = Path("PBApiServer.py").read_text(encoding="utf-8")

    assert "Authorization" not in run_source
    assert "%%TOKEN%%" not in run_source
    assert "Authorization" not in edit_source
    assert "%%TOKEN%%" not in edit_source
    assert "TOKEN" not in edit_source
    assert "Raw JSON" in edit_source
    assert "Instance name" not in edit_source
    assert "f-instance-name" not in edit_source
    assert "Enabled on" in edit_source
    assert "Coin Overrides" in edit_source
    assert "risk(sideConfig)" in adapter_source
    expected_live_fields = {
        "auto_gs",
        "balance_hysteresis_snap_pct",
        "balance_override",
        "candle_lock_timeout_seconds",
        "custom_endpoints_path",
        "defer_broad_candle_warmup",
        "enable_archive_candle_fetch",
        "enable_forager_ws_candles",
        "exchange_symbol_unavailable_cooldown_hours",
        "execution_delay_seconds",
        "fee_conversion_max_age_ms",
        "fee_pct_fallback",
        "fee_pct_sanity_abs_max",
        "fills_confirmation_overlap_minutes",
        "fills_recent_overlap_minutes",
        "filter_by_min_effective_cost",
        "force_cold_startup",
        "forager_ws_candle_rest_audit_minutes",
        "forager_score_hysteresis_pct",
        "forced_mode_long",
        "forced_mode_short",
        "hedge_mode",
        "hsl_accept_incomplete_history",
        "hsl_position_during_cooldown_policy",
        "hsl_signal_mode",
        "inactive_coin_candle_ttl_minutes",
        "leverage",
        "limit_order_create_max_market_dist_pct",
        "margin_mode_preference",
        "market_order_near_touch_threshold",
        "market_orders_allowed",
        "market_snapshot_ticker_strategy",
        "max_active_candle_tail_gap_minutes",
        "max_concurrent_api_requests",
        "max_disk_candles_per_symbol_per_tf",
        "max_forager_candle_refresh_seconds",
        "max_forager_candle_staleness_minutes",
        "max_memory_candles_per_symbol",
        "max_n_cancellations_per_batch",
        "max_n_creations_per_batch",
        "max_n_restarts_per_day",
        "max_ohlcv_fetches_per_minute",
        "max_realized_loss_pct",
        "max_warmup_minutes",
        "minimum_coin_age_days",
        "order_match_tolerance_pct",
        "order_replacement_churn_gate_activation_count",
        "order_replacement_churn_gate_market_dist_pct",
        "order_replacement_churn_gate_stability_minutes",
        "order_replacement_churn_gate_window_minutes",
        "pnls_max_lookback_days",
        "recv_window_ms",
        "startup_phase_budgets",
        "time_in_force",
        "warmup_concurrency",
        "warmup_jitter_seconds",
        "warmup_ratio",
    }
    for field in expected_live_fields:
        assert f"{field}:" in adapter_source
    expected_logging_fields = {
        "backup_count", "dir", "level", "live_event_debug_profiles", "max_bytes_mb",
        "memory_snapshot_interval_minutes", "persist_to_file", "rotation",
        "volume_refresh_info_threshold_seconds",
    }
    expected_monitor_fields = {
        "checkpoint_interval_minutes", "compress_rotated_segments", "emit_completed_candles", "enabled",
        "event_rotation_mb", "event_rotation_minutes", "include_raw_fill_payloads", "max_total_bytes",
        "price_tick_min_interval_ms", "retain_candles", "retain_days", "retain_fills", "retain_price_ticks",
        "root_dir", "snapshot_interval_seconds",
    }
    for field in expected_logging_fields | expected_monitor_fields:
        assert f"{field}:" in adapter_source
    assert "var sharedLoggingFields" in adapter_source
    assert "var sharedMonitorFields" in adapter_source
    assert "input.closest('.form-group') || input.closest('.chk-row')" in adapter_source
    assert "result.logging = Object.assign({}, baseLogging)" in edit_source
    assert "runEditorAdapter.managedMonitorKeys.forEach" in edit_source
    assert "f-startup-phase-budgets" in edit_source
    assert "f-log-debug-profiles" in edit_source
    assert "f-monitor-enabled" in edit_source
    shared_marker = '<!-- Rows 1-3: shared PB7/PB8 8-column grid -->'
    bot_marker = '<div class="section-title section-title-with-control">'
    assert edit_source.index(shared_marker) < edit_source.index(bot_marker)
    assert edit_source.index(bot_marker) < edit_source.index('id="f-strategy-kind"')
    assert edit_source.index('id="f-strategy-kind"') < edit_source.index('id="f-long-twe"')
    assert "strategySelect.addEventListener('change'" in edit_source
    assert "changeRunStrategyKind(this.value)" in edit_source
    shared_order = [
        'id="f-user"', 'id="f-enabled-on"', 'id="f-version"', 'id="f-leverage"',
        'id="f-margin-mode"', 'id="f-logging-level"', 'id="f-min-coin-age"',
        'id="f-pnls-lookback"', 'id="f-warmup-ratio"', 'id="f-max-loss-pct"',
        'id="f-note"', 'id="f-price-dist"', 'id="f-exec-delay"',
        'id="f-market-order-threshold"', 'id="f-filter-min-cost"',
        'id="f-market-orders"', 'id="f-hedge-mode"', 'id="f-auto-gs"',
    ]
    positions = [edit_source.index(field, edit_source.index(shared_marker)) for field in shared_order]
    assert positions == sorted(positions)
    assert (
        '<div class="form-group"></div>\n'
        '  <div class="form-group" data-v7-only style="grid-column:span 2; justify-content:flex-end">'
    ) in edit_source
    assert "websocketPath: isV8 ? '/api/v8/ws/v8'" in list_adapter_source
    assert "runListAdapter.configureUi()" in run_source
    assert '"v7_edit.html"' in api_source
    assert '"v7_run.html"' in api_source
    assert not Path("frontend/v8_edit.html").exists()
    assert not Path("frontend/v8_run.html").exists()
    assert "window.confirm" not in run_source + edit_source
    assert "'v8_run':             '/api/v8/main_page'" in nav_source
    assert "'v8_run':                      '44_pbv8_run'" in nav_source
    assert 'app.include_router(v8_router, prefix="/api/v8", tags=["v8"])' in server_source
    assert Path("docs/help/44_pbv8_run.md").is_file()
    assert Path("docs/help_de/44_pbv8_run.md").is_file()
