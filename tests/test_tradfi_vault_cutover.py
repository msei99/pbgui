"""Focused regression coverage for the TradFi credential-vault cutover."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from credential_store import CredentialStore


def test_tradfi_routes_have_no_reveal_and_return_metadata_only(monkeypatch, tmp_path: Path) -> None:
    """TradFi APIs never expose stored values and no reveal route remains."""

    from api import api_keys

    store = CredentialStore(tmp_path / "credentials")

    class Publisher:
        def publish_tradfi(self, credential_id: str) -> dict:
            return {"status": "published", "credential_id": credential_id}

        def publish_tombstone(self, credential_id: str, kind: str) -> dict:
            return {"status": "tombstoned", "credential_id": credential_id, "kind": kind}

    monkeypatch.setattr(api_keys, "_credential_store", lambda: store)
    monkeypatch.setattr(api_keys, "_cluster_credential_publisher", lambda _store: Publisher())
    monkeypatch.setattr(api_keys, "_project_local_tradfi", lambda _store, _pending=None: {"status": "current"})

    saved = api_keys.update_tradfi_config(
        api_keys.TradFiConfig(
            provider="tiingo",
            label="market data",
            api_key="vault-only-token",
        ),
        session=None,
    )
    profiles = api_keys.tradfi_get_profiles(session=None)
    config = api_keys.get_tradfi_config(session=None)
    rendered = json.dumps({"saved": saved, "profiles": profiles, "config": config.model_dump()})

    assert "vault-only-token" not in rendered
    assert saved["profile"]["has_api_key"] is True
    assert profiles["profiles"][0]["configured"] is True
    assert profiles["profiles"][0]["has_api_key"] is True
    assert config.has_api_key is True
    assert all(route.path != "/tradfi/reveal" for route in api_keys.router.routes)

    deleted = api_keys.clear_tradfi_config(
        profile_id=saved["profile"]["id"],
        session=None,
    )
    assert deleted["tombstone"]["status"] == "tombstoned"
    assert store.list_tradfi() == []


def test_tradfi_test_accepts_profile_id_or_one_time_body_secret(monkeypatch, tmp_path: Path) -> None:
    """Connection tests resolve stored IDs or use an unsaved authenticated body secret."""

    from api import api_keys

    store = CredentialStore(tmp_path / "credentials")
    record = store.create_tradfi("tiingo", {"api_key": "stored-token"})
    calls: list[tuple[str, str, str]] = []
    monkeypatch.setattr(api_keys, "_credential_store", lambda: store)
    monkeypatch.setattr(api_keys, "_get_pb7_paths", lambda: ("/venv/python", "/pb7"))
    monkeypatch.setattr(
        api_keys,
        "_run_tradfi_test",
        lambda _py, _pb7, provider, key, secret: calls.append((provider, key, secret)) or (True, "ok"),
    )

    api_keys.tradfi_test_connection(
        api_keys.TradFiTestRequest(profile_id=record["id"]),
        session=None,
    )
    api_keys.tradfi_test_connection(
        api_keys.TradFiTestRequest(provider="tiingo", api_key="one-time-token"),
        session=None,
    )

    assert calls == [
        ("tiingo", "stored-token", ""),
        ("tiingo", "one-time-token", ""),
    ]


def test_tradfi_profiles_return_every_profile_with_explicit_activity(monkeypatch, tmp_path: Path) -> None:
    """Profile listing does not collapse inactive metadata or expose stored values."""
    from api import api_keys

    store = CredentialStore(tmp_path / "credentials")
    active = store.create_tradfi("tiingo", {"api_key": "active-only-token"}, label="active")
    inactive = store.create_tradfi(
        "tiingo",
        {"api_key": "inactive-only-token"},
        label="inactive",
        active=False,
    )
    monkeypatch.setattr(api_keys, "_credential_store", lambda: store)
    monkeypatch.setattr(
        api_keys,
        "_tradfi_replicated_selection",
        lambda: {
            "tiingo": {
                "provider": "tiingo",
                "profile_id": active["id"],
                "activation_generation": 4,
                "conflicted": False,
                "updated_at": 100,
            }
        },
    )
    monkeypatch.setattr(
        api_keys,
        "_tradfi_projection_status",
        lambda _store: {
            "status": "error",
            "desired_generation": 5,
            "applied_generation": 4,
            "attempts": 3,
            "last_error": "projection unavailable",
        },
    )

    payload = api_keys.tradfi_get_profiles(session=None)
    profiles = {item["id"]: item for item in payload["profiles"]}

    assert set(profiles) == {active["id"], inactive["id"]}
    assert profiles[active["id"]]["active"] is True
    assert profiles[inactive["id"]]["active"] is False
    assert profiles[active["id"]]["has_api_key"] is True
    assert profiles[active["id"]]["replicated_active"] is True
    assert profiles[active["id"]]["activation_generation"] == 4
    assert profiles[inactive["id"]]["replicated_active"] is False
    assert profiles[active["id"]]["last_operation_id"] == ""
    assert payload["replicated_active_profiles"]["tiingo"]["profile_id"] == active["id"]
    assert payload["projection"] == {
        "status": "error",
        "desired_generation": 5,
        "applied_generation": 4,
        "attempts": 3,
        "last_error": "projection unavailable",
    }
    rendered = json.dumps(payload)
    assert "active-only-token" not in rendered
    assert "inactive-only-token" not in rendered


def test_tradfi_create_new_keeps_multiple_same_provider_profiles(monkeypatch, tmp_path: Path) -> None:
    """Explicit create_new never silently versions an existing provider profile."""

    from api import api_keys

    store = CredentialStore(tmp_path / "credentials")

    class Publisher:
        """Standalone publisher adapter for API transaction coverage."""

        def publish_tradfi(self, credential_id: str) -> dict:
            return {"status": "published", "credential_id": credential_id}

    monkeypatch.setattr(api_keys, "_credential_store", lambda: store)
    monkeypatch.setattr(api_keys, "_cluster_credential_publisher", lambda _store: Publisher())
    monkeypatch.setattr(api_keys, "_project_local_tradfi", lambda _store, _pending=None: {"status": "current"})

    first = api_keys.update_tradfi_config(
        api_keys.TradFiConfig(provider="tiingo", api_key="first", create_new=True),
        session=None,
    )
    second = api_keys.update_tradfi_config(
        api_keys.TradFiConfig(provider="tiingo", api_key="second", create_new=True),
        session=None,
    )

    assert first["profile"]["id"] != second["profile"]["id"]
    assert len([item for item in store.list_tradfi() if item["provider"] == "tiingo"]) == 2


def test_tradfi_subprocess_keeps_secrets_out_of_argv_and_redacts_urls(monkeypatch) -> None:
    """PB7 provider tests use inherited environment secrets and redact diagnostics."""

    from api import api_keys

    captured = {}

    def fake_run(argv, **kwargs):
        captured["argv"] = argv
        captured["env"] = kwargs["env"]
        return SimpleNamespace(
            returncode=1,
            stdout="",
            stderr="request failed https://provider.invalid/query?token=one-time-token",
        )

    monkeypatch.setattr(api_keys.subprocess, "run", fake_run)
    success, message = api_keys._run_tradfi_test(
        "/venv/python",
        "/pb7",
        "tiingo",
        "one-time-token",
        "one-time-secret",
    )

    assert success is False
    assert "one-time-token" not in " ".join(captured["argv"])
    assert "one-time-secret" not in " ".join(captured["argv"])
    assert captured["env"]["PBGUI_TRADFI_API_KEY"] == "one-time-token"
    assert "one-time-token" not in message
    assert "https://" not in message


def test_market_data_and_runtime_consumers_resolve_tiingo_from_vault(monkeypatch, tmp_path: Path) -> None:
    """Market Data and standalone runtime helpers use the same active vault profile."""

    from api import market_data as market_data_api
    import hyperliquid_best_1m as hb

    store = CredentialStore(tmp_path / "data" / "credentials")
    record = store.create_tradfi("tiingo", {"api_key": "server-only-token"})
    monkeypatch.setattr(market_data_api, "PBGDIR", tmp_path)
    monkeypatch.setattr(hb, "CredentialStore", lambda _root: store)

    resolved_record, key = market_data_api._active_tiingo_credential()
    profiles = hb._load_tradfi_profiles_from_ini(tmp_path)

    assert resolved_record["id"] == record["id"]
    assert key == "server-only-token"
    assert profiles["tiingo"]["profile_id"] == record["id"]
    assert profiles["tiingo"]["api_key"] == "server-only-token"


def test_frontend_and_market_data_sources_contain_no_tiingo_secret_state() -> None:
    """Tiingo tokens are absent from browser fields, action bodies, and settings persistence."""

    root = Path(__file__).resolve().parent.parent
    frontend = (root / "frontend" / "market_data_main.html").read_text(encoding="utf-8")
    api_source = (root / "api" / "market_data.py").read_text(encoding="utf-8")
    editor = (root / "frontend" / "api_keys_editor.html").read_text(encoding="utf-8")

    assert "tiingo_api_key" not in frontend
    assert "settings-tiingo-api-key" not in frontend
    assert "JSON.stringify({ api_key:" not in frontend
    assert 'settings.get("tiingo_api_key")' not in api_source
    assert '"tradfi_profiles"' not in api_source
    assert "/tradfi/reveal" not in editor
    assert 'id="tradfiProfilesBody"' in editor
    assert "selectTradfiProfile(this.dataset.profileId)" in editor
    assert "item.active === true" in editor
    assert "Rotate Replacement" in editor
    assert "tradfiToggleActive()" in editor
    assert "tradfiLoadGeneration" in editor
    assert "tradfiActionController" in editor
    assert "Stored in vault; enter only to replace" in editor
    assert "createNew: !tradfiProfileId" in editor
    assert "create_new: intent.createNew" in editor
    assert "replicated active selection" in editor
    assert "Retry PB7 Projection" in editor
    assert "pending_delete" in editor
    assert "Failed to load profiles:" in editor
    assert "checkPendingTradfiSave(intent)" in editor
    assert "pending_operation_id === pending.operationId" in editor
    assert "last_operation_id === pending.operationId" in editor
    assert "tradfiPendingSaveIntent = { ...intent, operationId: operationId }" in editor
    assert "finishTradfiAction(context, false)" in editor
    assert "Submitted secrets remain in the form for an exact retry" in editor
    assert "error.operationId" in editor


def test_projection_failure_keeps_profile_inactive_and_retry_resumes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """TradFi is not locally selected until publication and projection both succeed."""

    from api import api_keys

    store = CredentialStore(tmp_path / "credentials")
    publications: list[str] = []

    class Publisher:
        """Record idempotent publication attempts."""

        def publish_tradfi(self, credential_id: str) -> dict:
            publications.append(credential_id)
            return {"status": "published", "credential_id": credential_id}

    projection_attempts = 0

    def project(_store: CredentialStore, pending_id: str | None = None) -> dict:
        nonlocal projection_attempts
        projection_attempts += 1
        if projection_attempts == 1:
            raise OSError("projection unavailable")
        return {"status": "current", "pending_id": pending_id}

    monkeypatch.setattr(api_keys, "_credential_store", lambda: store)
    monkeypatch.setattr(api_keys, "_cluster_credential_publisher", lambda _store: Publisher())
    monkeypatch.setattr(api_keys, "_project_local_tradfi", project)
    request = api_keys.TradFiConfig(
        provider="tiingo",
        api_key="retry-token",
        operation_id="tradfi-retry-1",
    )

    with pytest.raises(api_keys.HTTPException) as first:
        api_keys.update_tradfi_config(request, session=None)
    assert first.value.status_code == 500
    pending = store.list_tradfi()
    assert len(pending) == 1
    assert pending[0]["pending"] is True
    assert api_keys.tradfi_get_profiles(session=None)["profiles"][0]["pending_operation_id"] == "tradfi-retry-1"
    assert store.list_tradfi(active_only=True) == []

    resumed = api_keys.update_tradfi_config(request, session=None)
    assert resumed["profile"]["id"] == pending[0]["id"]
    assert resumed["profile"]["generation"] == 1
    assert [item["id"] for item in store.list_tradfi(active_only=True)] == [pending[0]["id"]]
    assert publications == [pending[0]["id"]]
    assert api_keys.tradfi_get_profiles(session=None)["profiles"][0]["last_operation_id"] == "tradfi-retry-1"


def test_projection_retry_returns_only_operation_and_durable_status(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The explicit retry action reports projection generations without credentials."""

    from api import api_keys

    store = CredentialStore(tmp_path / "credentials")
    store.create_tradfi("tiingo", {"api_key": "retry-status-secret"})
    monkeypatch.setattr(api_keys, "_credential_store", lambda: store)
    monkeypatch.setattr(api_keys, "_cluster_credential_publisher", lambda _store: object())
    monkeypatch.setattr(
        api_keys,
        "reconcile_pending_credentials",
        lambda *_args, **_kwargs: {"status": "current", "items": []},
    )
    monkeypatch.setattr(
        api_keys,
        "_project_local_tradfi",
        lambda _store, _pending=None: {"status": "current"},
    )
    monkeypatch.setattr(
        api_keys,
        "_tradfi_projection_status",
        lambda _store: {
            "status": "current",
            "desired_generation": 6,
            "applied_generation": 6,
            "attempts": 2,
            "last_error": None,
        },
    )

    payload = api_keys.retry_tradfi_projection(
        api_keys.TradFiProjectionRetry(operation_id="projection-retry-1"),
        session=None,
    )

    assert payload["ok"] is True
    assert payload["operation_id"] == "projection-retry-1"
    assert payload["projection"]["desired_generation"] == 6
    assert "retry-status-secret" not in json.dumps(payload)


def test_api_backup_restore_merges_exchange_and_reprojects_vault(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The API restore route never replaces PB7 wholesale or accepts backup TradFi."""

    from api import api_keys

    pb7 = tmp_path / "pb7"
    pb7.mkdir()
    live = pb7 / "api-keys.json"
    live.write_text('{"old":{"exchange":"okx"},"tradfi":{"api_key":"old"}}', encoding="utf-8")
    backup_dir = tmp_path / "data" / "api-keys"
    backup_dir.mkdir(parents=True)
    backup_name = "api-keys7_2026-07-15_00-00-00.json"
    (backup_dir / backup_name).write_text(
        json.dumps({
            "_api_serial": 5,
            "alice": {"exchange": "binance", "secret": "exchange-secret"},
            "tradfi": {"api_key": "backup-tradfi-secret"},
        }),
        encoding="utf-8",
    )
    store = CredentialStore(tmp_path / "data" / "credentials")
    profile = store.create_tradfi("tiingo", {"api_key": "vault-tradfi-secret"})
    monkeypatch.setattr(api_keys, "_PBGDIR", str(tmp_path))
    monkeypatch.setattr(api_keys, "_credential_store", lambda: store)
    monkeypatch.setattr(api_keys.pbgui_purefunc, "is_pb7_installed", lambda: True, raising=False)
    monkeypatch.setattr(api_keys.pbgui_purefunc, "pb7dir", lambda: str(pb7), raising=False)

    result = api_keys.restore_backup(backup_name, session=None)
    payload = json.loads(live.read_text(encoding="utf-8"))

    assert result["restored_to"] == ["pb7"]
    assert payload["alice"]["secret"] == "exchange-secret"
    assert payload["tradfi"]["active_profile_id"] == profile["id"]
    assert payload["tradfi"]["api_key"] == "vault-tradfi-secret"
    assert "backup-tradfi-secret" not in json.dumps(payload)
