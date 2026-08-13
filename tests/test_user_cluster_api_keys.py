"""Tests for API-key persistence, Cluster Sync, and credential previews."""

from __future__ import annotations

import json
import stat
from pathlib import Path
from types import SimpleNamespace

import User as user_module
import pytest
from fastapi import HTTPException
from master.cluster_state import default_cluster_root, load_operations, read_local_identity, rebuild_materialized_state


def test_weex_requires_passphrase_server_side(monkeypatch) -> None:
    """WEEX credentials must fail closed even when a client bypasses the browser form."""
    from api import api_keys

    monkeypatch.setattr(api_keys, "_get_users", lambda: SimpleNamespace(find_user=lambda _name: None))
    data = api_keys.UserCreateUpdate(exchange="weex", key="key", secret="secret")

    with pytest.raises(HTTPException) as exc_info:
        api_keys.create_user(name="weex-user", data=data, session=None)

    assert exc_info.value.status_code == 400
    assert "Passphrase is required for weex" in str(exc_info.value.detail)


def test_bitunix_credentials_do_not_require_passphrase(monkeypatch) -> None:
    """Bitunix uses key and secret only."""
    from api import api_keys

    stored = SimpleNamespace(users=[], find_user=lambda _name: None, save=lambda: None)
    monkeypatch.setattr(api_keys, "_get_users", lambda: stored)
    monkeypatch.setattr(api_keys, "delete_user_state", lambda _name: None)
    data = api_keys.UserCreateUpdate(exchange="bitunix", key="key", secret="secret")

    result = api_keys.create_user(name="bitunix-user", data=data, session=None)

    assert result.exchange == "bitunix"
    assert stored.users[0].passphrase is None


def test_user_detail_never_contains_credential_values() -> None:
    """Bulk and detail responses may expose presence, never stored credential material."""
    from api import api_keys

    user = SimpleNamespace(
        name="alice",
        exchange="weex",
        key="public-key",
        secret="secret-value",
        passphrase="passphrase-value",
        wallet_address=None,
        private_key="private-value",
        is_vault=False,
        quote="USDT",
        options=None,
        extra=None,
    )

    detail = api_keys._user_to_detail(user, False)

    assert detail.key is None
    assert detail.key_masked == "********"
    assert detail.secret_masked == "********"
    assert detail.passphrase_masked == "********"
    assert detail.private_key_masked == "********"
    assert not any(value in str(detail) for value in (user.key, user.secret, user.passphrase, user.private_key))


def test_api_key_reveal_is_post_only_no_store_and_private_fields_are_unavailable(monkeypatch) -> None:
    """Only one explicitly selected API key may be revealed through a POST body."""
    from api import api_keys

    user = SimpleNamespace(key="public-key")
    monkeypatch.setattr(api_keys, "_get_users", lambda: SimpleNamespace(find_user=lambda name: user if name == "alice" else None))
    response = api_keys.Response()

    result = api_keys.reveal_user_key(api_keys.UserKeyRevealRequest(name="alice"), response, session=None)

    assert result == {"value": "public-key"}
    assert response.headers["Cache-Control"] == "no-store"
    reveal_routes = [route for route in api_keys.router.routes if "reveal" in route.path and route.path != "/tradfi/reveal"]
    assert [(route.path, route.methods) for route in reveal_routes] == [("/reveal-key", {"POST"})]


def test_api_key_backup_diff_redacts_all_credentials(monkeypatch, tmp_path: Path) -> None:
    """Backup comparison may expose structure, never credential values."""
    from api import api_keys

    backup_dir = tmp_path / "data" / "api-keys"
    backup_dir.mkdir(parents=True)
    first = {
        "alice": {"key": "first-public", "secret": "first-secret", "passphrase": "first-pass"},
        "wallet": {"private_key": "first-private"},
    }
    second = {
        "alice": {"key": "second-public", "secret": "second-secret", "passphrase": "second-pass"},
        "wallet": {"private_key": "second-private"},
    }
    (backup_dir / "api-keys7_first.json").write_text(json.dumps(first), encoding="utf-8")
    (backup_dir / "api-keys7_second.json").write_text(json.dumps(second), encoding="utf-8")
    monkeypatch.setattr(api_keys, "_PBGDIR", str(tmp_path))
    response = api_keys.Response()

    result = api_keys.diff_backups(
        api_keys.DiffRequest(filename1="api-keys7_first.json", filename2="api-keys7_second.json"),
        response,
        session=None,
    )

    rendered = "\n".join(result["lines1"] + result["lines2"])
    for secret in (
        "first-public", "first-secret", "first-pass", "first-private",
        "second-public", "second-secret", "second-pass", "second-private",
    ):
        assert secret not in rendered
    assert rendered.count("<redacted>") == 8
    assert response.headers["Cache-Control"] == "no-store"


def test_update_keeps_stored_api_key_when_detail_field_is_blank(monkeypatch) -> None:
    """Masked detail editing must not erase an unchanged API key."""
    from api import api_keys

    user = SimpleNamespace(
        name="alice", exchange="weex", key="stored-key", secret="stored-secret",
        passphrase="stored-pass", wallet_address=None, private_key=None, is_vault=False,
        quote="USDT", options=None, extra=None,
    )
    users = SimpleNamespace(find_user=lambda name: user if name == "alice" else None, save=lambda: None)
    monkeypatch.setattr(api_keys, "_get_users", lambda: users)
    monkeypatch.setattr(api_keys, "delete_user_state", lambda _name: None)
    monkeypatch.setattr(api_keys, "_is_user_in_use", lambda _name: False)
    data = api_keys.UserCreateUpdate(exchange="weex", key=None, secret=None, passphrase=None, quote="USDT")

    api_keys.update_user(name="alice", data=data, session=None)

    assert user.key == "stored-key"
    assert user.secret == "stored-secret"
    assert user.passphrase == "stored-pass"


def test_exchange_change_requires_replacements_and_clears_irrelevant_credentials(monkeypatch) -> None:
    """Credentials from one venue must never silently migrate to another venue."""
    from api import api_keys

    user = SimpleNamespace(
        name="alice", exchange="weex", key="old-key", secret="old-secret",
        passphrase="old-pass", wallet_address=None, private_key=None, is_vault=False,
        quote="USDT", options=None, extra=None,
    )
    users = SimpleNamespace(find_user=lambda name: user if name == "alice" else None, save=lambda: None)
    monkeypatch.setattr(api_keys, "_get_users", lambda: users)
    monkeypatch.setattr(api_keys, "delete_user_state", lambda _name: None)
    monkeypatch.setattr(api_keys, "_is_user_in_use", lambda _name: False)

    with pytest.raises(HTTPException, match="API Key"):
        api_keys.update_user(
            name="alice",
            data=api_keys.UserCreateUpdate(exchange="bitunix", key=None, secret=None),
            session=None,
        )

    api_keys.update_user(
        name="alice",
        data=api_keys.UserCreateUpdate(exchange="bitunix", key="new-key", secret="new-secret"),
        session=None,
    )

    assert user.key == "new-key"
    assert user.secret == "new-secret"
    assert user.passphrase is None
    assert user.private_key is None


def _blob_path(root: Path, base: str, blob_hash: str) -> Path:
    """Return one content-addressed cluster blob path."""

    digest = blob_hash.removeprefix("sha256:")
    return root / base / "sha256" / digest[:2] / f"{digest}.json"


def test_users_save_records_api_key_cluster_secret_blob(monkeypatch, tmp_path: Path) -> None:
    """Saving api-keys.json writes redacted payload and restricted secret blobs."""

    pb7 = tmp_path / "pb7"
    pb7.mkdir()
    (pb7 / "api-keys.json").write_text(
        '{"tradfi":{"provider":"tiingo","api_key":"tradfi-vault-secret"}}',
        encoding="utf-8",
    )
    (tmp_path / "pbgui.ini").write_text("[main]\npbname = local-pbgui\n", encoding="utf-8")
    monkeypatch.setattr(user_module, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(user_module.pbgui_purefunc, "pbgui_ini_path", lambda: tmp_path / "pbgui.ini")
    monkeypatch.setattr(user_module, "pb7dir", lambda: str(pb7))
    monkeypatch.setattr(user_module, "is_pb7_installed", lambda: True)

    users = user_module.Users()
    assert "tradfi" not in users.list()
    assert "tradfi" not in users._top_level_extras
    user = user_module.User()
    user.name = "api_user"
    user.exchange = "binance"
    user.key = "public-key"
    user.secret = "super-secret"
    users.users.append(user)

    users.save()

    cluster_root = default_cluster_root(tmp_path)
    identity = read_local_identity(cluster_root)
    operations = load_operations(cluster_root)
    api_op = next(item for item in operations if item["op"] == "UPSERT_API_KEYS")
    desired = rebuild_materialized_state(cluster_root, write=False)["desired_state"]["api_keys"]
    payload_blob = _blob_path(cluster_root, "config_blobs", api_op["payload_hash"])
    secret_blob = _blob_path(cluster_root, "secret_blobs", api_op["secret_blob_hash"])

    assert desired["serial"] == 1
    assert identity["created_from_pbname"] == "local-pbgui"
    assert desired["secret_blob_hash"] == api_op["secret_blob_hash"]
    assert payload_blob.is_file()
    assert secret_blob.is_file()
    assert b"super-secret" not in payload_blob.read_bytes()
    assert b"super-secret" in secret_blob.read_bytes()
    assert b"tradfi-vault-secret" not in secret_blob.read_bytes()
    assert stat.S_IMODE(secret_blob.stat().st_mode) == 0o600
    assert stat.S_IMODE(secret_blob.parent.stat().st_mode) == 0o700
    assert stat.S_IMODE((cluster_root / "secret_blobs").stat().st_mode) == 0o700
    assert stat.S_IMODE((tmp_path / "data" / "api-keys").stat().st_mode) == 0o700
    assert stat.S_IMODE((pb7 / "api-keys.json").stat().st_mode) == 0o600
    assert all(
        stat.S_IMODE(path.stat().st_mode) == 0o600
        for path in (tmp_path / "data" / "api-keys").glob("*.json")
    )
    saved_payload = json.loads((pb7 / "api-keys.json").read_text(encoding="utf-8"))
    assert saved_payload["_api_serial"] == 1
    assert saved_payload["tradfi"]["api_key"] == "tradfi-vault-secret"


def test_hl_expiry_preview_uses_copy_without_persisting_override(monkeypatch) -> None:
    """Unsaved private keys are checked on a copy without cache persistence."""
    from api import api_keys

    user = SimpleNamespace(
        name="alice",
        exchange="hyperliquid",
        private_key="stored-key",
        extra={"existing": True},
    )
    users = SimpleNamespace(find_user=lambda name: user if name == "alice" else None)
    calls = []

    def fake_check(checked_user, users_obj=None):
        calls.append((checked_user, users_obj))
        return api_keys.HLExpiryInfo(name=checked_user.name, status="ok")

    monkeypatch.setattr(api_keys, "_get_users", lambda: users)
    monkeypatch.setattr(api_keys, "_check_hl_expiry_single", fake_check)

    saved_result = api_keys.get_hl_expiry_single(name="alice", session=None)
    preview_result = api_keys.preview_hl_expiry_single(
        name="alice",
        override=api_keys.HLExpiryOverride(private_key="unsaved-key"),
        session=None,
    )

    assert saved_result.status == "ok"
    assert preview_result.status == "ok"
    assert calls[0] == (user, users)
    preview_user, preview_users_obj = calls[1]
    assert preview_user is not user
    assert preview_user.private_key == "unsaved-key"
    assert preview_user.extra == user.extra
    assert preview_user.extra is not user.extra
    assert preview_users_obj is None
    assert user.private_key == "stored-key"


def test_hl_expiry_state_records_identity_only_for_saved_key(monkeypatch) -> None:
    """Bind persisted expiry metadata to saved credentials and never to a preview."""
    from api import api_keys

    valid_until = 1_900_000_000_000
    user = SimpleNamespace(
        name="alice",
        private_key="stored-key",
        wallet_address="0xwallet",
        is_vault=False,
    )
    updates = []
    monkeypatch.setattr(api_keys, "_get_agent_address", lambda _key: "0xagent")
    monkeypatch.setattr(
        api_keys,
        "_query_hl_info",
        lambda _payload: [{"address": "0xagent", "validUntil": valid_until}],
    )
    monkeypatch.setattr(
        api_keys,
        "update_user_state",
        lambda name, **fields: updates.append((name, fields)),
    )

    fingerprint = api_keys._hl_credential_fingerprint(user, "0xagent")
    saved = api_keys._check_hl_expiry_single(
        user,
        users_obj=SimpleNamespace(_loaded_api_serial=42),
    )
    preview = api_keys._check_hl_expiry_single(user, users_obj=None)

    assert saved.valid_until == valid_until
    assert preview.valid_until == valid_until
    assert updates == [(
        "alice",
        {"hl_valid_until": valid_until, "hl_credential_fingerprint": fingerprint},
    )]


def test_hl_expiry_routes_separate_saved_get_from_preview_post() -> None:
    """Only POST accepts an unsaved private key body for the expiry preview."""
    from api import api_keys

    routes = [route for route in api_keys.router.routes if route.path == "/{name}/hl-expiry"]
    methods = {method for route in routes for method in route.methods}

    assert methods == {"GET", "POST"}
    assert "private_key" not in api_keys.get_hl_expiry_single.__annotations__
    assert api_keys.HLExpiryOverride(private_key="preview-key").private_key == "preview-key"
