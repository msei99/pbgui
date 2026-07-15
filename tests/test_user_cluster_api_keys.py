"""Tests for API-key persistence, Cluster Sync, and credential previews."""

from __future__ import annotations

import json
import stat
from pathlib import Path
from types import SimpleNamespace

import User as user_module
from master.cluster_state import default_cluster_root, load_operations, read_local_identity, rebuild_materialized_state


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


def test_hl_expiry_routes_separate_saved_get_from_preview_post() -> None:
    """Only POST accepts an unsaved private key body for the expiry preview."""
    from api import api_keys

    routes = [route for route in api_keys.router.routes if route.path == "/{name}/hl-expiry"]
    methods = {method for route in routes for method in route.methods}

    assert methods == {"GET", "POST"}
    assert "private_key" not in api_keys.get_hl_expiry_single.__annotations__
    assert api_keys.HLExpiryOverride(private_key="preview-key").private_key == "preview-key"
