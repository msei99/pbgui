"""Tests for API-key Cluster Sync operation recording."""

from __future__ import annotations

import json
import stat
from pathlib import Path

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
    (tmp_path / "pbgui.ini").write_text("[main]\npbname = local-pbgui\n", encoding="utf-8")
    monkeypatch.setattr(user_module, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(user_module, "pb7dir", lambda: str(pb7))
    monkeypatch.setattr(user_module, "is_pb7_installed", lambda: True)

    users = user_module.Users()
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
    assert stat.S_IMODE(secret_blob.stat().st_mode) == 0o600
    assert json.loads((pb7 / "api-keys.json").read_text(encoding="utf-8"))["_api_serial"] == 1
