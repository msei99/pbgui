"""Tests for V7 instance backup listing."""

from __future__ import annotations

import json
import os
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse

from api import v7_instances


def test_list_backups_skips_dirs_without_config_json(monkeypatch, tmp_path: Path) -> None:
    """Only restoreable backup directories are returned to the UI."""

    backup_root = tmp_path / "data" / "backup" / "v7" / "bybit_SOLUSDT"
    valid_dir = backup_root / "6"
    invalid_dir = backup_root / "7"
    valid_dir.mkdir(parents=True)
    invalid_dir.mkdir(parents=True)
    (valid_dir / "config.json").write_text("{}", encoding="utf-8")
    (invalid_dir / "backtest.json").write_text("{}", encoding="utf-8")
    os.utime(valid_dir, (200, 200))
    os.utime(invalid_dir, (300, 300))

    monkeypatch.setattr(v7_instances, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(v7_instances, "_load_local_instances", lambda: [])
    monkeypatch.setattr(v7_instances, "_enrich_with_vps_data", lambda instances: instances)

    result = v7_instances.list_backups(session=None)

    assert result["backups"][0]["name"] == "bybit_SOLUSDT"
    assert result["backups"][0]["timestamps"] == ["6"]
    assert result["backups"][0]["backup_items"][0]["id"] == "6"


def test_backup_draft_for_deleted_instance_preserves_editor_name(monkeypatch, tmp_path: Path) -> None:
    """Loading a backup after delete opens the editor with the original instance name."""

    backup_dir = tmp_path / "data" / "backup" / "v7" / "hl_mani10_PEPE" / "7"
    backup_dir.mkdir(parents=True)
    (backup_dir / "config.json").write_text(json.dumps({"pbgui": {"version": 7}, "live": {}}), encoding="utf-8")
    monkeypatch.setattr(v7_instances, "PBGDIR", str(tmp_path))
    v7_instances._draft_configs.clear()

    request = SimpleNamespace(url_for=lambda name: "http://test/api/v7/edit_page")
    session = SimpleNamespace(token="tok")

    result = v7_instances.create_backup_draft("hl_mani10_PEPE", "7", request, session=session)
    query = parse_qs(urlparse(result["edit_url"]).query)

    assert query["name"] == ["hl_mani10_PEPE"]
    assert query["draft_id"] == [result["draft_id"]]
    assert query["token"] == ["tok"]
    assert "new" not in query
