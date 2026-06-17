"""Regression tests for removing legacy SSH/FileSync sync paths."""

import asyncio
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _read(relative_path: str) -> str:
    """Return a repository file as text."""

    return (ROOT / relative_path).read_text(encoding="utf-8")


def test_legacy_worker_modules_are_not_present() -> None:
    """Ensure deleted legacy worker modules stay removed."""

    assert not (ROOT / "master" / "file_sync.py").exists()
    assert not (ROOT / "master" / "v7_config_sync.py").exists()
    assert not (ROOT / "frontend" / "js" / "api_sync_status.js").exists()


def test_api_startup_does_not_create_legacy_sync_workers() -> None:
    """Ensure PBApiServer does not initialize deleted sync workers."""

    source = _read("PBApiServer.py")
    forbidden = [
        "FileSyncWorker",
        "V7ConfigSyncWorker",
        "init_file_sync",
        "file_sync.start_watchers",
        "v7_sync.start_watchers",
    ]
    for needle in forbidden:
        assert needle not in source


def test_api_key_ssh_sync_routes_are_removed() -> None:
    """Ensure direct API-key SSH sync endpoints and UI are absent."""

    api_source = _read("api/api_keys.py")
    ui_source = _read("frontend/api_keys_editor.html")
    vps_source = _read("frontend/vps_manager.html")
    for source in (api_source, ui_source, vps_source):
        assert "/sync/push-ssh" not in source
        assert "/sync/ssh-status" not in source
        assert "/sync/ssh-retention" not in source
        assert "Advanced API Sync" not in source
    assert "_file_sync_worker" not in api_source
    assert "createApiSyncStatusController" not in ui_source
    assert "createApiSyncStatusController" not in vps_source


def test_v7_routes_do_not_remote_write_configs() -> None:
    """Ensure V7 save/delete paths no longer perform direct remote writes."""

    source = _read("api/v7_instances.py")
    forbidden = [
        "remote_path_join",
        "remote_shell_path",
        "_open_sftp",
        "SFTP_RETRY_ATTEMPTS",
        "SFTP_RETRY_DELAY",
        "rm -rf",
    ]
    for needle in forbidden:
        assert needle not in source
    assert "cluster_sync" in source


def test_v7_sync_hook_returns_cluster_handoff(tmp_path, monkeypatch) -> None:
    """Ensure the legacy V7 sync hook no longer requires SSH state."""

    import api.v7_instances as v7_instances

    monkeypatch.setattr(v7_instances, "PBGDIR", str(tmp_path))
    instance_dir = tmp_path / "data" / "run_v7" / "demo"
    instance_dir.mkdir(parents=True)
    (instance_dir / "config.json").write_text("{}", encoding="utf-8")

    result = asyncio.run(v7_instances._ssh_sync_instance("demo"))

    assert result["cluster_sync"] is True
    assert result["disabled"] is True
    assert result["hosts"] == {}
