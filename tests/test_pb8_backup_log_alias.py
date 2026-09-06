"""Security checks for PB8 historical log aliases used by the shared viewer."""

from pathlib import Path

from master import async_logs


def test_pb8_backup_log_alias_resolves_only_allowlisted_files(monkeypatch, tmp_path: Path) -> None:
    """Virtual aliases remain below the PB8 backup root and reject arbitrary filenames."""

    monkeypatch.setattr(async_logs, "_project_root", lambda: tmp_path)
    expected = tmp_path / "data" / "backup" / "v8" / "alice" / "7" / "passivbot_err.log"

    assert async_logs.resolve_local_log_path("BotBackup:alice:8:7:passivbot_err.log") == expected
    assert async_logs.resolve_local_log_path("BotBackup:alice:8:../7:passivbot_err.log") is None
    assert async_logs.resolve_local_log_path("BotBackup:alice:8:7:config.json") is None
