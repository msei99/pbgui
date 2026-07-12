"""Tests for central logging helper rotation and per-service configuration."""

from pathlib import Path

import logging_helpers


def test_rotate_logfile_keeps_configured_backup_count(tmp_path):
    """Rotation should keep multiple generations up to backup_count."""
    log_path = tmp_path / "service.log"

    # Force multiple rotations with very small threshold
    for idx in range(1, 5):
        log_path.write_text(f"entry-{idx}\n", encoding="utf-8")
        logging_helpers.rotate_logfile_if_oversize(str(log_path), max_bytes=1, backup_count=3)

    assert (tmp_path / "service.log.1").exists()
    assert (tmp_path / "service.log.2").exists()
    assert (tmp_path / "service.log.3").exists()
    assert not (tmp_path / "service.log.4").exists(), "Must not keep more than backup_count files"


def test_rotate_settings_can_be_saved_per_service(tmp_path, monkeypatch):
    """Per-service rotate settings should persist in pbgui.ini and be read back."""
    monkeypatch.chdir(tmp_path)

    # Ensure an ini exists
    Path("pbgui.ini").write_text("[main]\n", encoding="utf-8")

    logging_helpers.set_rotate_defaults(8 * 1024 * 1024, 2)
    logging_helpers.set_rotate_settings("PBRun", 2 * 1024 * 1024, 5)

    default_max_bytes, default_backup_count = logging_helpers.get_rotate_defaults()
    pbrun_max_bytes, pbrun_backup_count = logging_helpers.get_rotate_settings(service="PBRun")
    other_max_bytes, other_backup_count = logging_helpers.get_rotate_settings(service="SomeOtherService")

    assert default_max_bytes == 8 * 1024 * 1024
    assert default_backup_count == 2

    assert pbrun_max_bytes == 2 * 1024 * 1024
    assert pbrun_backup_count == 5

    # Unconfigured service should fall back to defaults
    assert other_max_bytes == 8 * 1024 * 1024
    assert other_backup_count == 2
