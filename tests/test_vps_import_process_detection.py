"""Regression tests for Existing VPS import process and key handling."""

from pathlib import Path

from vps_manager_service import _ensure_import_public_key, _import_process_line_is_legacy, _set_import_key_check


def test_import_process_line_ignores_systemd_managed_process() -> None:
    """A process row marked as systemd is not a legacy process."""
    line = "123\t/home/mani/software/pbgui\tsystemd\tpython -u /home/mani/software/pbgui/PBRun.py"

    assert not _import_process_line_is_legacy(line, "/home/mani/software/pbgui")


def test_import_process_line_reports_matching_legacy_process() -> None:
    """A matching process row marked as legacy is reported."""
    line = "123\t/home/mani/software/pbgui\tlegacy\tpython -u /home/mani/software/pbgui/PBRun.py"

    assert _import_process_line_is_legacy(line, "/home/mani/software/pbgui")


def test_import_process_line_preserves_old_three_column_rows() -> None:
    """Old probe rows without manager metadata remain legacy-compatible."""
    line = "123\t/home/mani/software/pbgui\tpython -u /home/mani/software/pbgui/PBRun.py"

    assert _import_process_line_is_legacy(line, "/home/mani/software/pbgui")


def test_ensure_import_public_key_uses_existing_default_key(tmp_path: Path, monkeypatch) -> None:
    """Existing default key pairs are reused for import monitoring."""
    ssh_dir = tmp_path / ".ssh"
    ssh_dir.mkdir()
    private_key = ssh_dir / "id_ed25519"
    public_key = ssh_dir / "id_ed25519.pub"
    private_key.write_text("private-key-placeholder\n", encoding="utf-8")
    public_key.write_text("ssh-ed25519 AAAATEST pbgui-test\n", encoding="utf-8")
    monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))

    key_path, key = _ensure_import_public_key()

    assert key_path == public_key
    assert key == "ssh-ed25519 AAAATEST pbgui-test"


def test_set_import_key_check_updates_existing_check() -> None:
    """The save flow can replace the probe key-login check after installing a key."""
    probe = {"checks": [{"label": "SSH key login for monitoring", "ok": False, "detail": "failed"}]}

    _set_import_key_check(probe, True, "Key authentication succeeded.")

    assert probe["checks"] == [
        {"label": "SSH key login for monitoring", "ok": True, "detail": "Key authentication succeeded."}
    ]
