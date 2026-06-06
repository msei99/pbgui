"""Regression tests for local master uninstall path selection."""

from pathlib import Path

from setup.installer import core


def _write_unit(home: Path, unit: str, pbgui_dir: Path) -> Path:
    """Write a minimal pbgui systemd user unit for tests."""
    unit_dir = home / ".config" / "systemd" / "user"
    unit_dir.mkdir(parents=True, exist_ok=True)
    unit_path = unit_dir / unit
    unit_path.write_text(
        "[Service]\n"
        f"WorkingDirectory={pbgui_dir}\n"
        f"ExecStart={pbgui_dir.parent / 'venv_pbgui' / 'bin' / 'python'} -u {pbgui_dir / 'PBApiServer.py'}\n",
        encoding="utf-8",
    )
    return unit_path


def test_extract_pbgui_dir_from_unit_text_uses_working_directory() -> None:
    """The unit parser detects the checkout path from WorkingDirectory."""
    unit_text = "[Service]\nWorkingDirectory=/srv/one/pbgui\nExecStart=/venv/bin/python -u /srv/one/pbgui/PBRun.py\n"

    assert core._extract_pbgui_dir_from_unit_text(unit_text) == Path("/srv/one/pbgui")


def test_default_local_install_dir_prefers_existing_systemd_unit(tmp_path: Path, monkeypatch) -> None:
    """The installer defaults to the parent referenced by existing pbgui units."""
    home = tmp_path / "home"
    detected_parent = tmp_path / "detected"
    _write_unit(home, "pbgui-api.service", detected_parent / "pbgui")
    monkeypatch.setattr(core.Path, "home", staticmethod(lambda: home))

    assert core.default_local_install_dir() == str(detected_parent)


def test_local_systemd_units_for_install_skips_other_install(tmp_path: Path, monkeypatch) -> None:
    """Unit cleanup only selects units that point to the chosen install parent."""
    home = tmp_path / "home"
    selected_parent = tmp_path / "selected"
    other_parent = tmp_path / "other"
    _write_unit(home, "pbgui-api.service", other_parent / "pbgui")
    monkeypatch.setattr(core.Path, "home", staticmethod(lambda: home))
    logs: list[str] = []

    assert core._local_systemd_units_for_install(selected_parent, logs.append) == []
    assert any("points to" in line and "not selected" in line for line in logs)


def test_run_local_master_uninstall_keeps_other_install_units(tmp_path: Path, monkeypatch) -> None:
    """Uninstalling one parent does not stop or remove units for another parent."""
    home = tmp_path / "home"
    selected_parent = tmp_path / "selected"
    other_parent = tmp_path / "other"
    for parent in (selected_parent, other_parent):
        (parent / "pbgui").mkdir(parents=True)
    selected_unit = _write_unit(home, "pbgui-api.service", selected_parent / "pbgui")
    other_unit = _write_unit(home, "pbgui-pbrun.service", other_parent / "pbgui")
    monkeypatch.setattr(core.Path, "home", staticmethod(lambda: home))
    calls: list[list[str]] = []
    monkeypatch.setattr(core, "_run_user_systemctl_best_effort", lambda args, log: calls.append(args))
    logs: list[str] = []

    result = core.run_local_master_uninstall(
        core.LocalUninstallConfig(install_dir=str(selected_parent), confirm=True),
        logs.append,
    )

    assert result["ok"] is True
    assert ["stop", "pbgui-api.service"] in calls
    assert ["disable", "pbgui-api.service"] in calls
    assert ["stop", "pbgui-pbrun.service"] not in calls
    assert ["disable", "pbgui-pbrun.service"] not in calls
    assert not selected_unit.exists()
    assert other_unit.exists()
    assert not (selected_parent / "pbgui").exists()
    assert (other_parent / "pbgui").exists()
