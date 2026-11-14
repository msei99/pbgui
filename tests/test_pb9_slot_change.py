"""Offline reverse-separation checks for the existing Welcome setup writer."""

import configparser
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

import api.auth as auth
import pb9_runtime
import pbgui_purefunc


@pytest.fixture
def settings(tmp_path, monkeypatch):
    """Provide isolated INI data and inert endpoint dependencies, never real runtime data."""
    path = tmp_path / "settings.ini"
    values = {
        "pb7dir": str(tmp_path / "pb7"), "pb7venv": str(tmp_path / "env7" / "bin" / "python"),
        "pb8dir": str(tmp_path / "pb8"), "pb8venv": str(tmp_path / "env8" / "bin" / "python"),
        "pb9dir": str(tmp_path / "pb9"), "pb9venv": str(tmp_path / "env9" / "bin" / "python"),
        "pbname": "fixture", "role": "slave",
    }
    parser = configparser.ConfigParser()
    parser["main"] = values
    parser["foreign"] = {"keep": "original"}
    with path.open("w", encoding="utf-8") as handle:
        parser.write(handle)
    monkeypatch.setattr(pbgui_purefunc, "pbgui_ini_path", lambda: path)

    def current():
        """Read only the test path settings from the current isolated generation."""
        snapshot = pbgui_purefunc.load_ini_snapshot()
        return {key: snapshot.get("main", key) for key in values if key not in {"pb9dir", "pb9venv"}}

    monkeypatch.setattr(auth, "pb7_runtime_status", current)
    monkeypatch.setattr(auth, "pb8_runtime_status", current)
    monkeypatch.setattr(auth, "_bootstrap_payload", lambda _session: {"setup": {**current(), "pb8": current()}})

    def forbidden(*args, **kwargs):
        """Slot changes compare paths only, with no source, artifact or process checks."""
        pytest.fail("reverse validation inspected or executed a runtime")

    monkeypatch.setattr(pb9_runtime, "classify_passivbot_source", forbidden)
    monkeypatch.setattr(pb9_runtime, "inspect_pb9_runtime", forbidden)
    monkeypatch.setattr(subprocess, "run", forbidden)
    return SimpleNamespace(path=path, values=values, root=tmp_path)


def _request(settings, **changes):
    """Build a normal Welcome request without adding new PB9 request fields."""
    values = {key: value for key, value in settings.values.items() if key not in {"pb9dir", "pb9venv"}}
    values.update(changes)
    return auth.SetupConfigRequest(**values)


@pytest.mark.parametrize("slot", ["pb7", "pb8"])
@pytest.mark.parametrize("relation", ["same", "child", "parent"])
def test_setup_rejects_overlapping_source_without_any_publication(settings, slot, relation):
    """Every nesting direction rejects the entire batch, including identity changes."""
    pb9 = Path(settings.values["pb9dir"])
    target = {"same": pb9, "child": pb9 / "nested", "parent": pb9.parent}[relation]
    before = settings.path.read_bytes()
    with pytest.raises(HTTPException) as caught:
        auth.save_setup(_request(settings, **{slot + "dir": str(target), "pbname": "changed"}), SimpleNamespace())
    assert caught.value.status_code == 409
    assert caught.value.headers["X-PBGUI-Error-Codes"] == "source_overlap"
    assert str(pb9) not in caught.value.detail
    assert settings.path.read_bytes() == before


@pytest.mark.parametrize("slot", ["pb7", "pb8"])
@pytest.mark.parametrize("symlink", [False, True])
def test_setup_rejects_shared_environment_even_after_legacy_symlink_resolution(settings, slot, symlink):
    """Raw interpreter requests retain environment identity at the API boundary."""
    python = Path(settings.values["pb9venv"])
    if symlink:
        python.parent.mkdir(parents=True)
        python.symlink_to(sys.executable)
    before = settings.path.read_bytes()
    with pytest.raises(HTTPException) as caught:
        auth.save_setup(_request(settings, **{slot + "venv": str(python)}), SimpleNamespace())
    assert caught.value.status_code == 409
    assert caught.value.headers["X-PBGUI-Error-Codes"] == "venv_shared"
    assert settings.path.read_bytes() == before


def test_setup_ignores_unchanged_conflict_when_saving_identity(settings):
    """Old manual conflicts do not block unrelated name/role changes."""
    pbgui_purefunc.save_ini_section("main", {"pb7dir": settings.values["pb9dir"]})
    settings.values["pb7dir"] = settings.values["pb9dir"]
    response = auth.save_setup(_request(settings, pbname="new-name", role="master"), SimpleNamespace())
    assert response["setup"]["pbname"] == "new-name"
    assert response["setup"]["role"] == "master"
    assert pbgui_purefunc.load_ini_snapshot().get("foreign", "keep") == "original"


def test_conflict_free_change_does_not_require_pb9_artifacts(settings):
    """A reserved but uninstalled PB9 layout cannot force native inspection."""
    target = str(settings.root / "new_pb8")
    response = auth.save_setup(_request(settings, pb8dir=target), SimpleNamespace())
    assert response["setup"]["pb8dir"] == target
    assert not Path(settings.values["pb9dir"]).exists()


def test_without_pb9_the_setup_writer_retains_legacy_bytes(settings):
    """The optional guard cannot change serialization or require PB9 when unset."""
    def clear(parser):
        """Remove both optional keys from this isolated INI."""
        parser.remove_option("main", "pb9dir")
        parser.remove_option("main", "pb9venv")

    pbgui_purefunc.update_ini(clear)
    before = settings.path.read_bytes()
    request = _request(settings, pb8dir=str(settings.root / "new_pb8"), pbname="changed")
    expected = {key: getattr(request, key) for key in ("pb7dir", "pb7venv", "pb8dir", "pb8venv", "pbname", "role")}
    pbgui_purefunc.save_ini_section("main", expected)
    expected_bytes = settings.path.read_bytes()
    settings.path.write_bytes(before)
    auth.save_setup(request, SimpleNamespace())
    assert settings.path.read_bytes() == expected_bytes


def test_hook_receives_actual_changes_and_rolls_back_before_write(settings):
    """Batch validation sees normalized candidate values and only changed keys."""
    before = settings.path.read_bytes()
    target = str(settings.root / "new_pb8")

    def validate(parser, changed_keys):
        """Reject a complete candidate without publishing any of it."""
        assert parser.get("main", "pb8dir") == target
        assert changed_keys == {("main", "pb8dir")}
        raise RuntimeError("fixture rejection")

    with pytest.raises(RuntimeError, match="fixture rejection"):
        pbgui_purefunc.save_ini_section("main", {"pb8dir": target, "role": "slave"}, validate=validate)
    assert settings.path.read_bytes() == before


def test_guard_uses_pb9_reservation_from_locked_parser(settings, monkeypatch):
    """A PB9 update immediately before this transaction is observed, not overwritten."""
    original_update = pbgui_purefunc.update_ini
    target = str(settings.root / "new_pb8")

    def update_after_reservation_changed(mutator):
        """Publish another writer's reservation before entering the checked batch."""
        original_update(lambda parser: parser.set("main", "pb9dir", target))
        return original_update(mutator)

    monkeypatch.setattr(pbgui_purefunc, "update_ini", update_after_reservation_changed)
    with pytest.raises(HTTPException) as caught:
        auth.save_setup(_request(settings, pb8dir=target), SimpleNamespace())
    assert caught.value.status_code == 409
    snapshot = pbgui_purefunc.load_ini_snapshot()
    assert snapshot.get("main", "pb9dir") == target
    assert snapshot.get("main", "pb8dir") == settings.values["pb8dir"]
