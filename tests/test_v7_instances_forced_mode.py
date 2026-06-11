"""Tests for V7 Run forced-mode actions."""

import asyncio
import json

from api import v7_instances


def test_set_instance_forced_mode_panic_saves_and_syncs(monkeypatch, tmp_path):
    """Panic action sets both global forced modes, bumps version, backs up, and syncs."""
    inst_dir = tmp_path / "data" / "run_v7" / "test_inst"
    inst_dir.mkdir(parents=True)
    config_path = inst_dir / "config.json"
    config_path.write_text(json.dumps({"live": {}, "pbgui": {"version": 3}}), encoding="utf-8")
    synced = {"called": False}

    def fake_load(path, neutralize_added=False):
        """Load raw JSON for the test config."""
        return json.loads(path.read_text(encoding="utf-8"))

    def fake_save(cfg, path):
        """Save raw JSON for assertion."""
        path.write_text(json.dumps(cfg), encoding="utf-8")

    async def fake_sync(name):
        """Record that the forced-mode action triggered sync."""
        synced["called"] = True
        return {"name": name, "ok": 1, "failed": 0, "hosts": {}}

    monkeypatch.setattr(v7_instances, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(v7_instances, "load_pb7_config", fake_load)
    monkeypatch.setattr(v7_instances, "save_pb7_config", fake_save)
    monkeypatch.setattr(v7_instances, "_update_status_v7", lambda name: None)
    monkeypatch.setattr(v7_instances, "_ssh_sync_instance", fake_sync)

    result = asyncio.run(v7_instances.set_instance_forced_mode("test_inst", {"mode": "panic"}, session=None))

    saved = json.loads(config_path.read_text(encoding="utf-8"))
    assert result["ok"] is True
    assert result["forced_mode"] == "p"
    assert result["version"] == 4
    assert saved["live"]["forced_mode_long"] == "p"
    assert saved["live"]["forced_mode_short"] == "p"
    assert saved["pbgui"]["version"] == 4
    assert synced["called"] is True
    assert (tmp_path / "data" / "backup" / "v7" / "test_inst" / "3" / "config.json").is_file()


def test_set_instance_forced_mode_graceful_stop(monkeypatch, tmp_path):
    """Graceful Stop action writes the PB7 graceful_stop forced mode."""
    inst_dir = tmp_path / "data" / "run_v7" / "test_inst"
    inst_dir.mkdir(parents=True)
    config_path = inst_dir / "config.json"
    config_path.write_text(json.dumps({"live": {}, "pbgui": {"version": 8}}), encoding="utf-8")

    def fake_load(path, neutralize_added=False):
        """Load raw JSON for the test config."""
        return json.loads(path.read_text(encoding="utf-8"))

    def fake_save(cfg, path):
        """Save raw JSON for assertion."""
        path.write_text(json.dumps(cfg), encoding="utf-8")

    async def fake_sync(name):
        """Return a successful sync result."""
        return {"name": name, "ok": 1, "failed": 0, "hosts": {}}

    monkeypatch.setattr(v7_instances, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(v7_instances, "load_pb7_config", fake_load)
    monkeypatch.setattr(v7_instances, "save_pb7_config", fake_save)
    monkeypatch.setattr(v7_instances, "_update_status_v7", lambda name: None)
    monkeypatch.setattr(v7_instances, "_ssh_sync_instance", fake_sync)

    result = asyncio.run(v7_instances.set_instance_forced_mode("test_inst", {"mode": "graceful_stop"}, session=None))

    saved = json.loads(config_path.read_text(encoding="utf-8"))
    assert result["forced_mode"] == "graceful_stop"
    assert result["version"] == 9
    assert saved["live"]["forced_mode_long"] == "graceful_stop"
    assert saved["live"]["forced_mode_short"] == "graceful_stop"


def test_set_instance_forced_mode_tp_only(monkeypatch, tmp_path):
    """Take Profit Only action writes the PB7 tp_only forced mode."""
    inst_dir = tmp_path / "data" / "run_v7" / "test_inst"
    inst_dir.mkdir(parents=True)
    config_path = inst_dir / "config.json"
    config_path.write_text(json.dumps({"live": {}, "pbgui": {"version": 12}}), encoding="utf-8")

    def fake_load(path, neutralize_added=False):
        """Load raw JSON for the test config."""
        return json.loads(path.read_text(encoding="utf-8"))

    def fake_save(cfg, path):
        """Save raw JSON for assertion."""
        path.write_text(json.dumps(cfg), encoding="utf-8")

    async def fake_sync(name):
        """Return a successful sync result."""
        return {"name": name, "ok": 1, "failed": 0, "hosts": {}}

    monkeypatch.setattr(v7_instances, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(v7_instances, "load_pb7_config", fake_load)
    monkeypatch.setattr(v7_instances, "save_pb7_config", fake_save)
    monkeypatch.setattr(v7_instances, "_update_status_v7", lambda name: None)
    monkeypatch.setattr(v7_instances, "_ssh_sync_instance", fake_sync)

    result = asyncio.run(v7_instances.set_instance_forced_mode("test_inst", {"mode": "tp_only"}, session=None))

    saved = json.loads(config_path.read_text(encoding="utf-8"))
    assert result["forced_mode"] == "tp_only"
    assert result["version"] == 13
    assert saved["live"]["forced_mode_long"] == "tp_only"
    assert saved["live"]["forced_mode_short"] == "tp_only"
